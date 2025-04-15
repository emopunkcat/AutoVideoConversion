import os
import time
import json
import shutil
import logging
import subprocess
import configparser
import re
from datetime import datetime
from queue import Queue
from threading import Thread
from logging.handlers import RotatingFileHandler
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

try:
    import ffmpeg as ffmpeg_python
except ImportError:
    raise ImportError("ffmpeg-python required. Install with 'pip install ffmpeg-python'")

# --- Configuration Loading ---
def load_config():
    config = configparser.ConfigParser()
    config_file = "video_encoder.conf"
    
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Configuration file '{config_file}' not found in {os.getcwd()}")
    
    config.read(config_file)
    if not config.sections():
        raise ValueError(f"Configuration file '{config_file}' is empty or invalid")
    
    try:
        return {
            'VERSION': config['General']['version'],
            'WATCH_DIR': config['General']['watch_dir'],
            'LOG_FILE': config['General']['log_file'],
            'MAX_LOG_SIZE': int(config['General']['max_log_size_mb']) * 1024 * 1024,
            'BACKUP_COUNT': int(config['General']['backup_count']),
            'MAX_CONCURRENT_JOBS': int(config['General']['max_concurrent_jobs']),
            'FAILED_DIR': config['General']['failed_dir'],
            'MAX_RETRIES': int(config['General']['max_retries']),
            'VIDEO_EXTENSIONS': tuple(ext.strip().lower() for ext in config['FileSettings']['video_extensions'].split(',')),
            'MIN_VIDEO_SIZE': int(config['FileSettings']['min_video_size_mb']) * 1024 * 1024,
            'MAX_VIDEO_SIZE': int(config['FileSettings']['max_video_size_mb']) * 1024 * 1024,
            'OUTPUT_SUFFIX': config['FileSettings']['output_suffix'],
            'FFMPEG_PATH': config['FFmpegSettings']['ffmpeg_path'],
            'FFMPEG_FALLBACK_PATH': config['FFmpegSettings']['ffmpeg_fallback_path'],
            'ENCODER_SETTINGS': {
                'vcodec': config['EncoderSettings']['vcodec'],
                'profile:v': config['EncoderSettings']['profile'],
                'level': config['EncoderSettings']['level'],
                'crf': int(config['EncoderSettings']['crf']),
                'preset': config['EncoderSettings']['preset'],
                'pix_fmt': config['EncoderSettings']['pix_fmt'],
                'format': config['EncoderSettings']['format'],
                'acodec': config['EncoderSettings']['acodec'],
                'ab': config['EncoderSettings']['audio_bitrate'],
                'ar': int(config['EncoderSettings']['audio_sample_rate']),
                'movflags': config['EncoderSettings']['movflags'],
                'strict': config['EncoderSettings']['strict'],
                'map_metadata': 0,
                'r': int(config['EncoderSettings']['max_fps'])
            },
            'SECLUDED_FOLDERS': set(folder.strip().lower() for folder in config['FolderSettings']['secluded_folders'].split(','))
        }
    except (KeyError, ValueError) as e:
        raise type(e)(f"Configuration error: {str(e)}")

# --- Logger Setup ---
def setup_logger(log_file, max_log_size, backup_count):
    logger = logging.getLogger('VideoEncoder')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

# --- Statistics Management ---
class StatsManager:
    def __init__(self, stats_file, logger):
        self.stats_file = stats_file
        self.logger = logger
        self.stats = self._load_stats()

    def _load_stats(self):
        try:
            if os.path.exists(self.stats_file):
                with open(self.stats_file, 'r') as f:
                    return json.load(f)
            self.logger.info(f"Initializing new stats at {self.stats_file}")
            return {}
        except Exception as e:
            self.logger.error(f"Error loading stats: {str(e)}")
            return {}

    def _save_stats(self):
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats, f, indent=4)
        except Exception as e:
            self.logger.error(f"Error saving stats: {str(e)}")

    def update_stats(self, input_size, output_size, encoding_time, success=True):
        month_key = datetime.now().strftime("%Y-%m")
        month_stats = self.stats.setdefault(month_key, {
            "files_converted": 0, "failed_encodings": 0,
            "total_space_saved_bytes": 0, "total_input_size_bytes": 0,
            "total_output_size_bytes": 0, "total_encoding_time_seconds": 0
        })
        
        if success:
            month_stats["files_converted"] += 1
            month_stats["total_space_saved_bytes"] += max(0, input_size - output_size)
            month_stats["total_input_size_bytes"] += input_size
            month_stats["total_output_size_bytes"] += output_size
            month_stats["total_encoding_time_seconds"] += encoding_time
        else:
            month_stats["failed_encodings"] += 1
        self._save_stats()

    def get_lifetime_stats(self):
        return {
            key: sum(month[key] for month in self.stats.values())
            for key in next(iter(self.stats.values())).keys()
        } if self.stats else {
            "files_converted": 0, "failed_encodings": 0,
            "total_space_saved_bytes": 0, "total_input_size_bytes": 0,
            "total_output_size_bytes": 0, "total_encoding_time_seconds": 0
        }

    def format_stats(self, stats, label):
        return (
            f"{label} Stats:\n"
            f"  Files: {stats['files_converted']} converted, {stats['failed_encodings']} failed\n"
            f"  Space: {stats['total_space_saved_bytes'] / (1024 * 1024):.2f} MB saved\n"
            f"  Size: {stats['total_input_size_bytes'] / (1024 * 1024):.2f} MB in, "
            f"{stats['total_output_size_bytes'] / (1024 * 1024):.2f} MB out\n"
            f"  Time: {stats['total_encoding_time_seconds'] / 3600:.2f} hours"
        )

# --- Video Handler ---
class VideoHandler(FileSystemEventHandler):
    def __init__(self, queue, config, logger, processed_output_files):
        super().__init__()
        self.queue = queue
        self.config = config
        self.logger = logger
        self.processed_output_files = processed_output_files
        self.excluded_dirs = self.config['SECLUDED_FOLDERS'] | {'.videos', os.path.basename(self.config['FAILED_DIR']).lower()}

    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext not in self.config['VIDEO_EXTENSIONS']:
            return

        relative_path = os.path.relpath(file_path, self.config['WATCH_DIR'])
        if any(part.lower() in self.excluded_dirs for part in relative_path.split(os.sep)[:-1]):
            return

        time.sleep(1)
        if not os.path.exists(file_path):
            return

        filename = os.path.splitext(os.path.basename(file_path))[0]
        suffix_pattern = self.config['OUTPUT_SUFFIX'].replace('{date}', r'\d{8}')
        if re.search(rf"{suffix_pattern}(?:\.mp4)?$", filename):
            return
        
        if file_path in self.processed_output_files:
            return

        file_size = os.path.getsize(file_path)
        if not (self.config['MIN_VIDEO_SIZE'] <= file_size <= self.config['MAX_VIDEO_SIZE']):
            self.logger.info(f"File size out of range ({file_size / (1024 * 1024):.2f} MB): {file_path}")
            return

        self.logger.info(f"New video detected: {os.path.basename(file_path)}")
        self.queue.put(file_path)

# --- Video Encoder ---
class VideoEncoder:
    def __init__(self, queue, config, logger, processed_output_files, stats_manager):
        self.queue = queue
        self.config = config
        self.logger = logger
        self.processed_files = set()
        self.processed_output_files = processed_output_files
        self.stats_manager = stats_manager

    def process_video(self, input_path, retry_count=0):
        if input_path in self.processed_files:
            return
        self.processed_files.add(input_path)

        output_path = None
        input_size = output_size = encoding_time = 0
        success = False

        try:
            time.sleep(2)
            if not os.path.exists(input_path):
                return

            input_size = os.path.getsize(input_path)
            output_path = self._get_output_path(input_path)
            
            if os.path.exists(output_path):
                return

            self.logger.info(f"Encoding: {input_path} to {output_path} (Attempt {retry_count + 1}/{self.config['MAX_RETRIES'] + 1})")
            
            encoder_args = self.config['ENCODER_SETTINGS'].copy()
            probe = ffmpeg_python.probe(input_path)
            if any(s['codec_type'] == 'audio' and s.get('codec_name') == 'aac' for s in probe['streams']):
                encoder_args['acodec'] = 'copy'

            start_time = time.time()
            ffmpeg_python.input(input_path).output(output_path, **encoder_args).run(quiet=True, overwrite_output=True)
            encoding_time = time.time() - start_time
            output_size = os.path.getsize(output_path)
            
            self.processed_output_files.add(output_path)
            self._move_to_videos(input_path)
            success = True
            self.logger.info(f"Encoded: {output_path} in {encoding_time:.2f}s")

        except (ffmpeg_python.Error, Exception) as e:
            self.logger.error(f"Error encoding {input_path}: {str(e)}")
            self._cleanup_failed(output_path)
            if retry_count < self.config['MAX_RETRIES']:
                time.sleep(5)
                self.process_video(input_path, retry_count + 1)
            else:
                self._move_to_failed(input_path)
        finally:
            self.processed_files.discard(input_path)
            self.stats_manager.update_stats(input_size, output_size, encoding_time, success)

    def _get_output_path(self, input_path):
        current_date = datetime.now().strftime("%Y%m%d")
        directory, filename = os.path.split(input_path)
        filename = os.path.splitext(filename)[0]
        output_suffix = self.config['OUTPUT_SUFFIX'].format(date=current_date)
        return os.path.join(directory, f"{filename}{output_suffix}.mp4")

    def _cleanup_failed(self, output_path):
        if output_path and os.path.exists(output_path):
            try:
                os.remove(output_path)
            except Exception as e:
                self.logger.error(f"Cleanup failed for {output_path}: {str(e)}")

    def _move_to_failed(self, input_path):
        try:
            failed_path = os.path.join(self.config['FAILED_DIR'], os.path.basename(input_path))
            shutil.move(input_path, failed_path)
        except Exception as e:
            self.logger.error(f"Failed to move {input_path} to failed dir: {str(e)}")

    def _move_to_videos(self, input_path):
        try:
            videos_subdir = os.path.join(os.path.dirname(input_path), ".videos")
            os.makedirs(videos_subdir, exist_ok=True)
            shutil.move(input_path, os.path.join(videos_subdir, os.path.basename(input_path)))
        except Exception as e:
            self.logger.error(f"Failed to move {input_path} to .videos: {str(e)}")

    def worker(self):
        while True:
            try:
                input_path = self.queue.get()
                if input_path is None:
                    break
                self.process_video(input_path)
            except Exception as e:
                self.logger.error(f"Worker error: {str(e)}")
            finally:
                self.queue.task_done()

# --- Utility Functions ---
def monitor_queue_and_stats(queue, logger, stats_manager):
    while True:
        logger.info(f"Queue: {queue.qsize()} videos waiting")
        month_key = datetime.now().strftime("%Y-%m")
        logger.info(stats_manager.format_stats(
            stats_manager.stats.get(month_key, {
                "files_converted": 0, "failed_encodings": 0,
                "total_space_saved_bytes": 0, "total_input_size_bytes": 0,
                "total_output_size_bytes": 0, "total_encoding_time_seconds": 0
            }), f"Monthly ({month_key})"))
        logger.info(stats_manager.format_stats(stats_manager.get_lifetime_stats(), "Lifetime"))
        time.sleep(60)

def scan_existing_files(queue, config, logger, processed_output_files):
    excluded_dirs = config['SECLUDED_FOLDERS'] | {'.videos', os.path.basename(config['FAILED_DIR']).lower()}
    suffix_pattern = config['OUTPUT_SUFFIX'].replace('{date}', r'\d{8}')

    for root, dirs, files in os.walk(config['WATCH_DIR']):
        dirs[:] = [d for d in dirs if d.lower() not in excluded_dirs]
        for file in files:
            file_path = os.path.join(root, file)
            file_ext = os.path.splitext(file)[1].lower()
            filename = os.path.splitext(file)[0]

            if (file_ext not in config['VIDEO_EXTENSIONS'] or
                any(part.lower() in excluded_dirs for part in os.path.relpath(file_path, config['WATCH_DIR']).split(os.sep)[:-1]) or
                re.search(rf"{suffix_pattern}(?:\.mp4)?$", filename) or
                file_path in processed_output_files):
                continue

            file_size = os.path.getsize(file_path)
            if not (config['MIN_VIDEO_SIZE'] <= file_size <= config['MAX_VIDEO_SIZE']):
                continue

            logger.info(f"Found video: {file}")
            queue.put(file_path)

# --- Main Program ---
def start_service():
    config = load_config()
    logger = setup_logger(config['LOG_FILE'], config['MAX_LOG_SIZE'], config['BACKUP_COUNT'])
    stats_manager = StatsManager("stats.json", logger)
    os.makedirs(config['FAILED_DIR'], exist_ok=True)

    ffmpeg_cmd = config['FFMPEG_PATH']
    try:
        subprocess.run([ffmpeg_cmd, "-version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        ffmpeg_cmd = config['FFMPEG_FALLBACK_PATH']
        try:
            subprocess.run([ffmpeg_cmd, "-version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError(f"FFmpeg not found or not working at {ffmpeg_cmd}")

    os.makedirs(config['WATCH_DIR'], exist_ok=True)
    logger.info(f"Starting video encoding service v{config['VERSION']} at {config['WATCH_DIR']}")

    queue = Queue()
    processed_output_files = set()
    encoder = VideoEncoder(queue, config, logger, processed_output_files, stats_manager)
    
    scan_existing_files(queue, config, logger, processed_output_files)

    workers = [Thread(target=encoder.worker, daemon=True) for _ in range(config['MAX_CONCURRENT_JOBS'])]
    for i, w in enumerate(workers):
        w.start()
        logger.info(f"Started worker {i + 1}/{config['MAX_CONCURRENT_JOBS']}")

    monitor_thread = Thread(target=monitor_queue_and_stats, args=(queue, logger, stats_manager), daemon=True)
    monitor_thread.start()

    observer = Observer()
    observer.schedule(VideoHandler(queue, config, logger, processed_output_files), config['WATCH_DIR'], recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        observer.stop()
        for _ in range(config['MAX_CONCURRENT_JOBS']):
            queue.put(None)
        for w in workers:
            w.join()
        observer.join()

if __name__ == "__main__":
    try:
        start_service()
    except Exception as e:
        print(f"Service failed: {str(e)}")
        raise