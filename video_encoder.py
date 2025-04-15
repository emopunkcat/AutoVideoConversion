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
    raise ImportError("ffmpeg-python not installed. Please install with 'pip install ffmpeg-python'")

# --- Configuration Loading ---
def load_config():
    config = configparser.ConfigParser()
    config_file = "video_encoder.conf"

    if not os.path.exists(config_file):
        print(f"Configuration file '{config_file}' not found in {os.getcwd()}")
        raise FileNotFoundError(f"Configuration file '{config_file}' not found")

    print(f"Loading configuration from {config_file}")
    config.read(config_file)

    if not config.sections():
        print(f"Configuration file '{config_file}' is empty or invalid. It must contain sections like [General]")
        raise ValueError(f"Configuration file '{config_file}' is empty or invalid")

    try:
        # General settings
        if 'General' not in config:
            print("Missing [General] section in configuration file")
            raise KeyError("Missing [General] section")
        config_data = {
            'VERSION': config['General']['version'],
            'WATCH_DIR': config['General']['watch_dir'],
            'LOG_FILE': config['General']['log_file'],
            'MAX_LOG_SIZE': int(config['General']['max_log_size_mb']) * 1024 * 1024,
            'BACKUP_COUNT': int(config['General']['backup_count']),
            'MAX_CONCURRENT_JOBS': int(config['General']['max_concurrent_jobs']),
            'FAILED_DIR': config['General']['failed_dir'],
            'MAX_RETRIES': int(config['General']['max_retries']),
        }

        # File settings
        if 'FileSettings' not in config:
            print("Missing [FileSettings] section in configuration file")
            raise KeyError("Missing [FileSettings] section")
        config_data.update({
            'VIDEO_EXTENSIONS': tuple(ext.strip().lower() for ext in config['FileSettings']['video_extensions'].split(',')),
            'MIN_VIDEO_SIZE': int(config['FileSettings']['min_video_size_mb']) * 1024 * 1024,
            'MAX_VIDEO_SIZE': int(config['FileSettings']['max_video_size_mb']) * 1024 * 1024,
            'OUTPUT_SUFFIX': config['FileSettings']['output_suffix'],
        })

        # FFmpeg settings
        if 'FFmpegSettings' not in config:
            print("Missing [FFmpegSettings] section in configuration file")
            raise KeyError("Missing [FFmpegSettings] section")
        config_data.update({
            'FFMPEG_PATH': config['FFmpegSettings']['ffmpeg_path'],
            'FFMPEG_FALLBACK_PATH': config['FFmpegSettings']['ffmpeg_fallback_path'],
        })

        # Encoder settings
        if 'EncoderSettings' not in config:
            print("Missing [EncoderSettings] section in configuration file")
            raise KeyError("Missing [EncoderSettings] section")
        config_data['ENCODER_SETTINGS'] = {
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
        }

        # Folder settings
        if 'FolderSettings' not in config:
            print("Missing [FolderSettings] section in configuration file")
            raise KeyError("Missing [FolderSettings] section")
        config_data['SECLUDED_FOLDERS'] = set(folder.strip().lower() for folder in config['FolderSettings']['secluded_folders'].split(','))

        return config_data
    except KeyError as e:
        print(f"Configuration error: {str(e)}")
        raise KeyError(f"Missing configuration key: {str(e)}")
    except ValueError as e:
        print(f"Configuration error: {str(e)}")
        raise ValueError(f"Invalid configuration value: {str(e)}")

# --- Logger Setup ---
def setup_logger(log_file, max_log_size, backup_count):
    logger = logging.getLogger('VideoEncoder')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # File handler for detailed logging
    file_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler for basic output
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    return logger

# --- Statistics Management ---
class StatsManager:
    def __init__(self, stats_file, logger):
        self.stats_file = stats_file
        self.logger = logger
        self.stats = self.load_stats()

    def load_stats(self):
        if not os.path.exists(self.stats_file):
            self.logger.info(f"Stats file {self.stats_file} not found, initializing new stats")
            return {}

        try:
            with open(self.stats_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading stats from {self.stats_file}: {str(e)}")
            return {}

    def save_stats(self):
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats, f, indent=4)
            self.logger.debug(f"Updated stats saved to {self.stats_file}")
        except Exception as e:
            self.logger.error(f"Error saving stats to {self.stats_file}: {str(e)}")

    def get_month_key(self):
        return datetime.now().strftime("%Y-%m")

    def update_stats(self, input_size, output_size, encoding_time, success=True):
        month_key = self.get_month_key()
        
        if month_key not in self.stats:
            self.stats[month_key] = {
                "files_converted": 0,
                "failed_encodings": 0,
                "total_space_saved_bytes": 0,
                "total_input_size_bytes": 0,
                "total_output_size_bytes": 0,
                "total_encoding_time_seconds": 0
            }

        month_stats = self.stats[month_key]
        
        if success:
            month_stats["files_converted"] += 1
            space_saved = input_size - output_size if output_size > 0 else 0
            month_stats["total_space_saved_bytes"] += space_saved
            month_stats["total_input_size_bytes"] += input_size
            month_stats["total_output_size_bytes"] += output_size
            month_stats["total_encoding_time_seconds"] += encoding_time
        else:
            month_stats["failed_encodings"] += 1

        self.save_stats()

    def get_lifetime_stats(self):
        lifetime_stats = {
            "files_converted": 0,
            "failed_encodings": 0,
            "total_space_saved_bytes": 0,
            "total_input_size_bytes": 0,
            "total_output_size_bytes": 0,
            "total_encoding_time_seconds": 0
        }

        for month_stats in self.stats.values():
            for key in lifetime_stats:
                lifetime_stats[key] += month_stats[key]

        return lifetime_stats

    def format_stats(self, stats, label):
        return (
            f"{label} Stats:\n"
            f"  Files Converted: {stats['files_converted']}\n"
            f"  Failed Encodings: {stats['failed_encodings']}\n"
            f"  Total Space Saved: {stats['total_space_saved_bytes'] / (1024 * 1024):.2f} MB\n"
            f"  Total Input Size: {stats['total_input_size_bytes'] / (1024 * 1024):.2f} MB\n"
            f"  Total Output Size: {stats['total_output_size_bytes'] / (1024 * 1024):.2f} MB\n"
            f"  Total Encoding Time: {stats['total_encoding_time_seconds'] / 3600:.2f} hours"
        )

# --- Video Handler ---
class VideoHandler(FileSystemEventHandler):
    def __init__(self, queue, config, logger, processed_output_files):
        self.queue = queue
        self.config = config
        self.logger = logger
        self.processed_output_files = processed_output_files
        self.excluded_dirs = set(self.config['SECLUDED_FOLDERS']).union({'.videos', os.path.basename(self.config['FAILED_DIR']).lower()})

    def on_created(self, event):
        if event.is_directory:
            return
        
        file_path = event.src_path
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext not in self.config['VIDEO_EXTENSIONS']:
            return
        
        relative_path = os.path.relpath(file_path, self.config['WATCH_DIR'])
        folder_parts = relative_path.split(os.sep)
        if any(part.lower() in self.excluded_dirs for part in folder_parts[:-1]):
            self.logger.debug(f"Skipping file in excluded directory: {file_path}")
            return
        
        time.sleep(1)
        if not os.path.exists(file_path):
            self.logger.info(f"File no longer exists: {file_path}")
            return

        filename = os.path.splitext(os.path.basename(file_path))[0]
        
        suffix_pattern = self.config['OUTPUT_SUFFIX'].replace('{date}', r'\d{8}')
        full_pattern = rf"{suffix_pattern}\.mp4$"
        if re.search(suffix_pattern, filename) or re.search(full_pattern, file_path):
            self.logger.info(f"Skipping already encoded file: {file_path}")
            return
        
        if file_path in self.processed_output_files:
            self.logger.info(f"Skipping processed output file: {file_path}")
            return
        
        file_size = os.path.getsize(file_path)
        if file_size < self.config['MIN_VIDEO_SIZE']:
            self.logger.info(f"File too small ({file_size / (1024 * 1024):.2f} MB): {file_path}")
            return
        if file_size > self.config['MAX_VIDEO_SIZE']:
            self.logger.info(f"File too large ({file_size / (1024 * 1024):.2f} MB): {file_path}")
            return

        self.logger.info(f"New video detected: {os.path.basename(file_path)}")
        self.queue.put(file_path)

# --- Video Encoder ---
# --- Video Encoder ---
class VideoEncoder:
    def __init__(self, queue, config, logger, processed_output_files, stats_manager):
        self.queue = queue
        self.config = config
        self.logger = logger
        self.processed_files = set()
        self.processed_output_files = processed_output_files
        self.stats_manager = stats_manager
        self.batch_size = 5
        self.batch_delay = 10
        self.current_batch = []

    def process_video(self, input_path, retry_count=0):
        if input_path in self.processed_files:
            self.logger.info(f"Skipping already processed file: {input_path}")
            return

        self.processed_files.add(input_path)
        output_path = None
        input_size = 0
        output_size = 0
        encoding_time = 0
        success = False

        try:
            time.sleep(2)
            if not os.path.exists(input_path):
                self.logger.error(f"Input file not found: {input_path}")
                return

            input_size = os.path.getsize(input_path)

            current_date = datetime.now().strftime("%Y%m%d")
            directory = os.path.dirname(input_path)
            filename = os.path.splitext(os.path.basename(input_path))[0]
            output_suffix = self.config['OUTPUT_SUFFIX'].format(date=current_date)
            output_path = os.path.join(directory, f"{filename}{output_suffix}.mp4")
            
            if os.path.exists(output_path):
                self.logger.info(f"Skipping - encoded file already exists: {output_path}")
                return

            self.logger.info(f"Starting encoding: {input_path} to {output_path} (Attempt {retry_count + 1}/{self.config['MAX_RETRIES'] + 1})")
            
            probe = ffmpeg_python.probe(input_path)
            audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)
            
            encoder_args = self.config['ENCODER_SETTINGS'].copy()
            
            if audio_stream and audio_stream.get('codec_name') == 'aac':
                encoder_args['acodec'] = 'copy'
                self.logger.info("Input audio is AAC, copying instead of re-encoding")

            start_time = time.time()
            stream = ffmpeg_python.input(input_path)
            stream = stream.output(output_path, **encoder_args)
            stream.run(quiet=True, overwrite_output=True)
            encoding_time = time.time() - start_time
            
            output_size = os.path.getsize(output_path)
            self.logger.info(f"Successfully encoded: {output_path} in {encoding_time:.2f} seconds")
            
            self.processed_output_files.add(output_path)
            self.move_to_videos(input_path)
            success = True

        except ffmpeg_python.Error as e:
            self.logger.error(f"FFmpeg error encoding {input_path}: {str(e.stderr.decode() if e.stderr else str(e))}")
            self.cleanup_failed(output_path)
            if retry_count < self.config['MAX_RETRIES']:
                self.logger.info(f"Retrying {input_path} (Attempt {retry_count + 2}/{self.config['MAX_RETRIES'] + 1})")
                time.sleep(5)
                self.process_video(input_path, retry_count + 1)
            else:
                self.logger.error(f"Failed to encode {input_path} after {self.config['MAX_RETRIES'] + 1} attempts. Moving to failed directory.")
                self.move_to_failed(input_path)
        except Exception as e:
            self.logger.error(f"Unexpected error processing {input_path}: {str(e)}", exc_info=True)
            self.cleanup_failed(output_path)
            if retry_count < self.config['MAX_RETRIES']:
                self.logger.info(f"Retrying {input_path} (Attempt {retry_count + 2}/{self.config['MAX_RETRIES'] + 1})")
                time.sleep(5)
                self.process_video(input_path, retry_count + 1)
            else:
                self.logger.error(f"Failed to encode {input_path} after {self.config['MAX_RETRIES'] + 1} attempts. Moving to failed directory.")
                self.move_to_failed(input_path)
        finally:
            self.processed_files.remove(input_path)
            self.stats_manager.update_stats(input_size, output_size, encoding_time, success)

    def cleanup_failed(self, output_path):
        try:
            if output_path and os.path.exists(output_path):
                os.remove(output_path)
                self.logger.info(f"Cleaned up failed encode: {output_path}")
        except Exception as e:
            self.logger.error(f"Error cleaning up {output_path}: {str(e)}")

    def move_to_failed(self, input_path):
        try:
            failed_path = os.path.join(self.config['FAILED_DIR'], os.path.basename(input_path))
            shutil.move(input_path, failed_path)
            self.logger.info(f"Moved failed video to {failed_path}")
        except Exception as e:
            self.logger.error(f"Error moving {input_path} to failed directory: {str(e)}")

    def move_to_videos(self, input_path):
        try:
            video_dir = os.path.dirname(input_path)
            videos_subdir = os.path.join(video_dir, ".videos")
            os.makedirs(videos_subdir, exist_ok=True)

            video_name = os.path.basename(input_path)
            new_path = os.path.join(videos_subdir, video_name)
            shutil.move(input_path, new_path)
            self.logger.info(f"Moved original video to {new_path}")
        except Exception as e:
            self.logger.error(f"Error moving {input_path} to .videos directory: {str(e)}")

    def worker(self):
        self.logger.info("Worker thread started")
        while True:
            try:
                self.logger.debug("Waiting for item in queue...")
                input_path = self.queue.get()
                if input_path is None:
                    self.logger.info("Received shutdown signal, worker thread exiting")
                    break
                self.logger.info(f"Dequeued file for processing: {input_path}")
                self.process_video(input_path)  # Process immediately, no batching for now
                self.queue.task_done()
            except Exception as e:
                self.logger.error(f"Worker error: {str(e)}", exc_info=True)
                self.queue.task_done()  # Ensure the queue is unblocked even on error
# --- Utility Functions ---
def monitor_queue_and_stats(queue, logger, stats_manager):
    while True:
        logger.debug(f"Queue size check: {queue.qsize()} videos waiting")
        logger.info(f"Queue size: {queue.qsize()} videos waiting")

        month_key = stats_manager.get_month_key()
        monthly_stats = stats_manager.stats.get(month_key, {
            "files_converted": 0,
            "failed_encodings": 0,
            "total_space_saved_bytes": 0,
            "total_input_size_bytes": 0,
            "total_output_size_bytes": 0,
            "total_encoding_time_seconds": 0
        })
        lifetime_stats = stats_manager.get_lifetime_stats()

        logger.info(stats_manager.format_stats(monthly_stats, f"Monthly ({month_key})"))
        logger.info(stats_manager.format_stats(lifetime_stats, "Lifetime"))

        time.sleep(60)

# --- Main Program ---
def scan_existing_files(queue, config, logger, processed_output_files):
    logger.info(f"Scanning {config['WATCH_DIR']} for existing videos...")
    excluded_dirs = set(config['SECLUDED_FOLDERS']).union({'.videos', os.path.basename(config['FAILED_DIR']).lower()})
    suffix_pattern = config['OUTPUT_SUFFIX'].replace('{date}', r'\d{8}')
    full_pattern = rf"{suffix_pattern}\.mp4$"

    for root, dirs, files in os.walk(config['WATCH_DIR']):
        dirs[:] = [d for d in dirs if d.lower() not in excluded_dirs]
        
        for file in files:
            file_path = os.path.join(root, file)
            file_ext = os.path.splitext(file_path)[1].lower()
            filename = os.path.splitext(file)[0]

            if file_ext not in config['VIDEO_EXTENSIONS']:
                continue

            relative_path = os.path.relpath(file_path, config['WATCH_DIR'])
            folder_parts = relative_path.split(os.sep)
            if any(part.lower() in excluded_dirs for part in folder_parts[:-1]):
                logger.debug(f"Skipping file in excluded directory during scan: {file_path}")
                continue

            if re.search(suffix_pattern, filename) or re.search(full_pattern, file_path):
                logger.info(f"Skipping already encoded file: {file_path}")
                continue

            if file_path in processed_output_files:
                logger.info(f"Skipping processed output file: {file_path}")
                continue

            file_size = os.path.getsize(file_path)
            if file_size < config['MIN_VIDEO_SIZE']:
                logger.info(f"File too small during scan ({file_size / (1024 * 1024):.2f} MB): {file_path}")
                continue
            if file_size > config['MAX_VIDEO_SIZE']:
                logger.info(f"File too large during scan ({file_size / (1024 * 1024):.2f} MB): {file_path}")
                continue

            logger.info(f"Found existing video: {os.path.basename(file_path)}")
            queue.put(file_path)

def start_service():
    config = load_config()
    logger = setup_logger(config['LOG_FILE'], config['MAX_LOG_SIZE'], config['BACKUP_COUNT'])
    stats_manager = StatsManager("stats.json", logger)

    os.makedirs(config['FAILED_DIR'], exist_ok=True)

    ffmpeg_cmd = config['FFMPEG_PATH']
    try:
        subprocess.run([ffmpeg_cmd, "-version"], capture_output=True, check=True)
        logger.info("FFmpeg found at primary path")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.warning(f"FFmpeg not found at primary path, trying fallback")
        ffmpeg_cmd = config['FFMPEG_FALLBACK_PATH']
        try:
            subprocess.run([ffmpeg_cmd, "-version"], capture_output=True, check=True)
            logger.info("FFmpeg found at fallback path")
        except subprocess.CalledProcessError:
            logger.error("FFmpeg not working correctly. Please ensure FFmpeg is installed")
            raise
        except FileNotFoundError:
            logger.error(f"FFmpeg executable not found at '{ffmpeg_cmd}'. Verify the FFMPEG_PATH or FFMPEG_FALLBACK_PATH")
            raise

    os.makedirs(config['WATCH_DIR'], exist_ok=True)
    logger.info(f"Starting video encoding service v{config['VERSION']}")
    logger.info(f"Monitoring directory: {config['WATCH_DIR']}")

    queue = Queue()
    processed_output_files = set()
    encoder = VideoEncoder(queue, config, logger, processed_output_files, stats_manager)
    
    scan_existing_files(queue, config, logger, processed_output_files)

    workers = []
    for i in range(config['MAX_CONCURRENT_JOBS']):
        t = Thread(target=encoder.worker, daemon=True)
        t.start()
        logger.info(f"Started worker thread {i + 1}/{config['MAX_CONCURRENT_JOBS']}")
        workers.append(t)

    # Verify workers are alive
    time.sleep(1)  # Give threads a moment to start
    for i, worker in enumerate(workers):
        if not worker.is_alive():
            logger.error(f"Worker thread {i + 1} failed to start or has already exited")

    queue_monitor = Thread(target=monitor_queue_and_stats, args=(queue, logger, stats_manager), daemon=True)
    queue_monitor.start()

    event_handler = VideoHandler(queue, config, logger, processed_output_files)
    observer = Observer()
    observer.schedule(event_handler, config['WATCH_DIR'], recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down service...")
        observer.stop()
        for _ in range(config['MAX_CONCURRENT_JOBS']):
            queue.put(None)
        for w in workers:
            w.join()
        observer.join()
        logger.info("Service stopped")

if __name__ == "__main__":
    try:
        start_service()
    except Exception as e:
        print(f"Service failed to start: {str(e)}")
        raise