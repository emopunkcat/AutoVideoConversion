import os
import subprocess
import time
import re
from queue import Queue
from threading import Thread
from watchdog.observers import Observer
from config import load_config
from logger import setup_logger
from video_handler import VideoHandler
from video_encoder import VideoEncoder
from utils import monitor_queue_and_stats
from stats import StatsManager

def scan_existing_files(queue, config, logger, processed_output_files):
    """Scan the file server for existing video files and add them to the queue."""
    logger.info(f"Starting initial scan of {config['WATCH_DIR']} for existing video files")
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
                logger.info(f"Skipping file that matches output pattern during scan: {file_path}")
                continue

            if file_path in processed_output_files:
                logger.info(f"Skipping already processed output file during scan: {file_path}")
                continue

            file_size = os.path.getsize(file_path)
            if file_size < config['MIN_VIDEO_SIZE']:
                logger.info(f"Skipping file too small during scan ({file_size} bytes < {config['MIN_VIDEO_SIZE']} bytes): {file_path}")
                continue
            if file_size > config['MAX_VIDEO_SIZE']:
                logger.info(f"Skipping file too large during scan ({file_size} bytes > {config['MAX_VIDEO_SIZE']} bytes): {file_path}")
                continue

            logger.info(f"Found existing video during scan: {file_path}")
            queue.put(file_path)

def start_service():
    # Load configuration
    config = load_config()

    # Set up logger
    logger = setup_logger(config['LOG_FILE'], config['MAX_LOG_SIZE'], config['BACKUP_COUNT'])

    # Initialize stats manager
    stats_manager = StatsManager("stats.json", logger)

    # Ensure failed directory exists
    os.makedirs(config['FAILED_DIR'], exist_ok=True)

    # Check FFmpeg installation
    ffmpeg_cmd = config['FFMPEG_PATH']
    try:
        subprocess.run([ffmpeg_cmd, "-version"], capture_output=True, check=True)
        logger.info("FFmpeg found at primary path")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.warning(f"FFmpeg not found at primary path '{ffmpeg_cmd}', trying fallback")
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

    # Start the service
    os.makedirs(config['WATCH_DIR'], exist_ok=True)
    logger.info(f"Starting video encoding service v{config['VERSION']} for {config['WATCH_DIR']}")

    queue = Queue()
    processed_output_files = set()
    encoder = VideoEncoder(queue, config, logger, processed_output_files, stats_manager)
    
    # Scan for existing files
    scan_existing_files(queue, config, logger, processed_output_files)

    workers = []
    for _ in range(config['MAX_CONCURRENT_JOBS']):
        t = Thread(target=encoder.worker, daemon=True)
        t.start()
        workers.append(t)

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