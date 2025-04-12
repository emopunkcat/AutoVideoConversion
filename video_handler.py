import os
import re
import time
from watchdog.events import FileSystemEventHandler

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
        
        # Early filter: Check file extension first
        if file_ext not in self.config['VIDEO_EXTENSIONS']:
            return
        
        # Check if the file is in an excluded directory
        relative_path = os.path.relpath(file_path, self.config['WATCH_DIR'])
        folder_parts = relative_path.split(os.sep)
        if any(part.lower() in self.excluded_dirs for part in folder_parts[:-1]):
            self.logger.debug(f"Skipping file in excluded directory: {file_path}")
            return
        
        # Debounce: Wait briefly and re-check if the file still exists
        time.sleep(1)
        if not os.path.exists(file_path):
            self.logger.info(f"File no longer exists, skipping: {file_path}")
            return

        filename = os.path.splitext(os.path.basename(file_path))[0]
        
        # Check if the file matches the output naming pattern
        suffix_pattern = self.config['OUTPUT_SUFFIX'].replace('{date}', r'\d{8}')
        full_pattern = rf"{suffix_pattern}\.mp4$"
        if re.search(suffix_pattern, filename) or re.search(full_pattern, file_path):
            self.logger.info(f"Skipping file that matches output pattern: {file_path}")
            return
        
        # Check if the file is an already processed output file
        if file_path in self.processed_output_files:
            self.logger.info(f"Skipping already processed output file: {file_path}")
            return
        
        # Check file size
        file_size = os.path.getsize(file_path)
        if file_size < self.config['MIN_VIDEO_SIZE']:
            self.logger.info(f"Skipping file too small ({file_size} bytes < {self.config['MIN_VIDEO_SIZE']} bytes): {file_path}")
            return
        if file_size > self.config['MAX_VIDEO_SIZE']:
            self.logger.info(f"Skipping file too large ({file_size} bytes > {self.config['MAX_VIDEO_SIZE']} bytes): {file_path}")
            return

        self.logger.info(f"New video detected: {file_path}")
        self.queue.put(file_path)