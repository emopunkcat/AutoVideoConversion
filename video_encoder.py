import os
import time
import shutil
from datetime import datetime
try:
    import ffmpeg as ffmpeg_python
except ImportError:
    raise ImportError("ffmpeg-python not installed. Please install with 'pip install ffmpeg-python'")

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
            ffmpeg_python.input(input_path).output(
                output_path,
                **encoder_args
            ).run(quiet=False, overwrite_output=True)
            encoding_time = time.time() - start_time
            
            output_size = os.path.getsize(output_path)
            self.logger.info(f"Successfully encoded: {output_path}")
            
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
            # Update stats after each attempt
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
        while True:
            try:
                input_path = self.queue.get()
                if input_path is None:
                    break
                self.current_batch.append(input_path)
                
                if len(self.current_batch) >= self.batch_size:
                    for path in self.current_batch:
                        self.process_video(path)
                    self.current_batch = []
                    self.logger.info(f"Processed batch, waiting {self.batch_delay} seconds before next batch")
                    time.sleep(self.batch_delay)
                
                self.queue.task_done()
            except Exception as e:
                self.logger.error(f"Worker error: {str(e)}", exc_info=True)