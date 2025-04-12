import os
import configparser

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