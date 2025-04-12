# Auto Video Converter

Monitors a Folder/Drive/Share for new video files and re-encodes them to a more compressed and accessible format.

- Automated Video Encoding with FFmpeg (libx264): Encodes video files automatically using FFmpeg with the libx264 encoder.
- Real-Time File Server Monitoring with watchdog: Monitors a file server for new video files in real-time using the watchdog library.
- Initial Scan for Existing Video Files: Scans the file server at startup to process existing video files, not just new ones.
- Configuration-Driven via video_encoder.conf: Loads settings from a configuration file for easy customization.
- Customizable FFmpeg Encoding Settings: Supports H.264 encoding with settings like Baseline profile, Level 3.1, CRF 29, and ultrafast preset.
- Smart Audio Handling: Copies AAC audio if present in the input, avoiding unnecessary re-encoding.
- Output File Naming with Date Suffix: Names output files with a configurable suffix (e.g., _encoded_20250411.mp4).
- Original File Archiving to .videos Subdirectory: Moves original videos to a .videos subdirectory after encoding.
- Failed File Handling: Moves failed encodings to a FAILED_DIR after a configurable number of retries.
- File Filtering: Skips ineligible files based on extensions, size, secluded folders, and output naming pattern.
- Prevention of Re-encoding Already Encoded Files: Skips files that match the output pattern or have been processed.
- Concurrent Processing with Configurable Job Limits: Supports multiple concurrent encoding jobs (default: 2).
- Batch Processing: Processes videos in batches (5 files per batch, 10-second delay) to manage system load.
- Efficient Directory Monitoring: Excludes .videos, Failed, and secluded folders to reduce event noise.
- Resource Management: Limits concurrent jobs and uses batch delays to prevent system overload.
- Initial Scan Optimization: Skips excluded directories during the initial scan to reduce I/O overhead.
- Retry Mechanism for Failed Encodings: Retries failed encodings up to 2 times with a 5-second delay.
- Robust Error Handling: Handles FFmpeg errors, file operation errors, and unexpected exceptions.
- FFmpeg Installation Check with Fallback Path: Verifies FFmpeg availability at startup with a primary and fallback path.
- Dependency Check for ffmpeg-python: Ensures the ffmpeg-python library is installed.
- Detailed Logging: Logs all actions to a file (video_encoder.log) and console with rotating file handling.
- Queue Monitoring: Logs the queue size every 60 seconds to monitor processing backlog.
- Performance Monitoring: Logs batch processing events and debug messages for performance analysis.
- Device Compatibility: Uses H.264 Baseline profile, Level 3.1, YUV 4:2:0, MP4 container, and AAC audio for broad compatibility.
- Network File System Support: Supports monitoring network shares (e.g., SMB/NFS) with proper debouncing.
- Modular Design: Code is split into multiple files (config.py, logger.py, video_handler.py, etc.) for maintainability.
- Extensibility: Easy to add new features by extending existing modules.
- Lifetime Statistics Tracking: Tracks total files converted, failed encodings, space saved, input/output sizes, and encoding time.
- Persistent Stats Storage: Saves stats to stats.json, organized by month (e.g., 2025-04).
- Periodic Stats Logging: Logs monthly and lifetime stats every 60 seconds in a human-readable format.



## Installation

```bash
pip install ffmpeg_python
```

## Usage

Modify 
- video_encoder.conf
- Change the ```watch_dir = C:\VIDEO``` to whatever path youd like to monitor
- Add folder name exlcusions at the bottom
- Modify encoder settings
- FFMPEG executable path (If not specified it will fall back to SYSTEM path)

After configuration changes juts run ```main.py```
## License

[MIT](https://choosealicense.com/licenses/mit/)
