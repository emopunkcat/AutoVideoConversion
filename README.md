# Auto Video Converter

Monitors a Folder/Drive/Share for new video files and re-encodes them to a more compressed and accessible format.

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

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
