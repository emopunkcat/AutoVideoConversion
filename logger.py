import logging
from logging.handlers import RotatingFileHandler

def setup_logger(log_file, max_log_size, backup_count):
    logger = logging.getLogger('VideoEncoder')
    logger.setLevel(logging.DEBUG)  # Enable debug logging
    logger.handlers.clear()

    file_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger