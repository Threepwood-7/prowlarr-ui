"""Logging configuration for the application"""
import os
import logging
from logging.handlers import RotatingFileHandler

# Anchor log file to project root (two levels up from src/utils/)
LOG_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'prowlarr_ui.log')


def setup_logging():
    """Configure logging to file and console with 24MB rotating file"""
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=24*1024*1024,  # 24 MB
        backupCount=1  # Keep 1 backup file
    )
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Less verbose console output

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[file_handler, console_handler]
    )

    logger = logging.getLogger(__name__)
    logger.info("Logging initialized")
    return logger
