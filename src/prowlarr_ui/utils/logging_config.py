"""Logging configuration for the application"""

import logging
import tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Keep runtime log artifacts in the OS temp area (not the current working dir).
RUNTIME_DIR = Path(tempfile.gettempdir()) / "prowlarr-ui"
LOG_FILE_PATH = str(RUNTIME_DIR / "prowlarr_ui.log")
DOWNLOAD_HISTORY_PATH = str(RUNTIME_DIR / "download_history.log")


def setup_logging():
    """Configure logging to file and console with 24MB rotating file"""
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=24 * 1024 * 1024,  # 24 MB
        backupCount=1,  # Keep 1 backup file
    )
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Less verbose console output

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, console_handler])

    logger = logging.getLogger(__name__)
    logger.info("Logging initialized")
    return logger
