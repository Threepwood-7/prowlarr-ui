"""Background worker for queued download operations"""
import logging
import threading
from typing import List, Dict
from PySide6.QtCore import QThread, Signal

from src.api.prowlarr_client import ProwlarrClient

logger = logging.getLogger(__name__)


class DownloadWorker(QThread):
    """
    Background worker that processes a queue of downloads sequentially.
    Supports adding new items while the queue is running.
    Prevents UI freezing during batch download operations.
    """

    # Qt signals for thread communication
    progress = Signal(int, int, str)        # (current, total, title)
    item_downloaded = Signal(str, bool)     # (guid, success)
    queue_done = Signal()                   # All downloads complete (avoid QThread.finished clash)

    def __init__(self, client: ProwlarrClient, items: List[Dict]):
        """
        Args:
            client: ProwlarrClient instance
            items: List of dicts with keys: guid, indexer_id, title
        """
        super().__init__()
        self.client = client
        self._lock = threading.Lock()
        self.items = list(items)

    def add_items(self, new_items: List[Dict]):
        """Thread-safe append of new items to the queue while running."""
        with self._lock:
            self.items.extend(new_items)

    def run(self):
        """Process download queue sequentially, picking up newly added items"""
        idx = 0
        while True:
            with self._lock:
                if idx >= len(self.items):
                    break
                item = self.items[idx]

            guid = item['guid']
            indexer_id = item['indexer_id']
            title = item['title']

            try:
                with self._lock:
                    total = len(self.items)
                self.progress.emit(idx + 1, total, title)
            except Exception as e:
                logger.error(f"Failed to emit progress signal: {e}")

            logger.info(f"Downloading {idx + 1}/{total}: {title}")

            try:
                success = self.client.download(guid, indexer_id)
                try:
                    self.item_downloaded.emit(guid, success)
                except Exception as e:
                    logger.error(f"Failed to emit item_downloaded signal: {e}")
                if success:
                    logger.info(f"Download {idx + 1}/{total} succeeded: {title}")
                else:
                    logger.warning(f"Download {idx + 1}/{total} failed: {title}")
            except Exception as e:
                logger.error(f"Download {idx + 1}/{total} error: {title} - {e}")
                try:
                    self.item_downloaded.emit(guid, False)
                except Exception as emit_error:
                    logger.error(f"Failed to emit error item_downloaded signal: {emit_error}")

            idx += 1

        try:
            self.queue_done.emit()
        except Exception as e:
            logger.error(f"Failed to emit queue_done signal: {e}")
