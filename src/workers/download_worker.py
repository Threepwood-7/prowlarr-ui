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
    item_downloaded = Signal(str, int, bool)     # (guid, indexer_id, success)
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
        self._queued_keys = set()
        self.items = []
        # Normalize initial queue to unique items so duplicates are never processed twice.
        for item in items:
            key = self._item_key(item)
            if key in self._queued_keys:
                continue
            self._queued_keys.add(key)
            self.items.append(item)

    @staticmethod
    def _item_key(item: Dict):
        """Stable queue identity for deduplication."""
        return item.get('guid'), item.get('indexer_id')

    def add_items(self, new_items: List[Dict]) -> List[Dict]:
        """Thread-safe append of unique new items to the queue while running."""
        added = []
        with self._lock:
            for item in new_items:
                key = self._item_key(item)
                if key in self._queued_keys:
                    continue
                self._queued_keys.add(key)
                self.items.append(item)
                added.append(item)
        return added

    def run(self):
        """Process download queue sequentially, picking up newly added items"""
        idx = 0
        while True:
            with self._lock:
                if self.isInterruptionRequested():
                    logger.info("DownloadWorker interruption requested, stopping queue")
                    break
                if idx >= len(self.items):
                    break
                item = self.items[idx]

            guid = item['guid']
            indexer_id = item['indexer_id']
            title = item['title']

            if self.isInterruptionRequested():
                logger.info("DownloadWorker interrupted before item download")
                break

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
                    # Emit composite identity to disambiguate duplicate GUIDs across indexers.
                    self.item_downloaded.emit(guid, int(indexer_id), success)
                except Exception as e:
                    logger.error(f"Failed to emit item_downloaded signal: {e}")
                if success:
                    logger.info(f"Download {idx + 1}/{total} succeeded: {title}")
                else:
                    logger.warning(f"Download {idx + 1}/{total} failed: {title}")
            except Exception as e:
                logger.error(f"Download {idx + 1}/{total} error: {title} - {e}")
                try:
                    self.item_downloaded.emit(guid, int(indexer_id), False)
                except Exception as emit_error:
                    logger.error(f"Failed to emit error item_downloaded signal: {emit_error}")

            idx += 1

        try:
            self.queue_done.emit()
        except Exception as e:
            logger.error(f"Failed to emit queue_done signal: {e}")
