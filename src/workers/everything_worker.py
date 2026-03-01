"""Background worker for Everything search operations"""
import logging
import threading
from typing import List, Dict, Optional
from PySide6.QtCore import QThread, Signal

from src.api.everything_search import EverythingSearch

logger = logging.getLogger(__name__)


class EverythingCheckWorker(QThread):
    """
    Background worker thread for checking Everything existence
    Emits results in batches to avoid flooding the main thread
    Thread-safe: uses lock to prevent concurrent SDK access
    """

    # Qt signals for thread communication
    batch_ready = Signal(list)   # Emits list of (row_index, everything_results) tuples
    check_done = Signal()        # All rows checked
    progress = Signal(int, int)  # (checked_count, total_count)

    def __init__(self, everything: EverythingSearch, results: List[Dict],
                 title_match_chars: int, everything_search_chars: int,
                 batch_size: int = 10, access_lock: Optional[threading.Lock] = None):
        super().__init__()
        self.everything = everything
        self.results = results
        self.title_match_chars = title_match_chars
        self.everything_search_chars = everything_search_chars
        self.batch_size = batch_size
        self._access_lock = access_lock  # Optional lock for thread-safe SDK access

    def run(self):
        """Check each result in Everything, emit in batches (thread-safe with lock)"""
        try:
            total = len(self.results)
            batch = []
            interrupted = False
            for row, result in enumerate(self.results):
                if self.isInterruptionRequested():
                    interrupted = True
                    logger.info("EverythingCheckWorker interruption requested, stopping early")
                    break

                title = result.get('title', 'Unknown')

                # Search with wildcard pattern (thread-safe if lock provided)
                # Quote the prefix to prevent Everything interpreting |, !, <, >, () as operators
                prefix = title[:self.everything_search_chars].replace('"', '')
                search_query = f'"{prefix}"*'

                if self._access_lock:
                    with self._access_lock:
                        everything_results = self.everything.search(search_query, everything_max_results=100)
                else:
                    everything_results = self.everything.search(search_query, everything_max_results=100)

                if self.isInterruptionRequested():
                    interrupted = True
                    logger.info("EverythingCheckWorker interrupted after search call")
                    break

                # Collect matches into batch
                if everything_results:
                    batch.append((row, everything_results))

                # Emit batch when full
                if len(batch) >= self.batch_size:
                    try:
                        self.batch_ready.emit(batch)
                        self.progress.emit(row + 1, total)
                    except Exception as e:
                        logger.error(f"Failed to emit batch signals: {e}")
                    batch = []

            if not interrupted:
                # Emit remaining items
                if batch:
                    try:
                        self.batch_ready.emit(batch)
                    except Exception as e:
                        logger.error(f"Failed to emit final batch signal: {e}")
                # Always emit a terminal progress state so UI can display completion accurately.
                try:
                    self.progress.emit(total, total)
                except Exception as e:
                    logger.error(f"Failed to emit final progress signal: {e}")
        except Exception as e:
            logger.error(f"Everything check error: {e}")
        finally:
            try:
                self.check_done.emit()
            except Exception as e:
                logger.error(f"Failed to emit check_done signal: {e}")
