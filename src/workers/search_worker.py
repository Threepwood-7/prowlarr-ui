"""Background worker for Prowlarr API search operations"""
import time
import logging
from typing import List, Dict, Optional
from PySide6.QtCore import QThread, Signal

from src.api.prowlarr_client import ProwlarrClient

logger = logging.getLogger(__name__)


class SearchWorker(QThread):
    """
    Background worker thread for API calls
    Prevents UI freezing during network operations
    """

    # Qt signals for thread communication
    search_done = Signal(object, float)  # Emits (search results, elapsed_seconds)
    error = Signal(str)        # Emits error message
    progress = Signal(str)     # Emits progress updates

    def __init__(self, client: ProwlarrClient, query: str, indexer_ids: Optional[List[int]],
                 categories: Optional[List[int]], offset: int = 0, limit: int = 1000):
        super().__init__()
        self.client = client
        self.query = query
        self.indexer_ids = indexer_ids
        self.categories = categories
        self.offset = offset
        self.limit = limit

    def run(self):
        """Execute search in background thread"""
        try:
            # Allow shutdown path to cancel before any network work begins.
            if self.isInterruptionRequested():
                logger.info("SearchWorker interrupted before start")
                return

            start_time = time.time()
            try:
                self.progress.emit(f"Searching for '{self.query}'...")
            except Exception as e:
                logger.error(f"Failed to emit progress signal: {e}")

            # Execute the search
            results = self.client.search(
                self.query,
                self.indexer_ids,
                self.categories,
                self.offset,
                self.limit,
                should_cancel=self.isInterruptionRequested,
            )

            if results is None:
                results = []

            # Avoid emitting stale completion updates if shutdown requested.
            if self.isInterruptionRequested():
                logger.info("SearchWorker interrupted before completion emit")
                return

            elapsed = time.time() - start_time
            logger.info(f"Search completed in {elapsed:.2f}s, found {len(results)} results")

            try:
                self.search_done.emit(results, elapsed)
            except Exception as e:
                logger.error(f"Failed to emit search_done signal: {e}")
        except Exception as e:
            if self.isInterruptionRequested():
                logger.info("SearchWorker interrupted during execution")
                return
            error_msg = str(e)
            logger.error(f"Search error: {error_msg}")
            try:
                self.error.emit(error_msg)
            except Exception as emit_error:
                logger.error(f"Failed to emit error signal: {emit_error}")
