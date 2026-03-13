"""Background worker for Everything search operations"""

import logging

from PySide6.QtCore import QThread, Signal

from prowlarr_ui.api.everything_search import EverythingSearch

logger = logging.getLogger(__name__)


class EverythingCheckWorker(QThread):
    """
    Background worker thread for checking Everything existence
    Emits results in batches to avoid flooding the main thread
    Thread-safe: EverythingSearch handles SDK serialization internally
    """

    # Qt signals for thread communication
    batch_ready = Signal(list)  # Emits list of (row_index, everything_results) tuples
    check_done = Signal()  # All rows checked
    progress = Signal(int, int)  # (checked_count, total_count)

    def __init__(
        self,
        everything: EverythingSearch,
        results: list[dict[str, object]],
        title_match_chars: int,
        everything_search_chars: int,
        batch_size: int = 10,
    ):
        super().__init__()
        self.everything = everything
        # Snapshot result ordering so UI-side mutations cannot race iteration.
        self.results = list(results)
        self.title_match_chars = title_match_chars
        self.everything_search_chars = everything_search_chars
        self.batch_size = batch_size

    def run(self) -> None:
        """Check each result in Everything, emit in batches."""
        try:
            total = len(self.results)
            batch: list[tuple[int, list[tuple[str, int]]]] = []
            interrupted = False
            for row, result in enumerate(self.results):
                if self.isInterruptionRequested():
                    interrupted = True
                    logger.info(
                        "EverythingCheckWorker interruption requested, stopping early"
                    )
                    break

                title = str(result.get("title", "Unknown") or "Unknown")

                # Search with wildcard pattern
                # Quote the prefix so Everything does not treat punctuation
                # as search operators.
                prefix = title[: self.everything_search_chars].replace('"', "")
                search_query = f'"{prefix}"*'
                everything_results = self.everything.search(
                    search_query, everything_max_results=100
                )

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
                # Emit a terminal progress state so the UI can show completion.
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
