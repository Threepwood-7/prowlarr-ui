from __future__ import annotations

import logging
from typing import Any

from PySide6.QtCore import QThread, Signal

from prowlarr_ui.api.everything_search import EverythingSearch

logger = logging.getLogger(__name__)


class InitWorker(QThread):
    """Background worker to initialize Everything and load Prowlarr indexers."""

    init_done = Signal(object, list, str)  # (everything_instance, indexers, error)

    def __init__(
        self, everything_method: str, prowlarr: Any, everything_sdk_url: str = ""
    ) -> None:
        super().__init__()
        self.everything_method = everything_method
        self.prowlarr = prowlarr
        self.everything_sdk_url = everything_sdk_url

    def run(self) -> None:
        everything = None
        indexers: list[dict[str, Any]] = []
        error = ""
        if self.isInterruptionRequested():
            logger.info("InitWorker interrupted before initialization")
            return
        try:
            kwargs: dict[str, str] = {}
            if self.everything_sdk_url:
                kwargs["sdk_url"] = self.everything_sdk_url
            everything = EverythingSearch(self.everything_method, **kwargs)
        except Exception as exc:
            logger.error(f"Failed to initialize Everything: {exc}")
        if self.isInterruptionRequested():
            logger.info("InitWorker interrupted before indexer load")
            return
        try:
            if self.prowlarr:
                indexers = self.prowlarr.get_indexers(
                    should_cancel=self.isInterruptionRequested
                )
        except Exception as exc:
            error = f"Failed to load indexers: {exc}"
            logger.error(error)
        if self.isInterruptionRequested():
            logger.info("InitWorker interrupted before completion emit")
            return
        self.init_done.emit(everything, indexers, error)
