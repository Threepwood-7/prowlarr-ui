import os
import sys
from pathlib import Path
from typing import Any

import pytest
from PySide6.QtCore import QObject, Signal

# Ensure src layout package is importable when tests are run from nested paths.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


# Force headless backend for CI/local runs without a display server.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


class FakeEverythingSearch:
    """Offline Everything stub used by UI tests."""

    def __init__(self, integration_method: str = "sdk", sdk_url: str = ""):
        self.integration_method = integration_method
        self.sdk_url = sdk_url
        self.sdk_available = True
        self.http_available = False
        self.last_launch_query = None

    def search(self, query: str, everything_max_results: int = 10):
        return []

    def launch_search(self, query: str):
        self.last_launch_query = query


class FakeProwlarrClient:
    """Offline Prowlarr stub used by InitWorker and download/search paths."""

    def __init__(self, *args, **kwargs):
        self.download_calls: list[tuple[str, int]] = []

    def get_indexers(self, should_cancel=None) -> list[dict[str, Any]]:
        return [
            {"id": 1, "name": "Indexer One", "enable": True},
            {"id": 2, "name": "Indexer Two", "enable": True},
        ]

    def get_categories(self) -> list[dict[str, Any]]:
        return [
            {"id": 2000, "name": "Movies"},
            {"id": 5000, "name": "TV"},
        ]

    def search(
        self, query: str, indexer_ids=None, categories=None, offset: int = 0, limit: int = 100, should_cancel=None
    ):
        return []

    def download(self, guid: str, indexer_id: int, should_cancel=None) -> bool:
        self.download_calls.append((guid, indexer_id))
        return True


class FakeInitWorker(QObject):
    """
    Synchronous InitWorker stub.
    Emits init_done immediately to avoid thread timing/network dependencies in tests.
    """

    init_done = Signal(object, list, str)

    def __init__(self, everything_method, prowlarr, everything_sdk_url=""):
        super().__init__()
        self._everything_method = everything_method
        self._prowlarr = prowlarr
        self._everything_sdk_url = everything_sdk_url

    def start(self):
        everything = FakeEverythingSearch(self._everything_method, self._everything_sdk_url)
        indexers = self._prowlarr.get_indexers() if self._prowlarr else []
        self.init_done.emit(everything, indexers, "")

    def isRunning(self):
        return False

    def wait(self, timeout_ms=0):
        return True

    def terminate(self):
        return None


def _fake_config() -> dict[str, Any]:
    return {
        "prowlarr": {
            "host": "http://fake-prowlarr:9696",
            "api_key": "fake-api-key",
            "http_basic_auth_username": "",
            "http_basic_auth_password": "",
        },
        "settings": {
            "title_match_chars": 42,
            "everything_search_chars": 42,
            "everything_recheck_delay": 50,
            "web_search_url": "https://example.com/search?q={query}",
            "everything_integration_method": "sdk",
            "prowlarr_page_size": 100,
            "everything_max_results": 5,
            "everything_batch_size": 10,
            "api_timeout": 5,
            "api_retries": 0,
        },
        "preferences": {
            "search_history": [],
            "bookmarks": [],
            "selected_indexers": [1, 2],
            "selected_categories": [2000, 5000],
            "hide_existing": False,
            "splitter_sizes": [300, 1100],
            "hidden_columns": [],
            "column_widths": [],
        },
    }


@pytest.fixture
def mocked_main(monkeypatch, tmp_path):
    import prowlarr_ui.app as main

    monkeypatch.setattr(main, "ProwlarrClient", FakeProwlarrClient)
    monkeypatch.setattr(main, "EverythingSearch", FakeEverythingSearch)
    monkeypatch.setattr(main, "InitWorker", FakeInitWorker)
    monkeypatch.setattr(main, "load_config", lambda: _fake_config())
    monkeypatch.setattr(main, "save_config", lambda _cfg: None)
    monkeypatch.setattr(main, "validate_config", lambda _cfg: [])
    monkeypatch.setenv(main.PREFERENCES_INI_OVERRIDE_ENV, str(tmp_path / "test_prefs.ini"))
    return main


@pytest.fixture
def window(qtbot, mocked_main):
    win = mocked_main.MainWindow()
    qtbot.addWidget(win)
    qtbot.waitUntil(lambda: "Loading..." not in win.status_label.text(), timeout=2000)
    yield win
