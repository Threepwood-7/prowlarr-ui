from PySide6.QtCore import QThread, Signal
from PySide6.QtWidgets import QPushButton, QTableWidgetItem


def test_main_window_initializes_with_mocked_services(window):
    assert window.status_label.text().startswith("Ready")
    root = window.indexers_model.item(0)
    assert root is not None
    assert root.rowCount() == 2


def test_start_search_blocked_while_download_queue_running(window):
    class RunningWorker:
        def isRunning(self):
            return True

        def wait(self, timeout_ms=0):
            return True

    window.download_worker = RunningWorker()
    window.query_input.setText("some query")
    window.start_search()

    assert "Cannot start a new search while downloads are running" in window.status_label.text()
    assert window.current_worker is None
    window.download_worker = None


def test_start_search_blocked_while_download_worker_reference_exists(window):
    class FinishingWorker:
        def isRunning(self):
            return False

        def wait(self, timeout_ms=0):
            return True

    window.download_worker = FinishingWorker()
    window.query_input.setText("some query")
    window.start_search()

    assert "Cannot start a new search while downloads are running" in window.status_label.text()
    assert window.current_worker is None
    window.download_worker = None


def test_collect_row_download_item_accepts_zero_indexer_id(window):
    row = window.results_table.rowCount()
    window.results_table.insertRow(row)
    window.results_table.setItem(row, window.COL_TITLE, QTableWidgetItem("Release Zero"))

    btn = QPushButton("Download")
    btn.setProperty("guid", "guid-zero")
    btn.setProperty("indexerId", 0)
    btn.setProperty("title", "Release Zero")
    window.results_table.setCellWidget(row, window.COL_DOWNLOAD, btn)

    item = window._collect_row_download_item(row)
    assert item == {"guid": "guid-zero", "indexer_id": 0, "title": "Release Zero"}


def test_start_download_queue_dedupes_initial_items_and_progress(window, mocked_main, monkeypatch):
    captured = {}

    class FakeDownloadWorker(QThread):
        progress = Signal(int, int, str)
        item_downloaded = Signal(str, int, bool)
        queue_done = Signal()

        def __init__(self, client, items):
            super().__init__()
            captured["items"] = list(items)

        def start(self):
            # Keep the test deterministic; we only validate enqueue payload/progress sizing here.
            return

    monkeypatch.setattr(mocked_main, "DownloadWorker", FakeDownloadWorker)

    items = [
        {"guid": "g1", "indexer_id": 1, "title": "One"},
        {"guid": "g1", "indexer_id": 1, "title": "One Duplicate"},
        {"guid": "g2", "indexer_id": 2, "title": "Two"},
    ]
    window.start_download_queue(items)

    assert len(captured["items"]) == 2
    assert window.download_progress.maximum() == 2


def test_start_everything_check_only_defers_for_older_generation(window):
    class RunningEverythingWorker:
        def isRunning(self):
            return True

        def wait(self, timeout_ms=0):
            return True

    window.current_results = [{"title": "Some.Result.2026"}]
    window.everything = object()
    window.everything_check_worker = RunningEverythingWorker()
    window._search_generation = 7

    # Same-generation running worker should not queue redundant deferred work.
    window._everything_check_generation = 7
    window._pending_everything_check_generation = None
    window.start_everything_check()
    assert window._pending_everything_check_generation is None

    # Older-generation worker should queue a deferred check for current generation.
    window._everything_check_generation = 6
    window.start_everything_check()
    assert window._pending_everything_check_generation == 7
    window.everything_check_worker = None


def test_download_buttons_only_enabled_for_actionable_rows(window):
    row = window.results_table.rowCount()
    window.results_table.insertRow(row)
    window.results_table.setItem(row, window.COL_TITLE, QTableWidgetItem("Already Downloaded"))

    btn = QPushButton("Download")
    btn.setProperty("guid", "guid-downloaded")
    btn.setProperty("indexerId", 10)
    btn.setProperty("title", "Already Downloaded")
    window.results_table.setCellWidget(row, window.COL_DOWNLOAD, btn)

    window._downloaded_release_keys.add(("guid-downloaded", 10))
    window.update_download_button_states()

    assert not window.download_all_btn.isEnabled()
    assert not window.download_selected_btn.isEnabled()
