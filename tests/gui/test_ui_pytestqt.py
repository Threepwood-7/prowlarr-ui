import os

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

    assert (
        "Cannot start a new search while downloads are running"
        in window.status_label.text()
    )
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

    assert (
        "Cannot start a new search while downloads are running"
        in window.status_label.text()
    )
    assert window.current_worker is None
    window.download_worker = None


def test_collect_row_download_item_accepts_zero_indexer_id(window):
    row = window.results_table.rowCount()
    window.results_table.insertRow(row)
    window.results_table.setItem(
        row, window.COL_TITLE, QTableWidgetItem("Release Zero")
    )

    btn = QPushButton("Download")
    btn.setProperty("guid", "guid-zero")
    btn.setProperty("indexerId", 0)
    btn.setProperty("title", "Release Zero")
    window.results_table.setCellWidget(row, window.COL_DOWNLOAD, btn)

    item = window._collect_row_download_item(row)
    assert item == {"guid": "guid-zero", "indexer_id": 0, "title": "Release Zero"}


def test_start_download_queue_dedupes_initial_items_and_progress(
    window, mocked_main, monkeypatch
):
    captured = {}

    class FakeDownloadWorker(QThread):
        progress = Signal(int, int, str)
        item_downloaded = Signal(str, int, bool)
        queue_done = Signal()

        def __init__(self, client, items):
            super().__init__()
            captured["items"] = list(items)

        def start(self):
            # Keep the test deterministic; only validate queue payload sizing.
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
    window.results_table.setItem(
        row, window.COL_TITLE, QTableWidgetItem("Already Downloaded")
    )

    btn = QPushButton("Download")
    btn.setProperty("guid", "guid-downloaded")
    btn.setProperty("indexerId", 10)
    btn.setProperty("title", "Already Downloaded")
    window.results_table.setCellWidget(row, window.COL_DOWNLOAD, btn)

    window._downloaded_release_keys.add(("guid-downloaded", 10))
    window.update_download_button_states()

    assert not window.download_all_btn.isEnabled()
    assert not window.download_selected_btn.isEnabled()


def test_view_menu_includes_fit_columns_action(window):
    view_action = next(
        (a for a in window.menuBar().actions() if a.text() == "&View"), None
    )
    assert view_action is not None
    view_menu = view_action.menu()
    assert view_menu is not None
    assert any(action.text() == "&Fit Columns" for action in view_menu.actions())


def test_tools_menu_includes_edit_ini_action(window):
    tools_action = next(
        (a for a in window.menuBar().actions() if a.text() == "&Tools"), None
    )
    assert tools_action is not None
    tools_menu = tools_action.menu()
    assert tools_menu is not None
    assert any(action.text() == "Edit &.ini File" for action in tools_menu.actions())


def test_file_menu_exit_shortcuts_are_ctrl_q_and_alt_x(window):
    file_action = next(
        (a for a in window.menuBar().actions() if a.text() == "&File"), None
    )
    assert file_action is not None
    file_menu = file_action.menu()
    assert file_menu is not None

    exit_action = next((a for a in file_menu.actions() if a.text() == "E&xit"), None)
    assert exit_action is not None
    shortcuts = {shortcut.toString() for shortcut in exit_action.shortcuts()}
    assert {"Ctrl+Q", "Alt+X"} <= shortcuts


def test_dynamic_bookmark_label_escapes_ampersand(window):
    query = "Rock & Roll"
    window.query_input.setText(query)
    window._add_bookmark()

    dynamic_action = next(
        (a for a in window.bookmarks_menu.actions() if a.data() == query), None
    )
    assert dynamic_action is not None
    assert dynamic_action.text() == "Rock && Roll"


def test_fit_columns_resizes_visible_columns_and_persists_widths(window):
    row = window.results_table.rowCount()
    window.results_table.insertRow(row)
    window.results_table.setItem(
        row, window.COL_TITLE, QTableWidgetItem("Some.Release.2026")
    )
    window.results_table.setItem(
        row,
        window.COL_INDEXER,
        QTableWidgetItem("VeryLongIndexerNameForFitColumnsRegressionTest"),
    )

    window.results_table.setColumnWidth(window.COL_INDEXER, 40)
    before = window.results_table.columnWidth(window.COL_INDEXER)

    window._fit_columns()

    after = window.results_table.columnWidth(window.COL_INDEXER)
    assert after >= before
    assert "fitted visible columns" in window.status_label.text().lower()
    saved = (
        window.preferences_store.get_int_list(window._pref_key("column_widths"), [])
        or []
    )
    assert len(saved) == window.COL_COUNT - 1


def test_edit_ini_file_opens_preferences_path(window, monkeypatch):
    opened = {}
    monkeypatch.setattr(
        "prowlarr_ui.app.open_path_in_default_app",
        lambda path: opened.__setitem__("path", path) or True,
    )

    ini_path = window.preferences_store.file_name()

    window._edit_preferences_ini_file()

    assert opened["path"] == ini_path
    assert os.path.exists(ini_path)
