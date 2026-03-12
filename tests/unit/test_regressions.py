import warnings

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QPushButton, QTableWidgetItem

from prowlarr_ui.api.prowlarr_client import ProwlarrClient
from prowlarr_ui.utils.config import validate_config
from prowlarr_ui.workers.search_worker import SearchWorker


def test_indexer_restore_empty_selection_keeps_root_consistent(window):
    window.preferences_store.set_value(window._pref_key("selected_indexers"), [])
    window.populate_indexers(
        [
            {"id": 1, "name": "Indexer One", "enable": True},
            {"id": 2, "name": "Indexer Two", "enable": True},
        ]
    )

    root = window.indexers_model.item(0)
    assert root is not None
    assert root.checkState() == Qt.Unchecked
    assert window.get_selected_indexers() == []


def test_indexer_restore_all_selection_returns_none(window):
    window.preferences_store.set_value(window._pref_key("selected_indexers"), [1, 2])
    window.populate_indexers(
        [
            {"id": 1, "name": "Indexer One", "enable": True},
            {"id": 2, "name": "Indexer Two", "enable": True},
        ]
    )

    root = window.indexers_model.item(0)
    assert root is not None
    assert root.checkState() == Qt.Checked
    assert window.get_selected_indexers() is None


def test_start_search_blocks_when_no_indexers_selected(window):
    root = window.indexers_model.item(0)
    assert root is not None
    root.setCheckState(Qt.Unchecked)

    window.query_input.setText("query")
    window.start_search()

    assert "No indexers selected" in window.status_label.text()
    assert window.current_worker is None


def test_persist_runtime_preferences_skips_unloaded_tree_overwrite(window):
    window.preferences_store.set_value(window._pref_key("selected_indexers"), [101, 202])
    window.preferences_store.set_value(window._pref_key("selected_categories"), [3030])

    window._indexers_loaded = False
    window._categories_loaded = False
    window._persist_runtime_preferences()

    assert (
        window.preferences_store.get_int_list(window._pref_key("selected_indexers"), None)
        == [101, 202]
    )
    assert (
        window.preferences_store.get_int_list(window._pref_key("selected_categories"), None)
        == [3030]
    )


def test_video_paths_are_keyed_by_release_identity_not_title(window):
    window.results_table.setRowCount(0)

    for row, guid in enumerate(("guid-a", "guid-b")):
        window.results_table.insertRow(row)
        title_item = QTableWidgetItem("Same.Title.2026")
        window.results_table.setItem(row, window.COL_TITLE, title_item)

        size_item = QTableWidgetItem("1.0 GB")
        size_item.setData(Qt.UserRole, 1024 * 1024 * 1024)
        window.results_table.setItem(row, window.COL_SIZE, size_item)

        btn = QPushButton("Download")
        btn.setProperty("guid", guid)
        btn.setProperty("indexerId", 1)
        btn.setProperty("title", "Same.Title.2026")
        window.results_table.setCellWidget(row, window.COL_DOWNLOAD, btn)

    worker = object()
    window.everything_check_worker = worker
    window._search_generation = 1
    window._everything_check_generation = 1
    window.on_everything_batch_ready(
        [(0, [("C:\\media\\a.mkv", 123)]), (1, [("C:\\media\\b.mkv", 456)])],
        worker,
    )

    assert window._get_video_path_for_row(0) == "C:\\media\\a.mkv"
    assert window._get_video_path_for_row(1) == "C:\\media\\b.mkv"


def test_populate_reconnects_signals_without_disconnect_warnings(window):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        window.populate_indexers(
            [
                {"id": 1, "name": "Indexer One", "enable": True},
                {"id": 2, "name": "Indexer Two", "enable": True},
            ]
        )
        window.populate_indexers(
            [
                {"id": 1, "name": "Indexer One", "enable": True},
                {"id": 2, "name": "Indexer Two", "enable": True},
            ]
        )
        window.populate_categories(
            [
                {"id": 2000, "name": "Movies"},
                {"id": 5000, "name": "TV"},
            ]
        )
        window.populate_categories(
            [
                {"id": 2000, "name": "Movies"},
                {"id": 5000, "name": "TV"},
            ]
        )

    assert not any("Failed to disconnect" in str(w.message) for w in caught)


def test_search_worker_preserves_explicit_empty_filter_lists():
    captured = {}

    class FakeClient:
        def search(self, query, indexer_ids=None, categories=None, offset=0, limit=1000, should_cancel=None):
            captured["query"] = query
            captured["indexer_ids"] = indexer_ids
            captured["categories"] = categories
            captured["offset"] = offset
            captured["limit"] = limit
            return []

    worker = SearchWorker(FakeClient(), "abc", [], [], 5, 10)
    worker.run()

    assert captured["query"] == "abc"
    assert captured["indexer_ids"] == []
    assert captured["categories"] == []
    assert captured["offset"] == 5
    assert captured["limit"] == 10


def test_prowlarr_client_search_includes_explicit_empty_filters():
    client = ProwlarrClient("http://localhost:9696", "api")
    captured = {}

    def fake_api_request(endpoint, params=None, method="GET", data=None, should_cancel=None):
        captured["endpoint"] = endpoint
        captured["params"] = dict(params or {})
        return []

    client._api_request = fake_api_request
    client.search("abc", indexer_ids=[], categories=[], offset=3, limit=50)

    assert captured["endpoint"] == "search"
    assert captured["params"]["indexerIds"] == []
    assert captured["params"]["categories"] == []
    assert captured["params"]["offset"] == 3
    assert captured["params"]["limit"] == 50


def test_validate_config_clamps_new_float_shutdown_and_watchdog_settings():
    config = {
        "prowlarr": {
            "host": "http://localhost:9696",
            "api_key": "x",
        },
        "settings": {
            "download_queue_stale_grace_seconds": "bad",
            "shutdown_force_after_seconds": 0,
            "shutdown_force_arm_seconds": 999,
            "everything_check_stale_grace_seconds": -1,
        },
    }

    warnings_out = validate_config(config)
    settings = config["settings"]

    assert settings["download_queue_stale_grace_seconds"] == 20.0
    assert settings["shutdown_force_after_seconds"] == 1.0
    assert settings["shutdown_force_arm_seconds"] == 60.0
    assert settings["everything_check_stale_grace_seconds"] == 0.1
    assert any("download_queue_stale_grace_seconds" in w for w in warnings_out)
    assert any("shutdown_force_after_seconds" in w for w in warnings_out)
    assert any("shutdown_force_arm_seconds" in w for w in warnings_out)
    assert any("everything_check_stale_grace_seconds" in w for w in warnings_out)
