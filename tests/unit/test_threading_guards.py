import inspect
import time

from PySide6.QtCore import Qt
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import QPushButton, QTableWidgetItem

from prowlarr_ui.api.prowlarr_client import ProwlarrClient
from prowlarr_ui.workers.download_worker import DownloadWorker
from prowlarr_ui.workers.everything_worker import EverythingCheckWorker
from prowlarr_ui.workers.search_worker import SearchWorker


class _SignalStub:
    def __init__(self):
        self._callbacks = []

    def connect(self, callback):
        self._callbacks.append(callback)

    def emit(self, *args, **kwargs):
        for callback in list(self._callbacks):
            callback(*args, **kwargs)


class _QueuedEverythingWorkerStub:
    def __init__(self, *_args, **_kwargs):
        self.batch_ready = _SignalStub()
        self.check_done = _SignalStub()
        self.progress = _SignalStub()
        self.started = False
        self._running = False

    def start(self):
        self.started = True
        self._running = True

    def isRunning(self):
        return self._running

    def wait(self, _timeout_ms=0):
        self._running = False
        return True

    def requestInterruption(self):
        self._running = False


def _seed_one_row(window, title="Some.Release.2026"):
    window.results_table.setRowCount(0)
    row = window.results_table.rowCount()
    window.results_table.insertRow(row)
    window.results_table.setItem(row, window.COL_TITLE, QTableWidgetItem(title))
    size_item = QTableWidgetItem("1.0 GB")
    # Store numeric size in UserRole to match production expectations.
    size_item.setData(Qt.UserRole, 1024 * 1024 * 1024)
    window.results_table.setItem(row, window.COL_SIZE, size_item)


def _is_spinner_busy(window):
    return window.activity_bar.minimum() == 0 and window.activity_bar.maximum() == 0


def test_spinner_tag_reference_count(window):
    window.start_spinner("search")
    assert _is_spinner_busy(window)

    # Re-adding the same tag now increments a reference count.
    window.start_spinner("search")
    window.stop_spinner("search")
    assert _is_spinner_busy(window)
    window.stop_spinner("search")
    assert not _is_spinner_busy(window)

    window.start_spinner("search")
    window.start_spinner("everything")
    window.stop_spinner("search")
    assert _is_spinner_busy(window)
    window.stop_spinner("everything")
    assert not _is_spinner_busy(window)


def test_table_sort_lock_requires_all_owners_release(window):
    assert window.results_table.isSortingEnabled() is True

    window._acquire_table_sort_lock("download")
    assert window.results_table.isSortingEnabled() is False

    window._acquire_table_sort_lock("everything")
    window._release_table_sort_lock("download")
    assert window.results_table.isSortingEnabled() is False

    window._release_table_sort_lock("everything")
    assert window.results_table.isSortingEnabled() is True


def test_everything_batch_ignores_stale_worker(window):
    _seed_one_row(window)
    title_item = window.results_table.item(0, window.COL_TITLE)
    assert title_item.toolTip() == ""

    active_worker = object()
    stale_worker = object()
    window.everything_check_worker = active_worker
    window._search_generation = 9
    window._everything_check_generation = 9

    window.on_everything_batch_ready(
        [(0, [("C:\\media\\file.mkv", 123)])], stale_worker
    )
    assert title_item.toolTip() == ""

    window.on_everything_batch_ready(
        [(0, [("C:\\media\\file.mkv", 123)])], active_worker
    )
    assert title_item.toolTip().startswith("Found in Everything")


def test_everything_progress_and_done_ignore_stale_worker(window):
    active_worker = object()
    stale_worker = object()
    window.everything_check_worker = active_worker
    window._search_generation = 4
    window._everything_check_generation = 4

    window.start_spinner("everything")
    baseline = "baseline"
    window.status_label.setText(baseline)

    window._on_everything_progress(1, 5, stale_worker)
    assert window.status_label.text() == baseline

    window.on_everything_check_finished(stale_worker)
    assert window.everything_check_worker is active_worker
    assert _is_spinner_busy(window)

    window._on_everything_progress(2, 5, active_worker)
    assert "Checking Everything: 2/5" in window.status_label.text()

    window.on_everything_check_finished(active_worker)
    assert window.everything_check_worker is None
    assert not _is_spinner_busy(window)


def test_recheck_is_deferred_and_replayed(window, mocked_main, monkeypatch):
    _seed_one_row(window, "Replay.Me.2026")
    window.everything = object()
    window._search_generation = 11

    class RunningWorker:
        def isRunning(self):
            return True

    running_worker = RunningWorker()
    window.everything_check_worker = running_worker

    title_key = "Replay.Me.2026"[: window.title_match_chars].lower()
    window._recheck_everything_for_titles({title_key}, expected_generation=11)
    assert window._pending_everything_recheck is not None
    assert window._pending_everything_recheck["generation"] == 11

    created = {}

    class FakeWorker(_QueuedEverythingWorkerStub):
        def __init__(
            self,
            everything,
            results,
            title_match_chars,
            everything_search_chars,
            batch_size=10,
        ):
            super().__init__()
            created["results"] = list(results)
            created["batch_size"] = batch_size

    monkeypatch.setattr(mocked_main, "EverythingCheckWorker", FakeWorker)
    window.on_everything_check_finished(running_worker)

    assert window._pending_everything_recheck is None
    assert isinstance(window.everything_check_worker, FakeWorker)
    assert window.everything_check_worker.started
    assert len(created["results"]) == 1


def test_recheck_deferral_merges_title_keys(window):
    class RunningWorker:
        def isRunning(self):
            return True

    window.everything_check_worker = RunningWorker()
    window._search_generation = 15

    window._recheck_everything_for_titles({"alpha"}, expected_generation=15)
    window._recheck_everything_for_titles({"beta"}, expected_generation=15)

    assert window._pending_everything_recheck is not None
    assert window._pending_everything_recheck["title_keys"] == {"alpha", "beta"}


def test_recheck_and_deferred_recheck_are_noops_during_shutdown(window, monkeypatch):
    _seed_one_row(window, "Shutdown.Check.2026")
    window._search_generation = 21
    window._shutdown_in_progress = True

    title_key = "Shutdown.Check.2026"[: window.title_match_chars].lower()

    # Direct call is guarded and should no-op while shutdown is active.
    window._recheck_everything_for_titles({title_key}, expected_generation=21)

    window._pending_everything_recheck = {"title_keys": {title_key}, "generation": 21}
    monkeypatch.setattr(
        window,
        "_recheck_everything_for_titles",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("recheck should not run during shutdown")
        ),
    )

    window._run_deferred_everything_recheck()
    assert window._pending_everything_recheck is None


def test_everything_check_ownership_blocks_replacement_even_if_worker_not_running(
    window, mocked_main, monkeypatch
):
    class OwnedWorker:
        def isRunning(self):
            return False

    owned = OwnedWorker()
    window.everything = object()
    window.current_results = [{"title": "Owned.Result.2026"}]
    window._search_generation = 8
    window._everything_check_generation = 8
    window.everything_check_worker = owned

    monkeypatch.setattr(
        mocked_main,
        "EverythingCheckWorker",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("replacement worker should not be created")
        ),
    )

    window.start_everything_check()
    assert window.everything_check_worker is owned
    assert window._pending_everything_check_generation is None


def test_start_everything_check_handles_worker_constructor_failure(
    window, mocked_main, monkeypatch
):
    window.everything = object()
    window.current_results = [{"title": "Ctor.Fail.2026"}]

    def fail_worker(*_args, **_kwargs):
        raise RuntimeError("constructor boom")

    monkeypatch.setattr(mocked_main, "EverythingCheckWorker", fail_worker)

    window.start_everything_check()

    assert window.everything_check_worker is None
    assert "failed to start everything check" in window.status_label.text().lower()


def test_recheck_defers_when_worker_reference_exists_even_if_not_running(window):
    _seed_one_row(window, "Deferred.Reference.2026")

    class OwnedWorker:
        def isRunning(self):
            return False

    window.everything_check_worker = OwnedWorker()
    window._search_generation = 14
    title_key = "Deferred.Reference.2026"[: window.title_match_chars].lower()

    window._recheck_everything_for_titles({title_key}, expected_generation=14)

    assert window._pending_everything_recheck is not None
    assert window._pending_everything_recheck["title_keys"] == {title_key}


def test_everything_check_active_recovers_deleted_wrapper(window):
    class DeletedWorker:
        def isRunning(self):
            raise RuntimeError(
                "wrapped C/C++ object of type EverythingCheckWorker has been deleted"
            )

    window.everything_check_worker = DeletedWorker()
    window._acquire_table_sort_lock("everything")
    window.start_spinner("everything")

    active = window._is_everything_check_active()

    assert active is False
    assert window.everything_check_worker is None
    assert window.results_table.isSortingEnabled() is True
    assert not _is_spinner_busy(window)


def test_download_queue_active_recovers_deleted_wrapper(window):
    class DeletedWorker:
        def isRunning(self):
            raise RuntimeError(
                "wrapped C/C++ object of type DownloadWorker has been deleted"
            )

    window.download_worker = DeletedWorker()
    window._acquire_table_sort_lock("download")
    window.search_btn.setEnabled(False)

    active = window._is_download_queue_active()

    assert active is False
    assert window.download_worker is None
    assert window.search_btn.isEnabled() is True
    assert window.results_table.isSortingEnabled() is True


def test_wait_worker_uses_cooperative_stop_only(window):
    class SlowWorker:
        def __init__(self):
            self.interrupt_calls = 0
            self.terminate_calls = 0

        def isRunning(self):
            return True

        def requestInterruption(self):
            self.interrupt_calls += 1

        def wait(self, _timeout_ms=0):
            return False

        def terminate(self):
            self.terminate_calls += 1

    worker = SlowWorker()
    result = window._wait_worker(worker, "SlowWorker", timeout_ms=1)
    assert result is False
    assert worker.interrupt_calls == 1
    assert worker.terminate_calls == 0


def test_load_all_cancel_interrupts_running_worker_and_restores_ui(window):
    class RunningWorker:
        def __init__(self):
            self.interrupted = False

        def isRunning(self):
            return True

        def requestInterruption(self):
            self.interrupted = True

    worker = RunningWorker()
    window._load_all_active = True
    window.current_worker = worker
    window.search_btn.setEnabled(False)
    window.load_all_btn.setText("Cancel")
    window.start_spinner("search")

    window.start_load_all_pages()

    assert worker.interrupted is True
    assert window.current_worker is None
    assert window.search_btn.isEnabled()
    assert window.load_all_btn.text() == "Load A&ll"
    assert not _is_spinner_busy(window)
    assert "cancelled" in window.status_label.text().lower()


def test_download_handlers_ignore_stale_worker_signals(window, monkeypatch):
    row = window.results_table.rowCount()
    window.results_table.insertRow(row)
    window.results_table.setItem(
        row, window.COL_TITLE, QTableWidgetItem("Downloaded.Row")
    )
    window.results_table.setItem(
        row, window.COL_INDEXER, QTableWidgetItem("Indexer One")
    )

    btn = QPushButton("Download")
    btn.setProperty("guid", "guid-1")
    btn.setProperty("indexerId", 42)
    btn.setProperty("title", "Downloaded.Row")
    window.results_table.setCellWidget(row, window.COL_DOWNLOAD, btn)

    monkeypatch.setattr(
        window, "_write_download_history", lambda *_args, **_kwargs: None
    )
    window._release_key_to_row = {("guid-1", 42): row}
    window.download_progress.setValue(0)

    active_worker = object()
    stale_worker = object()
    window.download_worker = active_worker

    window.on_download_progress(1, 1, "Downloaded.Row", stale_worker)
    assert window.download_progress.value() == 0

    window.on_item_downloaded("guid-1", 42, True, stale_worker)
    assert ("guid-1", 42) not in window._downloaded_release_keys

    window.on_download_queue_finished(stale_worker)
    assert window.download_worker is active_worker

    window.on_item_downloaded("guid-1", 42, True, active_worker)
    assert ("guid-1", 42) in window._downloaded_release_keys

    window.on_download_queue_finished(active_worker)
    assert window.download_worker is None


def test_download_queue_finished_skips_everything_recheck_during_shutdown(
    window, monkeypatch
):
    window.everything = object()
    window._downloaded_title_keys = {"title-key"}
    window._shutdown_in_progress = True

    scheduled = {"count": 0}
    monkeypatch.setattr(
        window,
        "_schedule_timer",
        lambda *_args, **_kwargs: scheduled.__setitem__(
            "count", scheduled["count"] + 1
        ),
    )

    window.on_download_queue_finished()

    assert scheduled["count"] == 0
    assert window._downloaded_title_keys == set()


def test_download_finish_keeps_sorting_disabled_when_everything_lock_active(window):
    worker = object()
    window.download_worker = worker
    window._download_queue_owner_since = time.monotonic()
    window._acquire_table_sort_lock("download")
    window._acquire_table_sort_lock("everything")

    window.on_download_queue_finished(worker)

    assert window.download_worker is None
    assert window.results_table.isSortingEnabled() is False


def test_everything_finish_keeps_sorting_disabled_while_download_queue_is_active(
    window,
):
    active_worker = object()
    window.everything_check_worker = active_worker
    window._search_generation = 5
    window._everything_check_generation = 5
    window._acquire_table_sort_lock("download")
    window._acquire_table_sort_lock("everything")
    window.download_worker = (
        object()
    )  # Queue ownership remains active until queue_done clears it.
    window.start_spinner("everything")

    window.on_everything_check_finished(active_worker)

    assert window.everything_check_worker is None
    assert window.results_table.isSortingEnabled() is False


def test_apply_default_sort_is_blocked_while_download_queue_active(window):
    class RunningWorker:
        def isRunning(self):
            return True

    window.download_worker = RunningWorker()
    window.current_results = [
        {"title": "b-title", "indexer": "z", "age": 10},
        {"title": "a-title", "indexer": "a", "age": 1},
    ]
    baseline = list(window.current_results)

    window.apply_default_sort()

    assert (
        "cannot reset sorting while downloads are running"
        in window.status_label.text().lower()
    )
    assert window.current_results == baseline


def test_close_event_ignores_and_retries_when_workers_refuse_to_stop(
    window, monkeypatch
):
    class StubbornWorker:
        def __init__(self):
            self.interrupt_calls = 0

        def isRunning(self):
            return True

        def requestInterruption(self):
            self.interrupt_calls += 1

        def wait(self, _timeout_ms=0):
            return False

    stubborn = StubbornWorker()
    window.init_worker = stubborn
    window.current_worker = stubborn
    window.everything_check_worker = stubborn
    window.download_worker = stubborn
    window._all_workers = [stubborn]

    scheduled = {"count": 0}
    monkeypatch.setattr(
        window,
        "_schedule_timer",
        lambda _delay, _cb: scheduled.__setitem__("count", scheduled["count"] + 1),
    )

    event = QCloseEvent()
    window.closeEvent(event)

    assert not event.isAccepted()
    assert scheduled["count"] == 1
    assert window._close_retry_pending is True
    assert _is_spinner_busy(window)
    assert stubborn.interrupt_calls >= 1


def test_close_event_prompts_then_forces_exit_after_shutdown_deadline(
    window, monkeypatch
):
    class StubbornWorker:
        def __init__(self):
            self.running = True
            self.terminate_calls = 0

        def isRunning(self):
            return self.running

        def requestInterruption(self):
            return None

        def wait(self, _timeout_ms=0):
            return False

        def terminate(self):
            self.terminate_calls += 1
            self.running = False

    now = {"value": 0.0}
    monkeypatch.setattr("prowlarr_ui.app.time.monotonic", lambda: now["value"])
    scheduled = {"count": 0}
    monkeypatch.setattr(
        window,
        "_schedule_timer",
        lambda *_args, **_kwargs: scheduled.__setitem__(
            "count", scheduled["count"] + 1
        ),
    )

    stubborn = StubbornWorker()
    window._shutdown_force_after_seconds = 1.0
    window.init_worker = stubborn
    window.current_worker = stubborn
    window.everything_check_worker = stubborn
    window.download_worker = stubborn
    window._all_workers = [stubborn]

    first = QCloseEvent()
    window.closeEvent(first)
    assert not first.isAccepted()
    assert scheduled["count"] == 1

    now["value"] = 2.0
    second = QCloseEvent()
    window.closeEvent(second)
    assert not second.isAccepted()
    assert window._shutdown_force_prompted is True
    # Prompt path should not auto-schedule another retry.
    assert scheduled["count"] == 1

    third = QCloseEvent()
    window.closeEvent(third)
    assert third.isAccepted()
    assert stubborn.terminate_calls >= 1


def test_close_event_prompt_disarms_shutdown_mode_and_cancels_retry_timer(
    window, monkeypatch
):
    class StubbornWorker:
        def isRunning(self):
            return True

        def requestInterruption(self):
            return None

        def wait(self, _timeout_ms=0):
            return False

    class TimerStub:
        def __init__(self):
            self.stop_calls = 0

        def isActive(self):
            return True

        def stop(self):
            self.stop_calls += 1

    now = {"value": 0.0}
    monkeypatch.setattr("prowlarr_ui.app.time.monotonic", lambda: now["value"])
    retry_timer = TimerStub()
    monkeypatch.setattr(
        window, "_schedule_timer", lambda *_args, **_kwargs: retry_timer
    )

    stubborn = StubbornWorker()
    window._shutdown_force_after_seconds = 1.0
    window.init_worker = stubborn
    window.current_worker = stubborn
    window.everything_check_worker = stubborn
    window.download_worker = stubborn
    window._all_workers = [stubborn]

    first = QCloseEvent()
    window.closeEvent(first)
    assert not first.isAccepted()
    assert window._shutdown_in_progress is True
    assert window._close_retry_pending is True

    now["value"] = 2.0
    second = QCloseEvent()
    window.closeEvent(second)
    assert not second.isAccepted()
    assert window._shutdown_in_progress is False
    assert window._close_retry_pending is False
    assert retry_timer.stop_calls == 1


def test_close_event_force_path_aborts_when_workers_survive_terminate(window):
    class UnstoppableWorker:
        def __init__(self):
            self.terminate_calls = 0

        def isRunning(self):
            return True

        def requestInterruption(self):
            return None

        def wait(self, _timeout_ms=0):
            return False

        def terminate(self):
            self.terminate_calls += 1

    stubborn = UnstoppableWorker()
    window.init_worker = stubborn
    window.current_worker = stubborn
    window.everything_check_worker = stubborn
    window.download_worker = stubborn
    window._all_workers = [stubborn]
    window._shutdown_force_armed_until = time.monotonic() + 5.0
    window._shutdown_force_prompted = True

    event = QCloseEvent()
    window.closeEvent(event)

    assert not event.isAccepted()
    assert stubborn.terminate_calls >= 1
    assert window._shutdown_in_progress is False
    assert window._shutdown_force_armed_until is None
    assert "close aborted" in window.status_label.text().lower()


def test_close_event_interrupts_newly_discovered_worker_on_retry(window, monkeypatch):
    class StubbornWorker:
        def __init__(self):
            self.interrupt_calls = 0

        def isRunning(self):
            return True

        def requestInterruption(self):
            self.interrupt_calls += 1

        def wait(self, _timeout_ms=0):
            return False

    first = StubbornWorker()
    second = StubbornWorker()
    window.init_worker = first
    window.current_worker = first
    window.everything_check_worker = first
    window.download_worker = first
    window._all_workers = [first]

    monkeypatch.setattr(window, "_schedule_timer", lambda *_args, **_kwargs: None)

    first_event = QCloseEvent()
    window.closeEvent(first_event)
    assert not first_event.isAccepted()
    assert first.interrupt_calls == 1

    # New running worker appears between retry passes.
    window.current_worker = second
    window._all_workers = [first, second]
    window._close_retry_pending = False

    second_event = QCloseEvent()
    window.closeEvent(second_event)
    assert not second_event.isAccepted()
    assert first.interrupt_calls == 1
    assert second.interrupt_calls == 1


def test_everything_worker_signature_drops_external_lock():
    signature = inspect.signature(EverythingCheckWorker.__init__)
    assert "access_lock" not in signature.parameters


def test_everything_worker_snapshots_input_results_list():
    results = [{"title": "One"}, {"title": "Two"}]
    worker = EverythingCheckWorker(object(), results, 42, 42, 10)

    results.clear()

    assert len(worker.results) == 2
    assert worker.results[0]["title"] == "One"


def test_track_worker_prunes_broken_wrappers_safely(window):
    class BrokenWorker:
        def isRunning(self):
            raise RuntimeError("wrapped C++ object deleted")

    class RunningWorker:
        def isRunning(self):
            return True

    running = RunningWorker()
    window._all_workers = [BrokenWorker()]
    window._track_worker(running)

    assert running in window._all_workers
    assert len(window._all_workers) == 1


def test_search_handlers_ignore_stale_worker(window):
    baseline = "baseline"
    active_worker = object()
    stale_worker = object()
    window.current_worker = active_worker
    window.status_label.setText(baseline)
    window.search_btn.setEnabled(False)

    window.on_search_progress("new status", stale_worker)
    assert window.status_label.text() == baseline

    window.page_fetch_finished(
        [{"title": "Ignored", "indexer": "X"}], 0.0, stale_worker
    )
    assert window.current_worker is active_worker

    window.search_error("ignored error", stale_worker)
    assert window.current_worker is active_worker
    assert window.search_btn.isEnabled() is False


def test_start_search_releases_lock_when_search_worker_constructor_fails(
    window, mocked_main, monkeypatch
):
    window.query_input.setText("constructor fail")

    def fail_worker(*_args, **_kwargs):
        raise RuntimeError("search ctor boom")

    monkeypatch.setattr(mocked_main, "SearchWorker", fail_worker)

    window.start_search()

    assert window.current_worker is None
    assert window.results_table.isSortingEnabled() is True
    assert "failed to start search" in window.status_label.text().lower()


def test_load_all_releases_lock_when_search_worker_constructor_fails(
    window, mocked_main, monkeypatch
):
    window.query_input.setText("load all ctor fail")

    def fail_worker(*_args, **_kwargs):
        raise RuntimeError("load-all ctor boom")

    monkeypatch.setattr(mocked_main, "SearchWorker", fail_worker)

    window.start_load_all_pages()

    assert window.current_worker is None
    assert window._load_all_active is False
    assert window.search_btn.isEnabled() is True
    assert window.load_all_btn.text() == "Load A&ll"
    assert window.results_table.isSortingEnabled() is True
    assert "failed to load page" in window.status_label.text().lower()


def test_fetch_page_releases_lock_when_search_worker_constructor_fails(
    window, mocked_main, monkeypatch
):
    window.query_input.setText("fetch ctor fail")

    def fail_worker(*_args, **_kwargs):
        raise RuntimeError("fetch ctor boom")

    monkeypatch.setattr(mocked_main, "SearchWorker", fail_worker)

    window.fetch_page(2)

    assert window.current_worker is None
    assert window.results_table.isSortingEnabled() is True
    assert "failed to fetch page" in window.status_label.text().lower()


def test_load_all_spinner_balanced_across_multiple_pages(window, monkeypatch):
    page1_worker = object()
    page2_worker = object()

    # Simulate a running Load All flow.
    window._load_all_active = True
    window._load_all_page = 1
    window.prowlarr_page_size = 2
    window._load_all_results = []
    window.current_worker = page1_worker
    window.start_spinner("search")

    def fake_next_page_fetch():
        # Real fetch path starts the spinner for the next page.
        window.current_worker = page2_worker
        window.start_spinner("search")

    monkeypatch.setattr(window, "_load_all_fetch_page", fake_next_page_fetch)

    window.page_fetch_finished(
        [{"title": "A", "indexer": "One"}, {"title": "B", "indexer": "One"}],
        0.0,
        page1_worker,
    )
    # Must remain a single active search spinner token.
    assert window._active_spinner_tags.get("search", 0) == 1

    window.page_fetch_finished(
        [{"title": "C", "indexer": "One"}],
        0.0,
        page2_worker,
    )
    assert window._active_spinner_tags.get("search", 0) == 0


def test_search_worker_passes_should_cancel_callback(qtbot):
    calls = {}

    class Client:
        def search(
            self, query, indexer_ids, categories, offset, limit, should_cancel=None
        ):
            calls["query"] = query
            calls["cancel"] = should_cancel
            return []

    worker = SearchWorker(Client(), "query", [1], [2000], 0, 100)
    emitted = {"done": False}
    worker.search_done.connect(
        lambda results, elapsed: emitted.__setitem__("done", True)
    )
    worker.run()

    assert calls["query"] == "query"
    assert callable(calls["cancel"])
    assert emitted["done"] is True


def test_download_worker_passes_should_cancel_callback(qtbot):
    calls = {}

    class Client:
        def download(self, guid, indexer_id, should_cancel=None):
            calls["guid"] = guid
            calls["indexer_id"] = indexer_id
            calls["cancel"] = should_cancel
            return True

    worker = DownloadWorker(Client(), [{"guid": "g1", "indexer_id": 7, "title": "One"}])
    worker.run()

    assert calls["guid"] == "g1"
    assert calls["indexer_id"] == 7
    assert callable(calls["cancel"])


def test_download_worker_add_items_returns_none_when_not_accepting():
    class Client:
        def download(self, guid, indexer_id, should_cancel=None):
            return True

    worker = DownloadWorker(Client(), [])
    worker._accepting_new_items = False
    result = worker.add_items([{"guid": "g1", "indexer_id": 1, "title": "One"}])
    assert result is None


def test_start_download_queue_retries_when_active_worker_no_longer_accepts(
    window, monkeypatch
):
    class ClosingWorker:
        def isRunning(self):
            return True

        def add_items(self, _items):
            return None

    scheduled = {"count": 0}

    def fake_schedule(_delay, _callback):
        scheduled["count"] += 1
        return None

    monkeypatch.setattr(window, "_schedule_timer", fake_schedule)
    window.download_worker = ClosingWorker()

    window.start_download_queue([{"guid": "g1", "indexer_id": 1, "title": "One"}])

    assert scheduled["count"] == 1
    assert "retrying enqueue" in window.status_label.text().lower()


def test_start_download_queue_recovers_when_add_items_raises_for_deleted_worker(
    window, mocked_main, monkeypatch
):
    class BrokenWorker:
        def add_items(self, _items):
            raise RuntimeError(
                "wrapped C/C++ object of type DownloadWorker has been deleted"
            )

        def isRunning(self):
            raise RuntimeError(
                "wrapped C/C++ object of type DownloadWorker has been deleted"
            )

    created = {"count": 0}

    class FreshWorker:
        def __init__(self, *_args, **_kwargs):
            created["count"] += 1
            self.progress = _SignalStub()
            self.item_downloaded = _SignalStub()
            self.queue_done = _SignalStub()
            self.started = False

        def start(self):
            self.started = True

        def isRunning(self):
            return True

    monkeypatch.setattr(mocked_main, "DownloadWorker", FreshWorker)
    monkeypatch.setattr(window, "_track_worker", lambda *_args, **_kwargs: None)
    window.download_worker = BrokenWorker()

    window.start_download_queue([{"guid": "g1", "indexer_id": 1, "title": "One"}])

    assert created["count"] == 1
    assert isinstance(window.download_worker, FreshWorker)
    assert window.download_worker.started is True


def test_start_download_queue_retries_when_add_items_raises_but_worker_still_running(
    window, monkeypatch
):
    class RunningBrokenWorker:
        def add_items(self, _items):
            raise RuntimeError("transient enqueue failure")

        def isRunning(self):
            return True

    scheduled = {"count": 0}
    monkeypatch.setattr(
        window,
        "_schedule_timer",
        lambda *_args, **_kwargs: scheduled.__setitem__(
            "count", scheduled["count"] + 1
        ),
    )
    window.download_worker = RunningBrokenWorker()

    window.start_download_queue([{"guid": "g1", "indexer_id": 1, "title": "One"}])

    assert scheduled["count"] == 1
    assert "retrying enqueue" in window.status_label.text().lower()


def test_start_download_queue_retries_while_old_worker_reference_exists(
    window, mocked_main, monkeypatch
):
    class FinishingWorker:
        def __init__(self):
            self.add_calls = 0

        def isRunning(self):
            return False

        def add_items(self, _items):
            self.add_calls += 1
            return None

    def fail_new_worker(*_args, **_kwargs):
        raise AssertionError(
            "should not create a replacement worker before queue_done clears ownership"
        )

    scheduled = {"count": 0}
    monkeypatch.setattr(mocked_main, "DownloadWorker", fail_new_worker)
    monkeypatch.setattr(
        window,
        "_schedule_timer",
        lambda _delay, _cb: scheduled.__setitem__("count", scheduled["count"] + 1),
    )

    finishing = FinishingWorker()
    window.download_worker = finishing

    window.start_download_queue([{"guid": "g1", "indexer_id": 1, "title": "One"}])

    assert finishing.add_calls == 1
    assert scheduled["count"] == 1
    assert "retrying enqueue" in window.status_label.text().lower()


def test_start_download_queue_retry_circuit_breaker_resets_stale_owner(
    window, mocked_main, monkeypatch
):
    class StaleWorker:
        def isRunning(self):
            return False

        def add_items(self, _items):
            return None

    created = {"count": 0}

    class FreshWorker:
        def __init__(self, *_args, **_kwargs):
            created["count"] += 1
            self.progress = _SignalStub()
            self.item_downloaded = _SignalStub()
            self.queue_done = _SignalStub()
            self.started = False

        def start(self):
            self.started = True

        def isRunning(self):
            return True

    monkeypatch.setattr(mocked_main, "DownloadWorker", FreshWorker)
    monkeypatch.setattr(window, "_track_worker", lambda *_args, **_kwargs: None)
    window._download_queue_retry_limit = 1
    window.download_worker = StaleWorker()

    window.start_download_queue(
        [{"guid": "g1", "indexer_id": 1, "title": "One"}], retry_attempt=1
    )

    assert created["count"] == 1
    assert isinstance(window.download_worker, FreshWorker)
    assert window.download_worker.started is True


def test_start_download_queue_retry_circuit_breaker_stops_when_worker_still_running(
    window, monkeypatch
):
    class RunningClosingWorker:
        def isRunning(self):
            return True

        def add_items(self, _items):
            return None

    scheduled = {"count": 0}
    monkeypatch.setattr(
        window,
        "_schedule_timer",
        lambda *_args, **_kwargs: scheduled.__setitem__(
            "count", scheduled["count"] + 1
        ),
    )
    window._download_queue_retry_limit = 1
    window.download_worker = RunningClosingWorker()

    window.start_download_queue(
        [{"guid": "g1", "indexer_id": 1, "title": "One"}], retry_attempt=1
    )

    assert scheduled["count"] == 0
    assert "busy shutting down" in window.status_label.text().lower()


def test_download_queue_stale_owner_watchdog_recovers_ui(window):
    class NotRunningWorker:
        def isRunning(self):
            return False

    window.download_worker = NotRunningWorker()
    window._download_queue_stale_grace_seconds = 0.1
    window._download_queue_owner_since = time.monotonic() - 1.0
    window._acquire_table_sort_lock("download")
    window.search_btn.setEnabled(False)

    active = window._is_download_queue_active()

    assert active is False
    assert window.download_worker is None
    assert window.search_btn.isEnabled() is True
    assert window.results_table.isSortingEnabled() is True


def test_action_entrypoints_blocked_while_shutdown_in_progress(window):
    window._shutdown_in_progress = True
    window.query_input.setText("some query")

    window.start_search()
    assert "shutdown in progress" in window.status_label.text().lower()
    assert window.current_worker is None

    window.start_load_all_pages()
    assert "shutdown in progress" in window.status_label.text().lower()
    assert window._load_all_active is False

    window.fetch_page(2)
    assert "shutdown in progress" in window.status_label.text().lower()
    assert window.current_worker is None

    window.start_download_queue([{"guid": "g1", "indexer_id": 1, "title": "One"}])
    assert "shutdown in progress" in window.status_label.text().lower()
    assert window.download_worker is None


def test_prowlarr_client_request_cancels_before_network(monkeypatch):
    client = ProwlarrClient("http://fake-host", "api", retries=3)
    calls = {"count": 0}

    from prowlarr_ui.api import prowlarr_client as pc_module

    def fake_get(*_args, **_kwargs):
        calls["count"] += 1
        raise AssertionError("network should not be called after cancellation")

    monkeypatch.setattr(pc_module.requests, "get", fake_get)

    try:
        client._api_request("search", should_cancel=lambda: True)
        raise AssertionError("expected cancellation error")
    except RuntimeError as e:
        assert "cancelled" in str(e).lower()

    assert calls["count"] == 0


def test_prowlarr_client_request_cancels_during_backoff(monkeypatch):
    class FakeResponse:
        status_code = 500
        text = "{}"

        @staticmethod
        def json():
            return {}

        def raise_for_status(self):
            return None

    client = ProwlarrClient("http://fake-host", "api", retries=3)
    from prowlarr_ui.api import prowlarr_client as pc_module

    calls = {"count": 0}

    def fake_get(*_args, **_kwargs):
        calls["count"] += 1
        return FakeResponse()

    monkeypatch.setattr(pc_module.requests, "get", fake_get)
    monkeypatch.setattr(client, "_sleep_with_cancel", lambda *_args, **_kwargs: True)

    try:
        client._api_request("search", should_cancel=lambda: False)
        raise AssertionError("expected cancellation error")
    except RuntimeError as e:
        assert "cancelled" in str(e).lower()

    # Stop immediately after first failed attempt/backoff.
    assert calls["count"] == 1


def test_prowlarr_client_uses_configured_timeout_for_cancellable_requests(monkeypatch):
    class FakeResponse:
        status_code = 200
        text = "{}"

        @staticmethod
        def json():
            return {}

        def raise_for_status(self):
            return None

    client = ProwlarrClient("http://fake-host", "api", timeout=120, retries=0)
    from prowlarr_ui.api import prowlarr_client as pc_module

    captured = {}

    def fake_get(*_args, **kwargs):
        captured["timeout"] = kwargs.get("timeout")
        return FakeResponse()

    monkeypatch.setattr(pc_module.requests, "get", fake_get)

    client._api_request("search", should_cancel=lambda: False)
    timeout_value = captured["timeout"]
    assert isinstance(timeout_value, float)
    assert timeout_value == 120.0


def test_close_event_does_not_schedule_duplicate_retry_when_already_pending(
    window, monkeypatch
):
    class StubbornWorker:
        def isRunning(self):
            return True

        def requestInterruption(self):
            return None

        def wait(self, _timeout_ms=0):
            return False

    stubborn = StubbornWorker()
    window.init_worker = stubborn
    window.current_worker = stubborn
    window.everything_check_worker = stubborn
    window.download_worker = stubborn
    window._all_workers = [stubborn]
    window._close_retry_pending = True

    scheduled = {"count": 0}
    monkeypatch.setattr(
        window,
        "_schedule_timer",
        lambda _delay, _cb: scheduled.__setitem__("count", scheduled["count"] + 1),
    )

    event = QCloseEvent()
    window.closeEvent(event)

    assert not event.isAccepted()
    assert scheduled["count"] == 0


def test_retry_close_noops_when_shutdown_not_active(window, monkeypatch):
    calls = {"count": 0}
    monkeypatch.setattr(
        window, "close", lambda: calls.__setitem__("count", calls["count"] + 1)
    )
    window._shutdown_in_progress = False

    window._retry_close()

    assert calls["count"] == 0


def test_close_event_retries_use_non_blocking_waits_after_first_attempt(
    window, monkeypatch
):
    class StubbornWorker:
        def __init__(self):
            self.interrupt_calls = 0
            self.wait_calls = []

        def isRunning(self):
            return True

        def requestInterruption(self):
            self.interrupt_calls += 1

        def wait(self, timeout_ms=0):
            self.wait_calls.append(timeout_ms)
            return False

    stubborn = StubbornWorker()
    window.init_worker = stubborn
    window.current_worker = stubborn
    window.everything_check_worker = stubborn
    window.download_worker = stubborn
    window._all_workers = [stubborn]

    scheduled = {"count": 0}
    monkeypatch.setattr(
        window,
        "_schedule_timer",
        lambda _delay, _cb: scheduled.__setitem__("count", scheduled["count"] + 1),
    )

    first_event = QCloseEvent()
    window.closeEvent(first_event)
    assert not first_event.isAccepted()
    assert window._shutdown_in_progress is True
    assert 75 in stubborn.wait_calls
    first_interrupt_calls = stubborn.interrupt_calls
    first_wait_len = len(stubborn.wait_calls)

    # Simulate a subsequent retry pass: no long waits, and no repeated interrupt flood.
    window._close_retry_pending = False
    second_event = QCloseEvent()
    window.closeEvent(second_event)
    assert not second_event.isAccepted()

    second_wait_slice = stubborn.wait_calls[first_wait_len:]
    assert second_wait_slice == []
    assert stubborn.interrupt_calls == first_interrupt_calls
