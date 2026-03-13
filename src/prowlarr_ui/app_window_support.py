"""Window support helpers for download history, row state, and spinner UI."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING

from threep_commons.desktop import open_path_in_default_app

if TYPE_CHECKING:
    from collections.abc import Callable

    from .app import EverythingMatches, MainWindow, ReleaseKey

logger = logging.getLogger(__name__)


def write_download_history(
    history_path: str,
    title: str,
    indexer: str,
    success: bool,
) -> None:
    """Append one download record to the persistent rotated history log."""
    try:
        max_size = 10 * 1024 * 1024
        os.makedirs(os.path.dirname(history_path), exist_ok=True)

        if os.path.exists(history_path) and os.path.getsize(history_path) > max_size:
            for index in range(4, 0, -1):
                old_file = f"{history_path}.{index}"
                new_file = f"{history_path}.{index + 1}"
                if os.path.exists(old_file):
                    os.replace(old_file, new_file)
            os.replace(history_path, f"{history_path}.1")
            logger.info(
                "Rotated download history log "
                f"(exceeded {max_size / 1024 / 1024:.1f} MB)"
            )

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "OK" if success else "FAIL"
        safe_title = title.replace("\t", " ").replace("\n", " ").replace("\r", "")
        safe_indexer = indexer.replace("\t", " ").replace("\n", " ").replace("\r", "")
        with open(history_path, "a", encoding="utf-8") as file_handle:
            file_handle.write(f"{timestamp}\t{status}\t{safe_indexer}\t{safe_title}\n")
    except Exception as exc:
        logger.error(f"Failed to write download history: {exc}")


def open_download_history(
    window: MainWindow,
    history_path: str,
    opener: Callable[[str], bool] = open_path_in_default_app,
) -> None:
    """Open the persistent download history log in the default system app."""
    if not os.path.exists(history_path):
        window.status_label.setText("No download history yet")
        return
    try:
        if not opener(history_path):
            raise RuntimeError("No default opener available")
    except Exception as exc:
        logger.error(f"Failed to open file: {exc}")
        window.status_label.setText(f"Cannot open file: {exc}")


def edit_preferences_ini_file(
    window: MainWindow,
    opener: Callable[[str], bool] = open_path_in_default_app,
) -> None:
    """Open the shared preferences INI file in the default system app."""
    ini_path = window.preferences_store.file_name()
    try:
        window.preferences_store.sync()
        ini_dir = os.path.dirname(os.path.abspath(ini_path))
        if ini_dir:
            os.makedirs(ini_dir, exist_ok=True)
        if not os.path.exists(ini_path):
            with open(ini_path, "a", encoding="utf-8"):
                pass
        if not opener(ini_path):
            raise RuntimeError("No default opener available")
    except Exception as exc:
        logger.error(f"Failed to open preferences INI file: {exc}")
        window.status_label.setText(f"Cannot open INI file: {exc}")


def update_download_button_states(window: MainWindow) -> None:
    """Refresh download button enabled state from visibility and selection."""

    def is_row_downloadable(row: int) -> bool:
        if window.results_table.isRowHidden(row):
            return False
        button = window.results_table.cellWidget(row, window.COL_DOWNLOAD)
        if not hasattr(button, "property"):
            return False
        guid = window.text_value(button.property("guid"))
        indexer_id = window.int_value(button.property("indexerId"), -1)
        return not window.is_release_downloaded(guid, indexer_id)

    has_visible_downloadable = any(
        is_row_downloadable(row) for row in range(window.results_table.rowCount())
    )
    window.download_all_btn.setEnabled(has_visible_downloadable)

    selected_rows = {index.row() for index in window.results_table.selectedIndexes()}
    has_selected_downloadable = any(is_row_downloadable(row) for row in selected_rows)
    window.download_selected_btn.setEnabled(has_selected_downloadable)


def get_current_row_title(window: MainWindow) -> str | None:
    """Return the title text from the currently selected results row."""
    current_row = window.results_table.currentRow()
    if current_row < 0:
        return None
    title_item = window.results_table.item(current_row, window.COL_TITLE)
    if title_item is None:
        return None
    return title_item.text()


def find_video_file(
    everything_results: EverythingMatches,
    video_extensions: set[str],
) -> str | None:
    """Return the first video file path from Everything search results."""
    logger.debug(f"_find_video_file: checking {len(everything_results)} results")
    for index, item in enumerate(everything_results):
        try:
            file_path, _size = item
            _, extension = os.path.splitext(file_path)
            if extension.lower() in video_extensions:
                logger.debug(f"_find_video_file: FOUND at index {index}: {file_path}")
                return file_path
        except Exception as exc:
            logger.warning(f"_find_video_file: ERROR at index {index}: {exc} {item!r}")
    logger.debug("_find_video_file: NO VIDEO FOUND")
    return None


def get_video_path_for_row(
    release_key: ReleaseKey | None,
    video_paths: dict[ReleaseKey, str],
) -> str | None:
    """Return the tracked video path for one results-table row."""
    if release_key is None:
        return None
    return video_paths.get(release_key)


def refresh_spinner(window: MainWindow, has_active_tags: bool) -> None:
    """Apply spinner state from the active-operation tags."""
    if has_active_tags:
        window.activity_bar.setRange(0, 0)
        return
    window.activity_bar.setRange(0, 1)
    window.activity_bar.setValue(1)


def start_spinner(
    window: MainWindow,
    spinner_tags: dict[str, int],
    tag: str = "default",
) -> None:
    """Mark one operation tag as active and refresh spinner state."""
    spinner_tags[tag] = spinner_tags.get(tag, 0) + 1
    refresh_spinner(window, has_active_tags=bool(spinner_tags))


def stop_spinner(
    window: MainWindow,
    spinner_tags: dict[str, int],
    tag: str = "default",
) -> None:
    """Mark one operation tag as complete and refresh spinner state."""
    count = spinner_tags.get(tag, 0)
    if count <= 1:
        spinner_tags.pop(tag, None)
    else:
        spinner_tags[tag] = count - 1
    refresh_spinner(window, has_active_tags=bool(spinner_tags))


def update_status(window: MainWindow, message: str) -> None:
    """Update the status label and append the message to the runtime log."""
    window.status_label.setText(message)
    window.log(message)


def toggle_log_window(window: MainWindow) -> None:
    """Show or hide the detached log window."""
    if window.log_window.isVisible():
        window.log_window.hide()
        return
    window.log_window.show()
