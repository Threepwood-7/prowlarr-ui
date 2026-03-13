"""Result-table navigation and keyboard shortcut helpers."""

from __future__ import annotations

import os
import shlex
import subprocess
import webbrowser
from typing import TYPE_CHECKING, cast
from urllib.parse import quote

from PySide6.QtCore import QEvent, QItemSelection, QItemSelectionModel, QObject, Qt
from PySide6.QtWidgets import QApplication, QTableWidget
from threep_commons.desktop import open_path_in_default_app

if TYPE_CHECKING:
    from PySide6.QtGui import QKeyEvent

    from .app import MainWindow


def toggle_find_bar(window: MainWindow) -> None:
    """Toggle the inline find bar for the results table."""
    if window.find_bar.isVisible():
        close_find_bar(window)
        return
    window.find_bar.setVisible(True)
    window.find_input.setFocus()
    window.find_input.selectAll()


def close_find_bar(window: MainWindow) -> None:
    """Hide the find bar and return focus to the results table."""
    window.find_bar.setVisible(False)
    window.results_table.setFocus()


def find_next(window: MainWindow) -> None:
    """Jump to the next visible result that matches the current find text."""
    find_in_table(window, forward=True)


def find_prev(window: MainWindow) -> None:
    """Jump to the previous visible result that matches the current find text."""
    find_in_table(window, forward=False)


def find_in_table(window: MainWindow, forward: bool = True) -> None:
    """Search visible result titles and select the next or previous match."""
    text = window.find_input.text().strip().lower()
    if not text:
        return

    row_count = window.results_table.rowCount()
    if row_count == 0:
        return

    current = window.results_table.currentRow()
    start = (current + (1 if forward else -1)) % row_count

    for index in range(row_count):
        row = (start + (index if forward else -index)) % row_count
        if window.results_table.isRowHidden(row):
            continue
        title_item = window.results_table.item(row, window.COL_TITLE)
        if title_item is None or text not in title_item.text().lower():
            continue
        window.results_table.setCurrentCell(row, window.COL_TITLE)
        window.results_table.scrollToItem(title_item)
        window.status_label.setText(f"Found: row {row + 1}")
        return

    window.status_label.setText(f"Not found: '{window.find_input.text()}'")


def handle_find_event(window: MainWindow, obj: QObject, event: QEvent) -> bool:
    """Handle Escape and Shift+Enter shortcuts while the find box has focus."""
    if obj != window.find_input or event.type() != QEvent.Type.KeyPress:
        return False
    key_event = cast("QKeyEvent", event)
    if key_event.key() == Qt.Key.Key_Escape:
        close_find_bar(window)
        return True
    if (
        key_event.key() == Qt.Key.Key_Return
        and key_event.modifiers() & Qt.KeyboardModifier.ShiftModifier
    ):
        find_prev(window)
        return True
    return False


def table_key_press(window: MainWindow, event: QKeyEvent) -> None:
    """Handle the result-table keyboard shortcuts and delegate everything else."""
    key = Qt.Key(event.key())
    modifiers = event.modifiers()

    if key == Qt.Key.Key_Space and _handle_table_download_shortcut(window, event):
        return
    if key == Qt.Key.Key_S and _handle_everything_launch_shortcut(window, event):
        return
    if key == Qt.Key.Key_C and _handle_copy_title_shortcut(window, event):
        return
    if key == Qt.Key.Key_G and _handle_web_search_shortcut(window, event):
        return
    if key == Qt.Key.Key_P and _handle_play_video_shortcut(window, event):
        return
    if _handle_custom_command_shortcut(window, key, event):
        return
    if key == Qt.Key.Key_A and modifiers & Qt.KeyboardModifier.ControlModifier:
        _handle_select_all_visible_shortcut(window, event)
        return
    if key == Qt.Key.Key_Tab:
        _handle_title_group_jump_shortcut(
            window,
            not bool(modifiers & Qt.KeyboardModifier.ShiftModifier),
            event,
        )
        return

    QTableWidget.keyPressEvent(window.results_table, event)


def jump_title_group(window: MainWindow, forward: bool = True) -> None:
    """Move selection to the next or previous title group boundary."""
    current_row = window.results_table.currentRow()
    if current_row < 0:
        return

    row_count = window.results_table.rowCount()
    if row_count == 0:
        return

    current_title_item = window.results_table.item(current_row, window.COL_TITLE)
    if current_title_item is None:
        return
    current_key = current_title_item.text()[: window.title_match_chars].lower()

    step = 1 if forward else -1
    row = current_row + step
    while 0 <= row < row_count:
        if not window.results_table.isRowHidden(row):
            title_item = window.results_table.item(row, window.COL_TITLE)
            if title_item is not None:
                key = title_item.text()[: window.title_match_chars].lower()
                if key != current_key:
                    window.results_table.setCurrentCell(row, window.COL_TITLE)
                    window.results_table.scrollToItem(title_item)
                    return
        row += step


def run_custom_command(window: MainWindow, key: Qt.Key, cmd_template: str) -> None:
    """Run one configured custom command with title and video placeholders."""
    current_row = window.results_table.currentRow()
    if current_row < 0:
        window.status_label.setText("No row selected")
        return

    title = window.get_current_row_title() or ""
    video = window.get_video_path_for_row(current_row) or ""
    key_name = {Qt.Key.Key_F2: "F2", Qt.Key.Key_F3: "F3", Qt.Key.Key_F4: "F4"}.get(
        key,
        "?",
    )

    command = cmd_template.replace("{title}", title).replace("{video}", video)
    window.log(f"{key_name} command: {command}")
    window.status_label.setText(f"Running {key_name} command...")

    try:
        args = shlex.split(command, posix=False)
        subprocess.Popen(args)
    except Exception as exc:
        window.log(f"{key_name} command failed: {exc}")
        window.status_label.setText(f"{key_name} command failed: {exc}")


def _move_to_next_visible_result_row(window: MainWindow, current_row: int) -> None:
    """Advance selection to the next visible result row."""
    next_row = current_row + 1
    while (
        next_row < window.results_table.rowCount()
        and window.results_table.isRowHidden(next_row)
    ):
        next_row += 1
    if next_row < window.results_table.rowCount():
        window.results_table.setCurrentCell(next_row, window.COL_AGE)


def _handle_table_download_shortcut(window: MainWindow, event: QKeyEvent) -> bool:
    """Download the current row and move to the next visible result."""
    current_row = window.results_table.currentRow()
    if current_row < 0:
        return False
    window.download_release(current_row)
    _move_to_next_visible_result_row(window, current_row)
    event.accept()
    return True


def _handle_everything_launch_shortcut(window: MainWindow, event: QKeyEvent) -> bool:
    """Launch Everything for the current release title."""
    if not window.everything:
        window.status_label.setText("Everything not initialized yet")
        event.accept()
        return True
    title = window.get_current_row_title()
    if not title:
        return False
    window.everything.launch_search(title)
    window.log(f"Launched Everything search for: {title}")
    event.accept()
    return True


def _handle_copy_title_shortcut(window: MainWindow, event: QKeyEvent) -> bool:
    """Copy the current release title to the clipboard."""
    title = window.get_current_row_title()
    if not title:
        return False
    QApplication.clipboard().setText(title)
    window.log(f"Copied to clipboard: {title}")
    window.status_label.setText("Title copied to clipboard")
    event.accept()
    return True


def _handle_web_search_shortcut(window: MainWindow, event: QKeyEvent) -> bool:
    """Open the configured web search for the current release title."""
    title = window.get_current_row_title()
    if not title:
        return False
    url = window.web_search_url.replace("{query}", quote(title))
    webbrowser.open(url)
    window.log(f"Opened web search for: {title}")
    event.accept()
    return True


def _handle_play_video_shortcut(window: MainWindow, event: QKeyEvent) -> bool:
    """Open the Everything-matched video file for the current row."""
    current_row = window.results_table.currentRow()
    if current_row < 0:
        return False
    video_path = window.get_video_path_for_row(current_row)
    if video_path:
        window.log(f"Playing: {video_path}")
        window.status_label.setText(f"Playing: {os.path.basename(video_path)}")
        open_path_in_default_app(video_path)
    else:
        window.status_label.setText("No video file found in Everything results")
    event.accept()
    return True


def _handle_custom_command_shortcut(
    window: MainWindow,
    key: Qt.Key,
    event: QKeyEvent,
) -> bool:
    """Run the custom command bound to one function key, if configured."""
    if key not in window.custom_commands:
        return False
    command = window.custom_commands[key]
    if command:
        run_custom_command(window, key, command)
    else:
        key_name = {
            Qt.Key.Key_F2: "F2",
            Qt.Key.Key_F3: "F3",
            Qt.Key.Key_F4: "F4",
        }.get(key, "?")
        window.status_label.setText(
            "No custom command configured for "
            f"{key_name} (set custom_command_{key_name} in config)"
        )
    event.accept()
    return True


def _handle_select_all_visible_shortcut(
    window: MainWindow,
    event: QKeyEvent,
) -> bool:
    """Select every visible row in the result table."""
    selection_model = window.results_table.selectionModel()
    selection = QItemSelection()
    column_count = window.results_table.columnCount()
    model = window.results_table.model()
    for row in range(window.results_table.rowCount()):
        if window.results_table.isRowHidden(row):
            continue
        top_left = model.index(row, 0)
        bottom_right = model.index(row, column_count - 1)
        selection.select(top_left, bottom_right)
    selection_model.select(
        selection,
        QItemSelectionModel.SelectionFlag.ClearAndSelect,
    )
    visible = len(selection.indexes()) // max(column_count, 1)
    window.status_label.setText(f"Selected {visible} visible rows")
    event.accept()
    return True


def _handle_title_group_jump_shortcut(
    window: MainWindow,
    forward: bool,
    event: QKeyEvent,
) -> bool:
    """Jump to the next or previous title group."""
    jump_title_group(window, forward=forward)
    event.accept()
    return True
