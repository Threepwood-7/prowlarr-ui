"""Result-table context menu helpers."""

from __future__ import annotations

import logging
import webbrowser
from typing import TYPE_CHECKING
from urllib.parse import quote

from PySide6.QtCore import QPoint, Qt
from PySide6.QtWidgets import QApplication, QMenu
from threep_commons.desktop import open_path_in_default_app

from .app_results_navigation import run_custom_command

if TYPE_CHECKING:
    from .app import MainWindow

logger = logging.getLogger(__name__)


def on_cell_double_clicked(window: MainWindow, row: int) -> None:
    """Trigger the standard download action for a double-clicked row."""
    window.download_release(row)


def show_header_context_menu(window: MainWindow, pos: QPoint) -> None:
    """Show the header menu used to toggle result-column visibility."""
    menu = QMenu(window)
    for column in range(window.COL_COUNT):
        if column == window.COL_TITLE:
            continue
        name = window.COL_HEADERS[column]
        action = menu.addAction(name)
        action.setCheckable(True)
        action.setChecked(not window.results_table.isColumnHidden(column))

        def toggle_column(checked: bool, col: int = column) -> None:
            window.toggle_column_visibility(col, not checked)

        action.toggled.connect(toggle_column)
    menu.exec(window.results_table.horizontalHeader().mapToGlobal(pos))


def show_context_menu(window: MainWindow, pos: QPoint) -> None:
    """Show the per-row context menu for the results table."""
    row = window.results_table.rowAt(pos.y())
    if row < 0:
        return

    menu = QMenu(window)
    _add_download_action(window, menu, row)
    menu.addSeparator()
    _add_copy_title_action(window, menu, row)
    _add_web_search_action(window, menu, row)
    _add_play_video_action(window, menu, row)
    _add_everything_search_action(window, menu, row)
    _add_custom_command_actions(window, menu)
    menu.exec(window.results_table.viewport().mapToGlobal(pos))


def context_copy_title(window: MainWindow, row: int) -> None:
    """Copy the title text for one row to the clipboard."""
    try:
        title_item = window.results_table.item(row, window.COL_TITLE)
        if title_item is None:
            return
        title = title_item.text()
        QApplication.clipboard().setText(title)
        window.log(f"Copied to clipboard: {title}")
        window.status_label.setText("Title copied to clipboard")
    except Exception as exc:
        logger.error(f"Failed to copy title: {exc}")


def context_web_search(window: MainWindow, row: int) -> None:
    """Open the configured web search for one result row."""
    try:
        title_item = window.results_table.item(row, window.COL_TITLE)
        if title_item is None:
            return
        title = title_item.text()
        url = window.web_search_url.replace("{query}", quote(title))
        webbrowser.open(url)
        window.log(f"Opened web search for: {title}")
    except Exception as exc:
        logger.error(f"Failed to open web search: {exc}")


def _add_download_action(window: MainWindow, menu: QMenu, row: int) -> None:
    """Add the standard download action for one result row."""
    download_action = menu.addAction("Download (Space)")

    def trigger_download() -> None:
        window.download_release(row)

    download_action.triggered.connect(trigger_download)


def _add_copy_title_action(window: MainWindow, menu: QMenu, row: int) -> None:
    """Add the copy-title action for one result row."""
    copy_action = menu.addAction("Copy Title (C)")

    def trigger_copy_title() -> None:
        context_copy_title(window, row)

    copy_action.triggered.connect(trigger_copy_title)


def _add_web_search_action(window: MainWindow, menu: QMenu, row: int) -> None:
    """Add the web-search action for one result row."""
    web_action = menu.addAction("Web Search (G)")

    def trigger_web_search() -> None:
        context_web_search(window, row)

    web_action.triggered.connect(trigger_web_search)


def _add_play_video_action(window: MainWindow, menu: QMenu, row: int) -> None:
    """Add the play-video action for one result row when a file is known."""
    play_action = menu.addAction("Play Video (P)")
    video_path = window.get_video_path_for_row(row)
    play_action.setEnabled(video_path is not None)

    def play_video() -> None:
        try:
            if video_path:
                open_path_in_default_app(video_path)
        except Exception as exc:
            logger.error(f"Failed to play video: {exc}")

    play_action.triggered.connect(play_video)


def _add_everything_search_action(
    window: MainWindow,
    menu: QMenu,
    row: int,
) -> None:
    """Add the Everything search action when the integration is available."""
    if not window.everything:
        return
    everything = window.everything
    everything_action = menu.addAction("Search Everything (S)")
    title_item = window.results_table.item(row, window.COL_TITLE)
    title = title_item.text() if title_item else None
    everything_action.setEnabled(title is not None)

    def search_everything() -> None:
        try:
            if title:
                everything.launch_search(title)
        except Exception as exc:
            logger.error(f"Failed to launch Everything: {exc}")

    everything_action.triggered.connect(search_everything)


def _add_custom_command_actions(window: MainWindow, menu: QMenu) -> None:
    """Add configured custom command actions to the row context menu."""
    for key, label in [
        (Qt.Key.Key_F2, "F2"),
        (Qt.Key.Key_F3, "F3"),
        (Qt.Key.Key_F4, "F4"),
    ]:
        command = window.custom_commands.get(key, "")
        if not command:
            continue
        action = menu.addAction(f"Custom Command {label}")

        def trigger_custom_command(
            _checked: bool = False,
            command_key: Qt.Key = key,
            command_text: str = command,
        ) -> None:
            run_custom_command(window, command_key, command_text)

        action.triggered.connect(trigger_custom_command)
