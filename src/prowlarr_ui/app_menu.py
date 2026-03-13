"""Menu and help-dialog helpers for the main Prowlarr window."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import QAction, QColor, QIcon, QKeySequence, QPainter, QPen, QPixmap
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QMenuBar,
    QTextBrowser,
    QVBoxLayout,
)

from .ui.help_text import HELP_HTML

if TYPE_CHECKING:
    from .app import MainWindow


def create_globe_icon() -> QIcon:
    """Build the application globe icon used for the main window."""
    size = 64
    pixmap = QPixmap(size, size)
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    pen = QPen(QColor(60, 130, 200), 2)
    painter.setPen(pen)
    painter.setBrush(QColor(180, 220, 255))
    margin = 3
    painter.drawEllipse(margin, margin, size - 2 * margin, size - 2 * margin)
    painter.setBrush(Qt.BrushStyle.NoBrush)
    center_x = size // 2
    center_y = size // 2
    radius = size // 2 - margin
    for offset in (-radius // 3, 0, radius // 3):
        width = abs(radius - abs(offset))
        painter.drawEllipse(center_x + offset - width // 2, margin, width, size - 6)
    for delta_y in (-radius // 3, 0, radius // 3):
        half_width = int(math.sqrt(max(0, radius * radius - delta_y * delta_y)))
        painter.drawLine(
            center_x - half_width,
            center_y + delta_y,
            center_x + half_width,
            center_y + delta_y,
        )
    painter.end()
    return QIcon(pixmap)


def setup_main_window_menu(window: MainWindow) -> None:
    """Create the full menu bar for the main application window."""
    menubar = window.menuBar()
    _build_file_menu(window, menubar)
    _build_view_menu(window, menubar)
    _build_tools_menu(window, menubar)
    _build_bookmarks_menu(window, menubar)
    _build_help_menu(window, menubar)


def show_help_dialog(window: MainWindow) -> None:
    """Show the scrollable keyboard-shortcuts and usage help dialog."""
    dialog = QDialog(window)
    dialog.setWindowTitle("Help")
    dialog.resize(520, 480)
    layout = QVBoxLayout(dialog)
    browser = QTextBrowser(dialog)
    browser.setOpenExternalLinks(False)
    browser.setHtml(HELP_HTML)
    layout.addWidget(browser)
    button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
    button_box.accepted.connect(dialog.accept)
    layout.addWidget(button_box)
    dialog.exec()


def _build_file_menu(window: MainWindow, menubar: QMenuBar) -> None:
    """Add the File menu and exit action."""
    file_menu = menubar.addMenu("&File")
    exit_action = QAction("E&xit", window)
    exit_action.setShortcuts([QKeySequence("Ctrl+Q"), QKeySequence("Alt+X")])
    exit_action.setShortcutContext(Qt.ShortcutContext.ApplicationShortcut)
    exit_action.setStatusTip("Close the application")
    exit_action.triggered.connect(window.close)
    file_menu.addAction(exit_action)


def _build_view_menu(window: MainWindow, menubar: QMenuBar) -> None:
    """Add the View menu and result-table utility actions."""
    view_menu = menubar.addMenu("&View")
    log_action = QAction("Show &Log", window)
    log_action.setStatusTip("Open the log window to view application messages")
    log_action.triggered.connect(window.toggle_log_window)
    view_menu.addAction(log_action)

    history_action = QAction("Download &History", window)
    history_action.setStatusTip("View the log of previously downloaded items")
    history_action.triggered.connect(window.open_download_history)
    view_menu.addAction(history_action)
    view_menu.addSeparator()

    best_per_group_action = QAction("Select &Best per Group", window)
    best_per_group_action.setStatusTip(
        "Highlight the best result in each title group based on size and seeders"
    )
    best_per_group_action.triggered.connect(window.select_best_per_group)
    view_menu.addAction(best_per_group_action)

    reset_sort_action = QAction("&Reset Sorting", window)
    reset_sort_action.setStatusTip(
        "Restore default sort order: Title ASC, Indexer DESC, Age ASC"
    )
    reset_sort_action.triggered.connect(window.apply_default_sort)
    view_menu.addAction(reset_sort_action)

    fit_columns_action = QAction("&Fit Columns", window)
    fit_columns_action.setStatusTip("Resize visible columns to fit their contents")
    fit_columns_action.triggered.connect(window.fit_columns)
    view_menu.addAction(fit_columns_action)

    reset_view_action = QAction("Reset &View", window)
    reset_view_action.setStatusTip(
        "Reset column widths, splitter position, and sort order to defaults"
    )
    reset_view_action.triggered.connect(window.reset_view)
    view_menu.addAction(reset_view_action)


def _build_tools_menu(window: MainWindow, menubar: QMenuBar) -> None:
    """Add the Tools menu and preferences action."""
    tools_menu = menubar.addMenu("&Tools")
    edit_ini_action = QAction("Edit &.ini File", window)
    edit_ini_action.setStatusTip(
        f"Open preferences INI file: {window.preferences_store.file_name()}"
    )
    edit_ini_action.triggered.connect(window.edit_preferences_ini_file)
    tools_menu.addAction(edit_ini_action)


def _build_bookmarks_menu(window: MainWindow, menubar: QMenuBar) -> None:
    """Add bookmark actions and load persisted bookmark entries."""
    window.bookmarks_menu = menubar.addMenu("&Bookmarks")

    add_bookmark_action = QAction("&Add Bookmark", window)
    add_bookmark_action.setStatusTip("Save the current search query as a bookmark")
    add_bookmark_action.triggered.connect(window.add_bookmark)
    window.bookmarks_menu.addAction(add_bookmark_action)

    remove_bookmark_action = QAction("&Delete Bookmark", window)
    remove_bookmark_action.setStatusTip("Remove a saved bookmark from the list")
    remove_bookmark_action.triggered.connect(window.remove_bookmark)
    window.bookmarks_menu.addAction(remove_bookmark_action)

    sort_bookmark_action = QAction("&Sort Bookmarks", window)
    sort_bookmark_action.setStatusTip("Sort all bookmarks alphabetically")
    sort_bookmark_action.triggered.connect(window.sort_bookmarks)
    window.bookmarks_menu.addAction(sort_bookmark_action)

    window.bookmarks_separator = window.bookmarks_menu.addSeparator()
    bookmarks = window.preferences_store.get_str_list(
        window.pref_key("bookmarks"),
        [],
    )
    window.replace_bookmarks(bookmarks)
    for bookmark in bookmarks:
        window.add_bookmark_action(bookmark)


def _build_help_menu(window: MainWindow, menubar: QMenuBar) -> None:
    """Add the Help menu and keyboard-shortcuts dialog action."""
    help_menu = menubar.addMenu("&Help")
    help_action = QAction("&Help", window)
    help_action.setShortcut("F1")
    help_action.setStatusTip("Show keyboard shortcuts and usage help")
    help_action.triggered.connect(window.show_help)
    help_menu.addAction(help_action)
