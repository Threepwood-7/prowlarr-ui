#!/usr/bin/env python3
"""
Prowlarr Search Client - PySide6 GUI Application
Searches Prowlarr indexers and integrates with Everything for duplicate detection
"""

import math
import os
import subprocess
import sys
import logging
import webbrowser
from datetime import datetime
from typing import Optional, List, Dict
from urllib.parse import quote

from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLineEdit,
    QPushButton,
    QTreeView,
    QTableWidget,
    QTableWidgetItem,
    QSplitter,
    QStatusBar,
    QHeaderView,
    QCompleter,
    QAbstractItemView,
    QLabel,
    QSpinBox,
    QProgressBar,
    QCheckBox,
    QGroupBox,
    QMenu,
    QDialog,
    QTextBrowser,
    QDialogButtonBox,
)
from PySide6.QtCore import Qt, QTimer, QThread, Signal, QItemSelection, QItemSelectionModel
from PySide6.QtGui import QStandardItemModel, QStandardItem, QColor, QAction, QPixmap, QPainter, QPen, QIcon, QShortcut, QKeySequence

# Import from modular structure
from src.utils.logging_config import setup_logging
from src.utils.config import load_config, save_config, ensure_config_exists, validate_config
from src.utils.formatters import format_size, format_age
from src.api.prowlarr_client import ProwlarrClient
from src.api.everything_search import EverythingSearch
from src.workers.search_worker import SearchWorker
from src.workers.everything_worker import EverythingCheckWorker
from src.workers.download_worker import DownloadWorker
from src.ui.widgets import NumericTableWidgetItem
from src.ui.log_window import LogWindow
from src.ui.help_text import HELP_HTML
from src.utils.quality_parser import parse_quality

logger = logging.getLogger(__name__)

# Anchor download history to script directory so it doesn't depend on CWD
DOWNLOAD_HISTORY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "download_history.log")

import functools
import traceback


def safe_slot(func):
    """Decorator to catch and log exceptions in Qt signal handlers.
    PySide6 silently swallows exceptions in slots, so this ensures
    they are logged to both the logger and the log window."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception:
            tb = traceback.format_exc()
            logger.error(f"Exception in {func.__name__}:\n{tb}")
            if hasattr(self, "log"):
                self.log(f"ERROR in {func.__name__}: {tb}")

    return wrapper


class InitWorker(QThread):
    """Background worker to initialize Everything and load Prowlarr indexers"""

    init_done = Signal(object, list, str)  # (everything_instance, indexers, error)

    def __init__(self, everything_method, prowlarr, everything_sdk_url=""):
        super().__init__()
        self.everything_method = everything_method
        self.prowlarr = prowlarr
        self.everything_sdk_url = everything_sdk_url

    def run(self):
        everything = None
        indexers = []
        error = ""
        if self.isInterruptionRequested():
            logger.info("InitWorker interrupted before initialization")
            return
        try:
            kwargs = {}
            if self.everything_sdk_url:
                kwargs["sdk_url"] = self.everything_sdk_url
            everything = EverythingSearch(self.everything_method, **kwargs)
        except Exception as e:
            logger.error(f"Failed to initialize Everything: {e}")
        if self.isInterruptionRequested():
            logger.info("InitWorker interrupted before indexer load")
            return
        try:
            if self.prowlarr:
                indexers = self.prowlarr.get_indexers()
        except Exception as e:
            error = f"Failed to load indexers: {e}"
            logger.error(error)
        if self.isInterruptionRequested():
            logger.info("InitWorker interrupted before completion emit")
            return
        self.init_done.emit(everything, indexers, error)


class MainWindow(QMainWindow):
    """
    Main application window
    Contains search controls, results grid, and status bar
    """

    # Column index constants
    COL_AGE = 0
    COL_TITLE = 1
    COL_QUALITY = 2
    COL_SIZE = 3
    COL_SEEDERS = 4
    COL_LEECHERS = 5
    COL_GRABS = 6
    COL_INDEXER = 7
    COL_DOWNLOAD = 8
    COL_COUNT = 9
    COL_HEADERS = ["Age", "Title", "Quality", "Size", "Seeders", "Leechers", "Grabs", "Indexer", "Download"]

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Prowlarr Search Client")
        self.setGeometry(100, 100, 1400, 800)
        self.setWindowIcon(self._create_globe_icon())

        # Load and validate configuration from TOML file
        self.config = load_config()
        config_warnings = validate_config(self.config)
        for w in config_warnings:
            logger.warning(f"Config: {w}")

        # Get configurable settings
        settings = self.config.get("settings", {})
        self.title_match_chars = settings.get("title_match_chars", 42)
        self.everything_search_chars = settings.get("everything_search_chars", 42)
        self.everything_recheck_delay = settings.get("everything_recheck_delay", 6000)  # Delay in ms before rechecking after download
        self.web_search_url = settings.get("web_search_url", "https://www.google.com/search?q={query}")
        self.everything_integration_method = settings.get("everything_integration_method", "sdk")
        self.prowlarr_page_size = settings.get("prowlarr_page_size", 100)
        self.everything_max_results = settings.get("everything_max_results", 5)
        self.custom_commands = {
            Qt.Key_F2: settings.get("custom_command_F2", ""),
            Qt.Key_F3: settings.get("custom_command_F3", ""),
            Qt.Key_F4: settings.get("custom_command_F4", ""),
        }
        self.everything_batch_size = settings.get("everything_batch_size", 10)

        # Initialize Prowlarr API client (lightweight - just stores config)
        try:
            prowlarr_config = self.config.get("prowlarr", {})
            self.prowlarr = ProwlarrClient(
                prowlarr_config.get("host", "http://localhost:9696"),
                prowlarr_config.get("api_key", ""),
                timeout=settings.get("api_timeout", 30),
                retries=settings.get("api_retries", 2),
                http_basic_auth_username=prowlarr_config.get("http_basic_auth_username", ""),
                http_basic_auth_password=prowlarr_config.get("http_basic_auth_password", ""),
            )
            logger.info("Prowlarr client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Prowlarr client: {e}")
            self.prowlarr = None

        # Defer Everything search integration to background
        self.everything = None

        # Load search history from config
        self.search_history = self.config.get("preferences", {}).get("search_history", [])

        # Current search state
        self.current_worker = None
        self.everything_check_worker = None
        self.download_worker = None
        self.current_results = []
        self.current_offset = 0
        self._video_paths = {}  # title -> video file path from Everything
        self._search_generation = 0  # Incremented on each new search, used to invalidate stale timers
        self._everything_check_generation = 0  # Generation when Everything check started
        self._pending_everything_check_generation = None  # Deferred generation to run after an in-flight check ends
        # Composite key avoids collisions when two indexers expose the same GUID.
        self._downloaded_release_keys = set()  # {(guid, indexer_id), ...}
        self._downloaded_title_keys = set()  # Title keys for Everything recheck scheduling
        self._release_key_to_row = {}  # (guid, indexer_id) -> row cache for download progress tracking

        # Multi-page fetch state
        self._load_all_active = False
        self._load_all_results = []
        self._load_all_page = 0

        # Worker tracking for proper cleanup
        self._all_workers = []  # Track all workers for forced cleanup on close

        # Timer tracking for proper cleanup
        self._pending_timers = []  # Track all QTimer.singleShot timers

        # Splitter save timer for debouncing
        self.splitter_save_timer = QTimer()
        self.splitter_save_timer.setSingleShot(True)
        self.splitter_save_timer.timeout.connect(self.save_splitter_sizes)

        # Debounced config save timer to avoid blocking UI on frequent preference changes.
        self._config_dirty = False
        self.config_save_timer = QTimer()
        self.config_save_timer.setSingleShot(True)
        self.config_save_timer.timeout.connect(self._flush_config_save)

        # Create log window (hidden by default)
        self.log_window = LogWindow(self)

        # Build UI components
        self.setup_ui()
        self.setup_menu()

        # Show config warnings in log window
        for w in config_warnings:
            self.log(f"WARNING: {w}")

        # Initialize heavy components in background thread
        self.init_worker = InitWorker(self.everything_integration_method, self.prowlarr, settings.get("everything_sdk_url", ""))
        self.init_worker.init_done.connect(self._on_init_done)
        self.init_worker.start()

    @safe_slot
    def on_splitter_moved(self, pos: int, index: int):
        """Handle splitter moved - debounce and save sizes after user stops moving"""
        # Restart timer - will only save 500ms after user stops moving
        self.splitter_save_timer.start(500)

    @safe_slot
    def save_splitter_sizes(self):
        """Save splitter sizes to config"""
        sizes = self.splitter.sizes()
        if "preferences" not in self.config:
            self.config["preferences"] = {}
        self.config["preferences"]["splitter_sizes"] = sizes
        self._schedule_config_save()
        logger.info(f"Splitter sizes saved: {sizes}")

    @safe_slot
    def on_hide_existing_toggled(self, checked: bool):
        """Handle Hide existing checkbox toggle - save preference and apply filter"""
        if "preferences" not in self.config:
            self.config["preferences"] = {}
        self.config["preferences"]["hide_existing"] = checked
        self._schedule_config_save()
        self.apply_hide_existing_filter()

    def apply_hide_existing_filter(self):
        """Convenience wrapper - apply all filters including hide-existing"""
        self.apply_result_filters()

    @safe_slot
    def apply_result_filters(self):
        """Apply all row filters: hide-existing, title text, min size, max age"""
        hide_existing = self.hide_existing_checkbox.isChecked()
        title_filter = self.filter_title_input.text().strip().lower() if hasattr(self, "filter_title_input") else ""
        min_size_mb = self.filter_min_size.value() if hasattr(self, "filter_min_size") else 0
        max_age_days = self.filter_max_age.value() if hasattr(self, "filter_max_age") else 0

        min_size_bytes = min_size_mb * 1024 * 1024

        for row in range(self.results_table.rowCount()):
            hidden = False

            title_item = self.results_table.item(row, self.COL_TITLE)
            # Hide existing (Everything match)
            if hide_existing and title_item and title_item.toolTip().startswith("Found in Everything"):
                hidden = True

            # Title text filter
            if not hidden and title_filter and title_item:
                if title_filter not in title_item.text().lower():
                    hidden = True

            # Min size filter
            if not hidden and min_size_bytes > 0:
                size_item = self.results_table.item(row, self.COL_SIZE)
                if size_item:
                    size_val = size_item.data(Qt.UserRole)
                    if size_val is not None and size_val < min_size_bytes:
                        hidden = True

            # Max age filter
            if not hidden and max_age_days > 0:
                age_item = self.results_table.item(row, self.COL_AGE)
                if age_item:
                    age_val = age_item.data(Qt.UserRole)
                    if age_val is not None and age_val > max_age_days:
                        hidden = True

            self.results_table.setRowHidden(row, hidden)

        self.update_download_button_states()
        self._update_status_bar_counts()

    @safe_slot
    def clear_result_filters(self):
        """Reset all filter controls to defaults"""
        self.filter_title_input.clear()
        self.filter_min_size.setValue(0)
        self.filter_max_age.setValue(0)

    @safe_slot
    def on_prowlarr_page_size_changed(self, value: int):
        """Handle max page size spinbox value change"""
        self.prowlarr_page_size = value
        # Update config and save via debounce
        if "settings" not in self.config:
            self.config["settings"] = {}
        self.config["settings"]["prowlarr_page_size"] = value
        self._schedule_config_save()
        logger.info(f"Max page size updated to {value}")

    @safe_slot
    def on_prowlarr_page_number_changed(self, value: int):
        """Handle page number change - fetch specific page"""
        # Only fetch if we have a current query and this isn't the initial setup
        if hasattr(self, "query_input") and self.query_input.text().strip():
            # Don't trigger if we're already in a search operation
            if not self.current_worker:
                self.fetch_page(value)

    def setup_ui(self):
        """Build the main UI layout"""
        # Create central widget with proper hierarchy
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Main layout without margins to avoid background text issues
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        central_widget.setLayout(main_layout)

        # Activity bar at top of window (thin progress bar)
        self.activity_bar = QProgressBar()
        self.activity_bar.setFixedHeight(4)
        self.activity_bar.setTextVisible(False)
        self.activity_bar.setRange(0, 1)
        self.activity_bar.setValue(1)  # full = idle
        main_layout.addWidget(self.activity_bar)

        # Horizontal splitter for left panel and center panel
        self.splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(self.splitter)

        # Create left and center panels
        left_panel = self.create_left_panel()
        center_panel = self.create_center_panel()

        self.splitter.addWidget(left_panel)
        self.splitter.addWidget(center_panel)

        # Load saved splitter sizes or use defaults
        saved_sizes = self.config.get("preferences", {}).get("splitter_sizes", [300, 1100])
        self.splitter.setSizes(saved_sizes)

        # Connect to save splitter position when moved
        self.splitter.splitterMoved.connect(self.on_splitter_moved)

        # Status bar at bottom
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)

        # Status label for messages (stretch=1 to fill available space)
        self.status_label = QLabel("Loading...")
        self.status_bar.addWidget(self.status_label, 1)

    def create_left_panel(self) -> QWidget:
        """Create left control panel with search controls"""
        panel = QWidget()
        layout = QVBoxLayout()
        layout.setContentsMargins(2, 2, 2, 2)
        layout.setSpacing(4)
        panel.setLayout(layout)

        # === Search group ===
        search_group = QGroupBox("Search")
        search_layout = QVBoxLayout()
        search_layout.setContentsMargins(6, 6, 6, 6)
        search_layout.setSpacing(4)
        search_group.setLayout(search_layout)

        # Search query input with autocomplete
        search_label = QLabel("Search &Query:")
        search_layout.addWidget(search_label)
        self.query_input = QLineEdit()
        search_label.setBuddy(self.query_input)
        self.query_input.setPlaceholderText("Enter search query...")
        self.query_input.returnPressed.connect(self._on_search_return_pressed)

        # Setup autocomplete from history
        self.completer = QCompleter(self.search_history)
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.query_input.setCompleter(self.completer)

        # Show suggestions when clicking in the search box
        def show_completer_on_focus(event):
            QLineEdit.mousePressEvent(self.query_input, event)
            if not self.query_input.text():
                self.completer.complete()

        self.query_input.mousePressEvent = show_completer_on_focus

        # Show history on Down arrow in empty search box
        original_key_press = self.query_input.keyPressEvent

        def search_key_press(event):
            if event.key() == Qt.Key_Down and not self.query_input.text():
                self.completer.setCompletionPrefix("")
                self.completer.complete()
                return
            original_key_press(event)

        self.query_input.keyPressEvent = search_key_press

        search_layout.addWidget(self.query_input)
        self.query_input.setToolTip("Enter a search term and press Enter to search Prowlarr indexers\nPress Down or Enter when empty to browse search history")

        # Search buttons
        button_layout = QHBoxLayout()
        button_layout.setSpacing(4)

        self.search_btn = QPushButton("&Search")
        self.search_btn.clicked.connect(self.start_search)
        self.search_btn.setToolTip("Search Prowlarr with the current query and filters")
        button_layout.addWidget(self.search_btn)

        self.load_all_btn = QPushButton("Load A&ll")
        self.load_all_btn.clicked.connect(self.start_load_all_pages)
        self.load_all_btn.setToolTip("Fetch all pages of results sequentially")
        button_layout.addWidget(self.load_all_btn)

        search_layout.addLayout(button_layout)
        layout.addWidget(search_group)

        # === Pagination group ===
        pagination_group = QGroupBox("Pagination")
        pagination_layout = QGridLayout()
        pagination_layout.setContentsMargins(6, 6, 6, 6)
        pagination_layout.setSpacing(4)
        pagination_group.setLayout(pagination_layout)

        # Max page size spinbox (left column)
        page_size_label = QLabel("&Max Page Size:")
        pagination_layout.addWidget(page_size_label, 0, 0)
        self.prowlarr_page_size_spinbox = QSpinBox()
        page_size_label.setBuddy(self.prowlarr_page_size_spinbox)
        self.prowlarr_page_size_spinbox.setMinimum(10)
        self.prowlarr_page_size_spinbox.setMaximum(10000)
        self.prowlarr_page_size_spinbox.setSingleStep(100)
        self.prowlarr_page_size_spinbox.setValue(self.prowlarr_page_size)
        self.prowlarr_page_size_spinbox.valueChanged.connect(self.on_prowlarr_page_size_changed)
        self.prowlarr_page_size_spinbox.setToolTip("Maximum number of results to fetch per page from Prowlarr")
        pagination_layout.addWidget(self.prowlarr_page_size_spinbox, 1, 0)

        # Page number spinbox (right column)
        page_num_label = QLabel("Page &Number:")
        pagination_layout.addWidget(page_num_label, 0, 1)
        self.prowlarr_page_number_spinbox = QSpinBox()
        page_num_label.setBuddy(self.prowlarr_page_number_spinbox)
        self.prowlarr_page_number_spinbox.setMinimum(1)
        self.prowlarr_page_number_spinbox.setMaximum(300)
        self.prowlarr_page_number_spinbox.setSingleStep(1)
        self.prowlarr_page_number_spinbox.setValue(1)
        self.prowlarr_page_number_spinbox.valueChanged.connect(self.on_prowlarr_page_number_changed)
        self.prowlarr_page_number_spinbox.setToolTip("Page number for paginated results (search again to apply)")
        pagination_layout.addWidget(self.prowlarr_page_number_spinbox, 1, 1)

        layout.addWidget(pagination_group)

        # === Filters group ===
        filters_group = QGroupBox("Filters")
        filters_layout = QVBoxLayout()
        filters_layout.setContentsMargins(6, 6, 6, 6)
        filters_layout.setSpacing(4)
        filters_group.setLayout(filters_layout)

        # Indexers tree view with checkboxes
        indexers_label = QLabel("&Indexers:")
        filters_layout.addWidget(indexers_label)
        self.indexers_tree = QTreeView()
        indexers_label.setBuddy(self.indexers_tree)
        self.indexers_model = QStandardItemModel()
        self.indexers_tree.setModel(self.indexers_model)
        self.indexers_tree.setHeaderHidden(True)
        self.indexers_tree.setToolTip("Select which Prowlarr indexers to search\nUse 'All' to toggle all at once")
        filters_layout.addWidget(self.indexers_tree, 1)  # stretch=1

        # Categories tree view with checkboxes
        categories_label = QLabel("Ca&tegories:")
        filters_layout.addWidget(categories_label)
        self.categories_tree = QTreeView()
        categories_label.setBuddy(self.categories_tree)
        self.categories_model = QStandardItemModel()
        self.categories_tree.setModel(self.categories_model)
        self.categories_tree.setHeaderHidden(True)
        self.categories_tree.setToolTip("Filter results by category (Movies, TV, Audio, etc.)\nUse 'All' to toggle all at once")
        filters_layout.addWidget(self.categories_tree, 2)  # stretch=2

        # Hide existing checkbox
        self.hide_existing_checkbox = QCheckBox("Hide e&xisting")
        saved_hide = self.config.get("preferences", {}).get("hide_existing", False)
        self.hide_existing_checkbox.setChecked(saved_hide)
        self.hide_existing_checkbox.toggled.connect(self.on_hide_existing_toggled)
        self.hide_existing_checkbox.setToolTip("Hide results that already exist on disk (detected via Everything)")
        filters_layout.addWidget(self.hide_existing_checkbox)

        layout.addWidget(filters_group, 1)  # filters group takes remaining space

        # === Download section (ungrouped, at bottom) ===
        download_layout = QHBoxLayout()
        download_layout.setSpacing(4)

        self.download_selected_btn = QPushButton("&Download Selected")
        self.download_selected_btn.clicked.connect(self.download_selected)
        self.download_selected_btn.setEnabled(False)
        self.download_selected_btn.setToolTip("Download highlighted rows (Ctrl+Click to multi-select)")
        download_layout.addWidget(self.download_selected_btn)

        self.download_all_btn = QPushButton("Download &All")
        self.download_all_btn.clicked.connect(self.download_all)
        self.download_all_btn.setEnabled(False)
        self.download_all_btn.setToolTip("Download all visible (non-hidden) results")
        download_layout.addWidget(self.download_all_btn)

        layout.addLayout(download_layout)

        # Download progress bar (always visible, resets to 0 when idle)
        self.download_progress = QProgressBar()
        self.download_progress.setTextVisible(True)
        self.download_progress.setFormat("%v/%m")
        self.download_progress.setMaximum(1)
        self.download_progress.setValue(0)
        layout.addWidget(self.download_progress)

        return panel

    def create_center_panel(self) -> QWidget:
        """Create center results panel with table and filter bar"""
        panel = QWidget()
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)  # Remove margins
        panel.setLayout(layout)

        # Filter bar
        filter_layout = QHBoxLayout()
        filter_layout.setContentsMargins(4, 2, 4, 2)

        filter_label = QLabel("Filte&r:")
        filter_layout.addWidget(filter_label)
        self.filter_title_input = QLineEdit()
        filter_label.setBuddy(self.filter_title_input)
        self.filter_title_input.setPlaceholderText("Title contains...")
        self.filter_title_input.setToolTip("Filter results by title (case-insensitive, Alt+R)")
        self.filter_title_input.textChanged.connect(self.apply_result_filters)
        filter_layout.addWidget(self.filter_title_input, 1)

        filter_layout.addWidget(QLabel("Min Size:"))
        self.filter_min_size = QSpinBox()
        self.filter_min_size.setRange(0, 999999)
        self.filter_min_size.setSuffix(" MB")
        self.filter_min_size.setToolTip("Minimum file size in MB (0 = no minimum)")
        self.filter_min_size.valueChanged.connect(self.apply_result_filters)
        filter_layout.addWidget(self.filter_min_size)

        filter_layout.addWidget(QLabel("Max Age:"))
        self.filter_max_age = QSpinBox()
        self.filter_max_age.setRange(0, 99999)
        self.filter_max_age.setSuffix(" days")
        self.filter_max_age.setToolTip("Maximum age in days (0 = no limit)")
        self.filter_max_age.valueChanged.connect(self.apply_result_filters)
        filter_layout.addWidget(self.filter_max_age)

        clear_filter_btn = QPushButton("Clear")
        clear_filter_btn.setToolTip("Clear all filters")
        clear_filter_btn.clicked.connect(self.clear_result_filters)
        filter_layout.addWidget(clear_filter_btn)

        layout.addLayout(filter_layout)

        # Results table
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(self.COL_COUNT)
        self.results_table.setHorizontalHeaderLabels(self.COL_HEADERS)

        # Configure table headers
        header = self.results_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(self.COL_TITLE, QHeaderView.Stretch)  # Title column stretches

        # Configure table behavior
        self.results_table.setAlternatingRowColors(False)  # We'll handle colors manually
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.results_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.setSortingEnabled(True)

        # Header right-click to show/hide columns
        header.setContextMenuPolicy(Qt.CustomContextMenu)
        header.customContextMenuRequested.connect(self._show_header_context_menu)

        # Restore hidden columns from preferences
        hidden_cols = self.config.get("preferences", {}).get("hidden_columns", [])
        for col_name in hidden_cols:
            if col_name in self.COL_HEADERS:
                col_idx = self.COL_HEADERS.index(col_name)
                if col_idx != self.COL_TITLE:  # Never hide Title
                    self.results_table.setColumnHidden(col_idx, True)

        # Connect to sort change signal to re-apply coloring
        header.sectionClicked.connect(self.on_sort_changed)

        # Connect selection change to update Download Selected button state
        self.results_table.itemSelectionChanged.connect(self.update_download_button_states)

        # Override keyboard handler for shortcuts
        self.results_table.keyPressEvent = self.table_key_press

        # Right-click context menu
        self.results_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.results_table.customContextMenuRequested.connect(self._show_context_menu)

        # Double-click to download
        self.results_table.cellDoubleClicked.connect(self._on_cell_double_clicked)

        layout.addWidget(self.results_table)

        # Find bar (hidden by default, shown with Ctrl+F)
        self.find_bar = QWidget()
        find_layout = QHBoxLayout()
        find_layout.setContentsMargins(4, 2, 4, 2)
        find_layout.addWidget(QLabel("Find:"))
        self.find_input = QLineEdit()
        self.find_input.setPlaceholderText("Search in titles... (Enter=next, Shift+Enter=prev, Esc=close)")
        self.find_input.returnPressed.connect(self._find_next)
        find_layout.addWidget(self.find_input, 1)
        find_prev_btn = QPushButton("<")
        find_prev_btn.setFixedWidth(30)
        find_prev_btn.setToolTip("Find previous (Shift+Enter)")
        find_prev_btn.clicked.connect(self._find_prev)
        find_layout.addWidget(find_prev_btn)
        find_next_btn = QPushButton(">")
        find_next_btn.setFixedWidth(30)
        find_next_btn.setToolTip("Find next (Enter)")
        find_next_btn.clicked.connect(self._find_next)
        find_layout.addWidget(find_next_btn)
        find_close_btn = QPushButton("X")
        find_close_btn.setFixedWidth(30)
        find_close_btn.setToolTip("Close find bar (Esc)")
        find_close_btn.clicked.connect(self._close_find_bar)
        find_layout.addWidget(find_close_btn)
        self.find_bar.setLayout(find_layout)
        self.find_bar.setVisible(False)
        layout.addWidget(self.find_bar)

        # Ctrl+F shortcut
        find_shortcut = QShortcut(QKeySequence("Ctrl+F"), self)
        find_shortcut.activated.connect(self._toggle_find_bar)

        # Esc to close find bar when focused
        self.find_input.installEventFilter(self)

        return panel

    @staticmethod
    def _create_globe_icon() -> QIcon:
        """Draw a simple globe icon (circle + meridians + parallels)"""
        size = 64
        pix = QPixmap(size, size)
        pix.fill(Qt.transparent)
        p = QPainter(pix)
        p.setRenderHint(QPainter.Antialiasing)
        pen = QPen(QColor(60, 130, 200), 2)
        p.setPen(pen)
        p.setBrush(QColor(180, 220, 255))
        m = 3  # margin
        p.drawEllipse(m, m, size - 2 * m, size - 2 * m)
        # Meridians (vertical ellipses)
        p.setBrush(Qt.NoBrush)
        cx, cy, r = size // 2, size // 2, size // 2 - m
        for offset in (-r // 3, 0, r // 3):
            w = abs(r - abs(offset))
            p.drawEllipse(cx + offset - w // 2, m, w, size - 2 * m)
        # Parallels (horizontal lines as arcs)
        for dy in (-r // 3, 0, r // 3):
            half_w = int(math.sqrt(max(0, r * r - dy * dy)))
            p.drawLine(cx - half_w, cy + dy, cx + half_w, cy + dy)
        p.end()
        return QIcon(pix)

    def setup_menu(self):
        """Create menu bar"""
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu("&File")
        exit_action = QAction("E&xit", self)
        exit_action.setStatusTip("Close the application")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # View menu
        view_menu = menubar.addMenu("&View")
        log_action = QAction("Show &Log", self)
        log_action.setStatusTip("Open the log window to view application messages")
        log_action.triggered.connect(self.toggle_log_window)
        view_menu.addAction(log_action)

        history_action = QAction("Download &History", self)
        history_action.setStatusTip("View the log of previously downloaded items")
        history_action.triggered.connect(self._open_download_history)
        view_menu.addAction(history_action)

        view_menu.addSeparator()

        best_per_group_action = QAction("Select &Best per Group", self)
        best_per_group_action.setStatusTip("Highlight the best result in each title group based on size and seeders")
        best_per_group_action.triggered.connect(self.select_best_per_group)
        view_menu.addAction(best_per_group_action)

        reset_sort_action = QAction("&Reset Sorting", self)
        reset_sort_action.setStatusTip("Restore default sort order: Title ASC, Indexer DESC, Age ASC")
        reset_sort_action.triggered.connect(self.apply_default_sort)
        view_menu.addAction(reset_sort_action)

        reset_view_action = QAction("Reset &View", self)
        reset_view_action.setStatusTip("Reset column widths, splitter position, and sort order to defaults")
        reset_view_action.triggered.connect(self._reset_view)
        view_menu.addAction(reset_view_action)

        # Bookmarks menu
        self.bookmarks_menu = menubar.addMenu("&Bookmarks")
        add_bm_action = QAction("&Add Bookmark", self)
        add_bm_action.setStatusTip("Save the current search query as a bookmark")
        add_bm_action.triggered.connect(self._add_bookmark)
        self.bookmarks_menu.addAction(add_bm_action)

        remove_bm_action = QAction("&Delete Bookmark", self)
        remove_bm_action.setStatusTip("Remove a saved bookmark from the list")
        remove_bm_action.triggered.connect(self._remove_bookmark)
        self.bookmarks_menu.addAction(remove_bm_action)

        sort_bm_action = QAction("&Sort Bookmarks", self)
        sort_bm_action.setStatusTip("Sort all bookmarks alphabetically")
        sort_bm_action.triggered.connect(self._sort_bookmarks)
        self.bookmarks_menu.addAction(sort_bm_action)

        self.bookmarks_separator = self.bookmarks_menu.addSeparator()

        # Load saved bookmarks into menu
        self._bookmarks = self.config.get("preferences", {}).get("bookmarks", [])
        for bm in self._bookmarks:
            self._add_bookmark_action(bm)

        # Help menu
        help_menu = menubar.addMenu("&Help")
        help_action = QAction("&Help", self)
        help_action.setShortcut("F1")
        help_action.setStatusTip("Show keyboard shortcuts and usage help")
        help_action.triggered.connect(self.show_help)
        help_menu.addAction(help_action)

    @safe_slot
    def show_help(self):
        """Show scrollable help dialog"""
        dlg = QDialog(self)
        dlg.setWindowTitle("Help")
        dlg.resize(520, 480)
        layout = QVBoxLayout(dlg)
        browser = QTextBrowser(dlg)
        browser.setOpenExternalLinks(False)
        browser.setHtml(HELP_HTML)
        layout.addWidget(browser)
        btn_box = QDialogButtonBox(QDialogButtonBox.Ok)
        btn_box.accepted.connect(dlg.accept)
        layout.addWidget(btn_box)
        dlg.exec()

    @safe_slot
    def _on_init_done(self, everything, indexers, error):
        """Handle background init completion - update UI on main thread"""
        self.everything = everything
        if self.everything:
            if self.everything_integration_method == "none":
                self.log("Everything integration disabled")
            else:
                self.log(f"Everything search integration initialized (method: {self.everything_integration_method})")

        if error:
            self.log(error)
            self.status_label.setText(error)
            return

        if not self.prowlarr:
            self.status_label.setText("Prowlarr not configured")
            return

        if indexers:
            self.populate_indexers(indexers)
            categories = self.prowlarr.get_categories()
            self.populate_categories(categories)
            self.log(f"Loaded {len(indexers)} indexers and {len(categories)} categories")
            self.status_label.setText(f"Ready - {len(indexers)} indexers loaded")
        else:
            self.status_label.setText("No indexers found")

    def populate_indexers(self, indexers: List[Dict]):
        """Populate indexers tree with checkboxes"""
        self.indexers_model.clear()

        # Root "All" item
        root = QStandardItem("All")
        root.setCheckable(True)
        root.setCheckState(Qt.Checked)
        self.indexers_model.appendRow(root)

        # Load saved indexer selection from config (None = no saved preference)
        saved_indexers = self.config.get("preferences", {}).get("selected_indexers", None)

        # Add each enabled indexer as child
        for indexer in indexers:
            if indexer.get("enable", False):
                item = QStandardItem(indexer.get("name", "Unknown"))
                item.setCheckable(True)
                item.setData(indexer.get("id"), Qt.UserRole)  # Store ID in user data

                # Restore saved state or default to checked on first run
                if saved_indexers is not None:
                    if indexer.get("id") in saved_indexers:
                        item.setCheckState(Qt.Checked)
                    else:
                        item.setCheckState(Qt.Unchecked)
                else:
                    item.setCheckState(Qt.Checked)

                root.appendRow(item)

        self.indexers_tree.expandAll()

        # Connect change handler (disconnect first to avoid duplicate connections)
        try:
            self.indexers_model.itemChanged.disconnect(self.indexer_item_changed)
        except RuntimeError:
            pass  # Not connected yet
        self.indexers_model.itemChanged.connect(self.indexer_item_changed)

    def populate_categories(self, categories: List[Dict]):
        """Populate categories tree with checkboxes"""
        self.categories_model.clear()

        # Root "All" item
        root = QStandardItem("All")
        root.setCheckable(True)
        root.setCheckState(Qt.Checked)
        self.categories_model.appendRow(root)

        # Load saved category selection from config (None = no saved preference)
        saved_categories = self.config.get("preferences", {}).get("selected_categories", None)

        # Add each category with code in brackets
        for category in categories:
            item = QStandardItem(f"{category.get('name', 'Unknown')} [{category.get('id')}]")
            item.setCheckable(True)
            item.setData(category.get("id"), Qt.UserRole)  # Store ID in user data

            # Restore saved state or default to checked on first run
            if saved_categories is not None:
                if category.get("id") in saved_categories:
                    item.setCheckState(Qt.Checked)
                else:
                    item.setCheckState(Qt.Unchecked)
            else:
                item.setCheckState(Qt.Checked)

            root.appendRow(item)

        self.categories_tree.expandAll()

        # Connect change handler (disconnect first to avoid duplicate connections)
        try:
            self.categories_model.itemChanged.disconnect(self.category_item_changed)
        except RuntimeError:
            pass  # Not connected yet
        self.categories_model.itemChanged.connect(self.category_item_changed)

    @safe_slot
    def indexer_item_changed(self, item: QStandardItem):
        """Handle indexer checkbox changes - sync parent and children"""
        root = self.indexers_model.item(0)
        if not root:
            return
        if item == root:
            self.indexers_model.blockSignals(True)
            for i in range(root.rowCount()):
                root.child(i).setCheckState(root.checkState())
            self.indexers_model.blockSignals(False)
        elif item.parent() == root:
            self.indexers_model.blockSignals(True)
            all_checked = all(root.child(i).checkState() == Qt.Checked for i in range(root.rowCount()))
            any_checked = any(root.child(i).checkState() == Qt.Checked for i in range(root.rowCount()))
            root.setCheckState(Qt.Checked if all_checked else (Qt.PartiallyChecked if any_checked else Qt.Unchecked))
            self.indexers_model.blockSignals(False)

    @safe_slot
    def category_item_changed(self, item: QStandardItem):
        """Handle category checkbox changes - sync parent and children"""
        root = self.categories_model.item(0)
        if not root:
            return
        if item == root:
            self.categories_model.blockSignals(True)
            for i in range(root.rowCount()):
                child = root.child(i)
                child.setCheckState(root.checkState())
                # Also update subcategories
                for j in range(child.rowCount()):
                    child.child(j).setCheckState(root.checkState())
            self.categories_model.blockSignals(False)
        else:
            self.categories_model.blockSignals(True)
            # Update parent "All" based on children states
            all_checked = all(root.child(i).checkState() == Qt.Checked for i in range(root.rowCount()))
            any_checked = any(root.child(i).checkState() != Qt.Unchecked for i in range(root.rowCount()))
            root.setCheckState(Qt.Checked if all_checked else (Qt.PartiallyChecked if any_checked else Qt.Unchecked))
            self.categories_model.blockSignals(False)

    def get_selected_indexers(self) -> List[int]:
        """
        Get list of selected indexer IDs
        Returns empty list if all are selected (don't pass to API)
        """
        root = self.indexers_model.item(0)
        if not root:
            return []

        # Check if all children are checked
        if root.checkState() == Qt.Checked:
            all_checked = True
            for i in range(root.rowCount()):
                if root.child(i).checkState() != Qt.Checked:
                    all_checked = False
                    break
            if all_checked:
                return []  # All selected - return empty to search all

        # Return only selected indexers
        selected = []
        for i in range(root.rowCount()):
            child = root.child(i)
            if child.checkState() == Qt.Checked:
                indexer_id = child.data(Qt.UserRole)
                if indexer_id:
                    selected.append(indexer_id)

        return selected

    def get_selected_categories(self) -> List[int]:
        """
        Get list of selected category IDs
        Returns empty list if all are selected (don't pass to API)
        """
        root = self.categories_model.item(0)
        if not root:
            return []

        # Check if all children are checked
        if root.checkState() == Qt.Checked:
            all_checked = True
            for i in range(root.rowCount()):
                if root.child(i).checkState() != Qt.Checked:
                    all_checked = False
                    break
            if all_checked:
                return []  # All selected - return empty to search all

        # Return only selected categories
        selected = []
        for i in range(root.rowCount()):
            child = root.child(i)
            if child.checkState() == Qt.Checked:
                category_id = child.data(Qt.UserRole)
                if category_id:
                    selected.append(category_id)

        return selected

    def _get_checked_indexer_ids(self) -> List[int]:
        """Get explicit list of checked indexer IDs for saving preferences"""
        root = self.indexers_model.item(0)
        if not root:
            return []
        ids = []
        for i in range(root.rowCount()):
            child = root.child(i)
            if child.checkState() == Qt.Checked:
                indexer_id = child.data(Qt.UserRole)
                if indexer_id:
                    ids.append(indexer_id)
        return ids

    def _get_checked_category_ids(self) -> List[int]:
        """Get explicit list of checked category IDs for saving preferences"""
        root = self.categories_model.item(0)
        if not root:
            return []
        ids = []
        for i in range(root.rowCount()):
            child = root.child(i)
            if child.checkState() == Qt.Checked:
                cat_id = child.data(Qt.UserRole)
                if cat_id:
                    ids.append(cat_id)
        return ids

    @safe_slot
    def _add_bookmark(self):
        """Bookmark the current search query"""
        query = self.query_input.text().strip()
        if not query:
            self.status_label.setText("Enter a query to bookmark")
            return
        if query in self._bookmarks:
            self.status_label.setText("Query already bookmarked")
            return
        self._bookmarks.append(query)
        self._add_bookmark_action(query)
        self._save_bookmarks()
        self.status_label.setText(f"Bookmarked: {query}")

    def _add_bookmark_action(self, query: str):
        """Add a bookmark entry to the Bookmarks menu"""
        action = QAction(query, self)
        action.setStatusTip(f"Search for \"{query}\"")
        action.triggered.connect(lambda checked, q=query: self._search_bookmark(q))
        self.bookmarks_menu.addAction(action)

    @safe_slot
    def _remove_bookmark(self):
        """Remove the current search query from bookmarks"""
        query = self.query_input.text().strip()
        if not query:
            self.status_label.setText("Enter the query to remove from bookmarks")
            return
        if query not in self._bookmarks:
            self.status_label.setText(f"'{query}' is not bookmarked")
            return
        self._bookmarks.remove(query)
        # Remove the matching action from the menu
        for action in self.bookmarks_menu.actions():
            if action.text() == query:
                self.bookmarks_menu.removeAction(action)
                break
        self._save_bookmarks()
        self.status_label.setText(f"Bookmark removed: {query}")

    def _search_bookmark(self, query: str):
        """Search for a bookmarked query"""
        try:
            self.query_input.setText(query)
            self.start_search()
        except Exception as e:
            logger.error(f"Error in _search_bookmark: {e}")

    @safe_slot
    def _sort_bookmarks(self):
        """Sort bookmarks alphabetically and rebuild menu"""
        if not self._bookmarks:
            return
        self._bookmarks.sort(key=str.lower)
        self._rebuild_bookmarks_menu()
        self._save_bookmarks()
        self.status_label.setText("Bookmarks sorted")

    def _rebuild_bookmarks_menu(self):
        """Remove all bookmark actions after the separator and re-add them"""
        # Remove existing bookmark entries (everything after the separator)
        actions = self.bookmarks_menu.actions()
        sep_index = actions.index(self.bookmarks_separator)
        for action in actions[sep_index + 1 :]:
            self.bookmarks_menu.removeAction(action)
        # Re-add
        for bm in self._bookmarks:
            self._add_bookmark_action(bm)

    def _save_bookmarks(self):
        """Persist bookmarks to config"""
        if "preferences" not in self.config:
            self.config["preferences"] = {}
        self.config["preferences"]["bookmarks"] = self._bookmarks
        self._schedule_config_save()

    @safe_slot
    def _on_search_return_pressed(self):
        """Handle Enter in search box: search if text, show history if empty"""
        if not self.query_input.text().strip():
            self.completer.setCompletionPrefix("")
            self.completer.complete()
            return
        self.start_search()

    def _is_download_queue_active(self) -> bool:
        """Central gate to prevent search-state mutations while downloads are running."""
        return bool(self.download_worker and self.download_worker.isRunning())

    @safe_slot
    def start_search(self):
        """Initiate a new search"""
        query = self.query_input.text().strip()
        if not query:
            self.status_label.setText("Please enter a search query")
            return

        # Keep table/model identity stable until the queue finishes processing items.
        if self._is_download_queue_active():
            self.status_label.setText("Cannot start a new search while downloads are running")
            return

        if not self.prowlarr:
            self.status_label.setText("Prowlarr client not initialized")
            return

        # Skip if a search is already running
        if self.current_worker:
            return

        # Cancel any active Load All - a new search supersedes it
        self._load_all_active = False

        # Add to search history (move to front if exists, avoiding duplicates)
        if query in self.search_history:
            self.search_history.remove(query)
        self.search_history.insert(0, query)
        self.search_history = self.search_history[:50]  # Keep last 50
        self.completer.model().setStringList(self.search_history)

        # Get filter selections
        indexer_ids = self.get_selected_indexers()
        categories = self.get_selected_categories()

        indexer_info = f"{len(indexer_ids)} selected: {indexer_ids}" if indexer_ids else "all"
        category_info = f"{len(categories)} selected: {categories}" if categories else "all"
        self.log(f"Starting search: query='{query}', page_size={self.prowlarr_page_size}, indexers={indexer_info}, categories={category_info}")

        # Disable download buttons and reset progress bar
        self.download_selected_btn.setEnabled(False)
        self.download_all_btn.setEnabled(False)
        self.download_progress.setMaximum(1)
        self.download_progress.setValue(0)

        # Clear previous results
        self.results_table.setRowCount(0)
        self.results_table.setSortingEnabled(False)  # Disable during population
        self.current_results = []
        self.current_offset = 0
        self._video_paths = {}
        self._downloaded_release_keys = set()
        self._search_generation += 1

        # Reset page number to 1 for new search
        self.prowlarr_page_number_spinbox.blockSignals(True)
        self.prowlarr_page_number_spinbox.setValue(1)
        self.prowlarr_page_number_spinbox.blockSignals(False)

        # Create and start worker thread
        self.current_worker = SearchWorker(self.prowlarr, query, indexer_ids, categories, 0, self.prowlarr_page_size)
        self._track_worker(self.current_worker)
        self.current_worker.search_done.connect(self.page_fetch_finished)
        self.current_worker.error.connect(self.search_error)
        self.current_worker.progress.connect(self.update_status)
        try:
            self.current_worker.start()
        except Exception as e:
            logger.error(f"Failed to start search worker: {e}")
            self.current_worker = None
            self.status_label.setText(f"Failed to start search: {e}")
            return

        # Update UI state
        self.search_btn.setEnabled(False)
        self.load_all_btn.setEnabled(False)
        self.start_spinner()

    @safe_slot
    def start_load_all_pages(self):
        """Start fetching all pages of results sequentially"""
        # Prevent Load All from clearing/replacing rows while the download queue is in flight.
        if self._is_download_queue_active():
            self.status_label.setText("Cannot load pages while downloads are running")
            return

        # If Load All is active, cancel it
        if self._load_all_active:
            self._load_all_active = False
            self.log("Load All: cancelled by user")
            self.status_label.setText("Load All cancelled")
            return

        query = self.query_input.text().strip()
        if not query:
            self.status_label.setText("Please enter a search query")
            return
        if not self.prowlarr:
            self.status_label.setText("Prowlarr client not initialized")
            return

        # Skip if a search is already running
        if self.current_worker:
            return

        # Add to search history (move to front if exists, avoiding duplicates)
        if query in self.search_history:
            self.search_history.remove(query)
        self.search_history.insert(0, query)
        self.search_history = self.search_history[:50]
        self.completer.model().setStringList(self.search_history)

        # Initialize multi-page state
        self._load_all_active = True
        self._load_all_results = []
        self._load_all_page = 1

        # Clear previous results
        self.results_table.setRowCount(0)
        self.results_table.setSortingEnabled(False)
        self.current_results = []
        self.current_offset = 0
        self._video_paths = {}
        self._downloaded_release_keys = set()
        self._search_generation += 1

        # Disable UI
        self.download_selected_btn.setEnabled(False)
        self.download_all_btn.setEnabled(False)
        self.download_progress.setMaximum(1)
        self.download_progress.setValue(0)

        self.prowlarr_page_number_spinbox.blockSignals(True)
        self.prowlarr_page_number_spinbox.setValue(1)
        self.prowlarr_page_number_spinbox.blockSignals(False)

        self.search_btn.setEnabled(False)
        self.load_all_btn.setText("Cancel")
        self.load_all_btn.setEnabled(True)

        self.log(f"Load All Pages: starting (page size: {self.prowlarr_page_size})")
        self._load_all_fetch_page()

    def _load_all_fetch_page(self):
        """Fetch the next page for Load All"""
        query = self.query_input.text().strip()
        indexer_ids = self.get_selected_indexers()
        categories = self.get_selected_categories()
        offset = (self._load_all_page - 1) * self.prowlarr_page_size

        self.status_label.setText(f"Loading page {self._load_all_page}...")
        self.current_worker = SearchWorker(self.prowlarr, query, indexer_ids, categories, offset, self.prowlarr_page_size)
        self._track_worker(self.current_worker)
        self.current_worker.search_done.connect(self.page_fetch_finished)
        self.current_worker.error.connect(self.search_error)
        self.current_worker.progress.connect(self.update_status)
        try:
            self.current_worker.start()
        except Exception as e:
            logger.error(f"Failed to start load-all worker: {e}")
            self.current_worker = None
            self._load_all_active = False
            self.search_btn.setEnabled(True)
            self.load_all_btn.setText("Load A&ll")
            self.load_all_btn.setEnabled(True)
            self.status_label.setText(f"Failed to load page: {e}")
            return

        self.search_btn.setEnabled(False)

        self.start_spinner()

    def fetch_page(self, page_number: int):
        """Fetch a specific page of results"""
        # Guard against row remapping races with active background downloads.
        if self._is_download_queue_active():
            self.status_label.setText("Cannot change page while downloads are running")
            return

        query = self.query_input.text().strip()
        if not query:
            return

        if not self.prowlarr:
            return

        # Cancel any active Load All - explicit page fetch supersedes it
        self._load_all_active = False

        # Get filter selections
        indexer_ids = self.get_selected_indexers()
        categories = self.get_selected_categories()

        # Calculate offset based on page number
        offset = (page_number - 1) * self.prowlarr_page_size

        self.log(f"Fetching page {page_number} (offset: {offset}, page size: {self.prowlarr_page_size})")

        # Clear previous results
        self.results_table.setRowCount(0)
        self.results_table.setSortingEnabled(False)
        self.current_results = []
        self.current_offset = offset
        self._video_paths = {}
        self._search_generation += 1

        # Create and start worker thread
        self.current_worker = SearchWorker(self.prowlarr, query, indexer_ids, categories, offset, self.prowlarr_page_size)
        self._track_worker(self.current_worker)
        self.current_worker.search_done.connect(self.page_fetch_finished)
        self.current_worker.error.connect(self.search_error)
        self.current_worker.progress.connect(self.update_status)
        try:
            self.current_worker.start()
        except Exception as e:
            logger.error(f"Failed to start fetch worker: {e}")
            self.current_worker = None
            self.status_label.setText(f"Failed to fetch page: {e}")
            return

        # Update UI state
        self.search_btn.setEnabled(False)

        self.start_spinner()

    def _track_worker(self, worker):
        """Track a worker for cleanup on app close"""
        if worker and worker not in self._all_workers:
            # Prune finished workers to prevent memory leak
            self._all_workers = [w for w in self._all_workers if w.isRunning()]
            self._all_workers.append(worker)

    def _save_config_with_retry(self) -> bool:
        """
        Save config. Returns True if successful, False otherwise.
        """
        try:
            save_config(self.config)
            self._config_dirty = False
            return True
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            self.status_label.setText(f"ERROR: Failed to save preferences: {e}")
            return False

    def _schedule_config_save(self, delay_ms: int = 300):
        """Mark config dirty and debounce disk writes to keep UI responsive."""
        self._config_dirty = True
        self.config_save_timer.start(delay_ms)

    def _flush_config_save(self):
        """Write config if dirty; used by debounced timer."""
        if not self._config_dirty:
            return
        try:
            save_config(self.config)
            self._config_dirty = False
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            if hasattr(self, "status_label"):
                self.status_label.setText(f"ERROR: Failed to save preferences: {e}")

    def _schedule_timer(self, delay_ms: int, callback):
        """
        Schedule a QTimer.singleShot with tracking for cleanup
        Prevents timers from firing after window closes
        """
        timer = QTimer()
        timer.setSingleShot(True)
        self._pending_timers.append(timer)

        # Wrap callback to auto-cleanup timer from tracking list
        def cleanup_wrapper():
            try:
                callback()
            except Exception:
                logger.error(f"Exception in scheduled timer callback: {traceback.format_exc()}")
            finally:
                if timer in self._pending_timers:
                    self._pending_timers.remove(timer)

        timer.timeout.connect(cleanup_wrapper)
        timer.start(delay_ms)
        return timer

    def _wait_worker(self, worker, name: str, timeout_ms: int = 3000):
        """Wait for a worker thread to finish with cooperative cancellation first."""
        if not worker:
            return
        try:
            # First ask the worker to stop cooperatively.
            if hasattr(worker, "requestInterruption"):
                worker.requestInterruption()
        except Exception as e:
            logger.debug(f"Failed to request interruption for {name}: {e}")

        try:
            if worker.wait(timeout_ms):
                return
        except Exception as e:
            logger.error(f"Failed waiting for {name}: {e}")
            return

        # Emergency fallback only if cooperative stop didn't work in time.
        logger.warning(f"{name} did not stop within {timeout_ms}ms, force terminating")
        try:
            worker.terminate()
            worker.wait(1000)
        except Exception as e:
            logger.error(f"Failed to terminate {name}: {e}")

    @safe_slot
    def page_fetch_finished(self, results: List[Dict], elapsed: float = 0.0):
        """Handle completed page fetch - loads only one page at a time"""
        self.current_worker = None

        # Multi-page "Load All" mode: accumulate results and fetch next page
        if self._load_all_active:
            self._load_all_results.extend(results)
            self.log(f"Load All: page {self._load_all_page} returned {len(results)} results (total: {len(self._load_all_results)})")
            self.status_label.setText(f"Loading... page {self._load_all_page}, {len(self._load_all_results)} results so far")

            if len(results) >= self.prowlarr_page_size:
                # More pages available, fetch next
                self._load_all_page += 1
                self.prowlarr_page_number_spinbox.blockSignals(True)
                self.prowlarr_page_number_spinbox.setValue(self._load_all_page)
                self.prowlarr_page_number_spinbox.blockSignals(False)
                self._load_all_fetch_page()
                return
            else:
                # Last page reached, display all accumulated results
                self._load_all_active = False
                results = self._load_all_results
                self._load_all_results = []
                self.log(f"Load All: complete, {len(results)} total results across {self._load_all_page} pages")
        else:
            # Cancelled or single-page: show accumulated results if any
            if self._load_all_results:
                self._load_all_results.extend(results)
                results = self._load_all_results
                self._load_all_results = []
                self.log(f"Load All: cancelled, showing {len(results)} results from {self._load_all_page} pages")

        self.search_btn.setEnabled(True)
        self.load_all_btn.setText("Load A&ll")
        self.load_all_btn.setEnabled(True)

        self.stop_spinner()

        # Replace results with new page
        self.current_results = results

        self.log(f"Page {self.prowlarr_page_number_spinbox.value()}: Received {len(results)} results in {elapsed:.1f}s")
        self.display_results(results)
        self._restore_column_widths()

        # Build per-indexer stats
        indexer_counts = {}
        for r in results:
            name = r.get("indexer", "Unknown")
            indexer_counts[name] = indexer_counts.get(name, 0) + 1
        if indexer_counts:
            parts = [f"{name}: {count}" for name, count in sorted(indexer_counts.items())]
            indexer_summary = " | ".join(parts)
            self.log(f"Indexer stats: {indexer_summary}")
            self.status_label.setText(f"{len(results)} results in {elapsed:.1f}s  [{indexer_summary}]")
        else:
            self.status_label.setText(f"No results ({elapsed:.1f}s)")

        # Update download button states based on visible rows and selection
        self.update_download_button_states()

        # Apply custom multi-column sort
        self.apply_default_sort()

    @safe_slot
    def search_error(self, error: str):
        """Handle search error"""
        self.current_worker = None
        self.search_btn.setEnabled(True)
        self.load_all_btn.setText("Load A&ll")
        self.load_all_btn.setEnabled(True)

        self.stop_spinner()

        # If load-all was active, stop it and show what we got
        if self._load_all_active:
            self._load_all_active = False
            if self._load_all_results:
                self.current_results = self._load_all_results
                self._load_all_results = []
                self.display_results(self.current_results)
                self.apply_default_sort()

        error_msg = f"Search failed: {error}"
        self.log(error_msg)
        self.status_label.setText(error_msg)

    @safe_slot
    def apply_default_sort(self):
        """
        Apply custom multi-column sort: Title ASC, then Indexer DESC, then Age ASC
        Sorts the current_results and redisplays the table
        """
        if not self.current_results:
            return

        self.log("Applying default sort: Title ASC, then Indexer DESC, then Age ASC...")

        # Sort using cached tuple keys (avoids redundant .lower() calls)
        # Key: (title_lower ASC, indexer_lower_inverted DESC, age ASC)
        def sort_key(r):
            return (
                r.get("title", "").lower(),
                # Invert string for descending: negate each char ordinal
                [-ord(c) for c in r.get("indexer", "").lower()],
                # Keep age positive so smaller day-counts sort first (true ASC).
                (r.get("age") or 0),
            )

        self.current_results.sort(key=sort_key)

        # Clear and redisplay
        self.results_table.setRowCount(0)
        self.results_table.setSortingEnabled(False)
        self.display_results(self.current_results)

        # Clear sort indicator BEFORE re-enabling sorting to prevent auto-resorting
        self.results_table.horizontalHeader().setSortIndicator(-1, Qt.AscendingOrder)
        self.results_table.setSortingEnabled(True)

        self._update_status_bar_counts()

        # Start Everything check in background
        self.start_everything_check()

    def start_everything_check(self):
        """Start background Everything checking for all results"""
        # Skip if Everything not initialized or no results
        if not self.everything or not self.current_results:
            return

        # Defer only when the running worker belongs to an older search generation.
        if self.everything_check_worker and self.everything_check_worker.isRunning():
            if self._everything_check_generation != self._search_generation:
                self._pending_everything_check_generation = self._search_generation
            return

        # We are starting now, so clear any stale deferred request marker.
        self._pending_everything_check_generation = None

        # Create and start new worker (tag with generation for stale batch detection)
        self._everything_check_generation = self._search_generation
        self.everything_check_worker = EverythingCheckWorker(
            self.everything, self.current_results, self.title_match_chars, self.everything_search_chars, self.everything_batch_size
        )
        self._track_worker(self.everything_check_worker)
        self.everything_check_worker.batch_ready.connect(self.on_everything_batch_ready)
        self.everything_check_worker.check_done.connect(self.on_everything_check_finished)
        self.everything_check_worker.progress.connect(self._on_everything_progress)
        self.results_table.setSortingEnabled(False)  # Prevent re-sort during check
        self.start_spinner()

        try:
            self.everything_check_worker.start()
        except Exception as e:
            logger.error(f"Failed to start Everything check worker: {e}")
            self.everything_check_worker = None
            self.results_table.setSortingEnabled(True)
            self.stop_spinner()
            return
        logger.info(f"Started Everything check for {len(self.current_results)} results")

    @safe_slot
    def on_everything_batch_ready(self, batch: list):
        """Process a batch of Everything check results"""
        # Discard stale batches from a previous search generation
        if self._everything_check_generation != self._search_generation:
            return
        hide = self.hide_existing_checkbox.isChecked()
        for row, everything_results in batch:
            title_item = self.results_table.item(row, self.COL_TITLE)
            if not title_item:
                continue

            # Make text dark gray
            title_item.setForeground(QColor(128, 128, 128))

            # Set tooltip with found results (FileName - Size), limited to configured max
            # Get release size for comparison
            size_item = self.results_table.item(row, self.COL_SIZE)
            release_size = size_item.data(Qt.UserRole) if size_item else 0

            tooltip_lines = [f"Found in Everything (release: {format_size(release_size)}):"]
            for filename, size in everything_results[: self.everything_max_results]:
                if os.path.isdir(filename):
                    tooltip_lines.append(f"  {filename}")
                else:
                    size_str = format_size(size)
                    tooltip_lines.append(f"  {filename} - {size_str}")

            title_item.setToolTip("\n".join(tooltip_lines))

            # Store video file path keyed by title
            try:
                video = self._find_video_file(everything_results)
                if video:
                    self._video_paths[title_item.text()] = video
            except Exception as e:
                self.log(f"ERROR in _find_video_file: {e}")

            # Hide this row if "Hide existing" is checked
            if hide:
                self.results_table.setRowHidden(row, True)

    @safe_slot
    def _on_everything_progress(self, checked: int, total: int):
        """Update status bar with Everything check progress"""
        self.status_label.setText(f"Checking Everything: {checked}/{total}")

    @safe_slot
    def on_everything_check_finished(self):
        """Cleanup when Everything check completes"""
        self.everything_check_worker = None
        self.results_table.setSortingEnabled(True)  # Re-enable sorting

        self.stop_spinner()
        logger.info("Everything check completed")
        # Re-apply all filters to catch any changes made during the check
        self.apply_result_filters()

        # If a newer generation requested a check while this worker was active, run it now.
        pending_gen = self._pending_everything_check_generation
        if pending_gen is not None:
            self._pending_everything_check_generation = None
            if pending_gen == self._search_generation:
                self.start_everything_check()

    @safe_slot
    def on_sort_changed(self, logical_index: int):
        """
        Handle sort order change
        Re-apply background colors to avoid similar colors in adjacent rows
        """
        # Give table a moment to complete sorting (tracked timer for cleanup)
        self._schedule_timer(50, self.reapply_row_colors)
        self._update_status_bar_counts()

    def get_palette_colors(self) -> List[QColor]:
        """
        Generate 24 distinct light colors for row backgrounds
        Colors are light but different enough to distinguish groups
        """
        return [
            QColor(255, 230, 230),  # Light red
            QColor(230, 255, 230),  # Light green
            QColor(230, 230, 255),  # Light blue
            QColor(255, 255, 230),  # Light yellow
            QColor(255, 230, 255),  # Light magenta
            QColor(230, 255, 255),  # Light cyan
            QColor(255, 240, 230),  # Light orange
            QColor(240, 230, 255),  # Light purple
            QColor(230, 240, 255),  # Light sky blue
            QColor(255, 255, 240),  # Light cream
            QColor(240, 255, 230),  # Light lime
            QColor(255, 230, 240),  # Light pink
            QColor(245, 245, 255),  # Very light blue
            QColor(255, 245, 245),  # Very light red
            QColor(240, 255, 245),  # Light mint
            QColor(255, 245, 230),  # Light peach
            QColor(245, 230, 255),  # Light lavender
            QColor(255, 250, 230),  # Light gold
            QColor(230, 250, 255),  # Light ice blue
            QColor(255, 230, 250),  # Light rose
            QColor(230, 255, 240),  # Light sea green
            QColor(255, 240, 245),  # Light blush
            QColor(240, 240, 255),  # Light periwinkle
            QColor(255, 245, 240),  # Light apricot
        ]

    def _update_status_bar_counts(self):
        """Update status bar with current sort criteria and visible/total counts"""
        total = self.results_table.rowCount()
        visible = sum(1 for r in range(total) if not self.results_table.isRowHidden(r))
        header = self.results_table.horizontalHeader()
        sort_col = header.sortIndicatorSection()
        sort_order = header.sortIndicatorOrder()
        if 0 <= sort_col < len(self.COL_HEADERS):
            direction = "ASC" if sort_order == Qt.AscendingOrder else "DESC"
            sort_info = f"{self.COL_HEADERS[sort_col]} {direction}"
        else:
            sort_info = "default"
        if visible < total:
            self.status_label.setText(f"Showing {visible}/{total} results (sorted by {sort_info})")
        else:
            self.status_label.setText(f"{total} results (sorted by {sort_info})")

    def reapply_row_colors(self):
        """
        Re-apply row background colors after sort
        Ensures no similar colors appear in adjacent rows
        """
        colors = self.get_palette_colors()
        row_count = self.results_table.rowCount()

        if row_count == 0:
            return

        # Build list of (row_index, title_key) for current sort order
        row_data = []
        for row in range(row_count):
            title_item = self.results_table.item(row, self.COL_TITLE)
            if title_item:
                title = title_item.text()
                title_key = title[: self.title_match_chars].lower()
                row_data.append((row, title_key))

        # Assign colors avoiding adjacent duplicates
        title_to_color = {}
        color_index = 0
        last_color_idx = -1

        for row, title_key in row_data:
            if title_key not in title_to_color:
                # Find a color that's different from the last used color
                attempts = 0
                while attempts < 24:
                    candidate_idx = color_index % 24
                    # Ensure we don't use the same color as the previous row
                    if candidate_idx != last_color_idx or len(title_to_color) >= 24:
                        title_to_color[title_key] = colors[candidate_idx]
                        last_color_idx = candidate_idx
                        color_index += 1
                        break
                    color_index += 1
                    attempts += 1

            # Apply color to all columns except button column
            color = title_to_color[title_key]
            for col in range(self.COL_DOWNLOAD):
                item = self.results_table.item(row, col)
                if item:
                    item.setBackground(color)

    def _recheck_everything_for_titles(self, title_keys: set, expected_generation: int = None):
        """
        Re-check Everything for all rows matching any of the given title prefixes.
        Called after download with configurable delay.
        Runs on a background worker to avoid blocking the main thread.
        """
        # Skip if a new search has started since the recheck was scheduled
        if expected_generation is not None and expected_generation != self._search_generation:
            self.log(f"Skipping recheck (search generation changed)")
            return

        # Skip if worker is still running to avoid concurrent SDK access
        if self.everything_check_worker and self.everything_check_worker.isRunning():
            self.log(f"Skipping recheck (Everything worker still running)")
            return

        # Collect rows matching any of the title groups
        recheck_results = []
        for check_row in range(self.results_table.rowCount()):
            check_title_item = self.results_table.item(check_row, self.COL_TITLE)
            if check_title_item:
                check_title = check_title_item.text()
                check_title_key = check_title[: self.title_match_chars].lower()
                if check_title_key in title_keys:
                    recheck_results.append({"title": check_title})

        if not recheck_results:
            return

        self.log(f"Re-checking Everything for {len(recheck_results)} rows across {len(title_keys)} title groups...")

        # Disable sorting to keep row indices stable during recheck
        self.results_table.setSortingEnabled(False)

        # Tag with generation for stale batch detection
        self._everything_check_generation = self._search_generation

        # Run recheck on background worker
        self.everything_check_worker = EverythingCheckWorker(
            self.everything, recheck_results, self.title_match_chars,
            self.everything_search_chars, self.everything_batch_size
        )
        self._track_worker(self.everything_check_worker)

        # Build title -> worker index map for resolving rows at batch delivery time
        recheck_title_map = {}  # worker index -> title string
        for i, result in enumerate(recheck_results):
            recheck_title_map[i] = result["title"]

        # Snapshot every current row for each title so duplicate titles are all updated.
        title_to_rows = {}
        for r in range(self.results_table.rowCount()):
            item = self.results_table.item(r, self.COL_TITLE)
            if not item:
                continue
            title_to_rows.setdefault(item.text(), []).append(r)

        # Remap worker row indices to actual table rows by title lookup (sort-safe)
        def on_recheck_batch(batch):
            try:
                remapped = []
                seen_rows = set()
                for idx, results in batch:
                    title = recheck_title_map.get(idx)
                    if title is None:
                        logger.warning(f"Recheck batch index {idx} not in title map")
                        continue
                    # Update all rows sharing the same title, not just the first one.
                    for r in title_to_rows.get(title, []):
                        if r in seen_rows:
                            continue
                        remapped.append((r, results))
                        seen_rows.add(r)
                if remapped:
                    self.on_everything_batch_ready(remapped)
            except Exception as e:
                logger.error(f"Error in on_recheck_batch: {e}")

        self.everything_check_worker.batch_ready.connect(on_recheck_batch)
        self.everything_check_worker.check_done.connect(self.on_everything_check_finished)
        try:
            self.everything_check_worker.start()
        except Exception as e:
            logger.error(f"Failed to start recheck worker: {e}")
            self.everything_check_worker = None
            self.results_table.setSortingEnabled(True)
            return

    def _toggle_column_visibility(self, col: int, hidden: bool):
        """Toggle column visibility and persist to config via debounce."""
        try:
            self.results_table.setColumnHidden(col, hidden)
            if "preferences" not in self.config:
                self.config["preferences"] = {}
            hidden_cols = [self.COL_HEADERS[c] for c in range(self.COL_COUNT) if self.results_table.isColumnHidden(c)]
            self.config["preferences"]["hidden_columns"] = hidden_cols
            self._schedule_config_save()
        except Exception as e:
            logger.error(f"Failed to toggle column visibility: {e}")

    def _save_column_widths(self):
        """Save current column widths to preferences"""
        widths = []
        for col in range(self.COL_COUNT):
            if col == self.COL_TITLE:
                continue  # Title column stretches, skip
            widths.append(self.results_table.columnWidth(col))
        if "preferences" not in self.config:
            self.config["preferences"] = {}
        self.config["preferences"]["column_widths"] = widths

    def _restore_column_widths(self):
        """Restore column widths from saved preferences"""
        widths = self.config.get("preferences", {}).get("column_widths", [])
        if not widths:
            return
        idx = 0
        for col in range(self.COL_COUNT):
            if col == self.COL_TITLE:
                continue  # Title column stretches, skip
            if idx < len(widths):
                self.results_table.setColumnWidth(col, widths[idx])
                idx += 1

    @safe_slot
    def _reset_view(self):
        """Reset all columns to visible and default widths"""
        for col in range(self.COL_COUNT):
            self.results_table.setColumnHidden(col, False)
        self.results_table.resizeColumnsToContents()
        # Re-set Title to stretch mode
        self.results_table.horizontalHeader().setSectionResizeMode(self.COL_TITLE, QHeaderView.Stretch)
        # Clear saved widths and hidden columns
        if "preferences" in self.config:
            self.config["preferences"].pop("column_widths", None)
            self.config["preferences"].pop("hidden_columns", None)
        self._schedule_config_save()
        self.status_label.setText("View reset: all columns visible, default widths")

    def display_results(self, results: List[Dict]):
        """
        Display search results in table
        Groups by title (configurable chars) with 24 alternating colors
        """
        # Warn user about large result sets that may cause UI slowdown
        if len(results) > 5000:
            self.log(f"WARNING: Displaying {len(results)} results may cause UI slowdown. Consider using filters.")
            self.status_label.setText(f"Loading {len(results)} results (may be slow)...")

        # Get 24 color palette
        colors = self.get_palette_colors()

        # Track colors for title groups
        title_colors = {}
        color_index = 0

        for result in results:
            row = self.results_table.rowCount()
            self.results_table.insertRow(row)

            # Age column (sortable by numeric value, age is in days)
            age = result.get("age") or 0
            age_str = format_age(age)
            age_item = NumericTableWidgetItem(age_str)
            age_item.setData(Qt.UserRole, age)  # Store numeric value for sorting
            self.results_table.setItem(row, self.COL_AGE, age_item)

            # Title column
            title = result.get("title", "Unknown")
            title_key = title[: self.title_match_chars].lower()  # Group by configurable chars

            # Assign color to this title group from 24-color palette
            if title_key not in title_colors:
                title_colors[title_key] = colors[color_index % 24]
                color_index += 1

            title_item = QTableWidgetItem(title)
            self.results_table.setItem(row, self.COL_TITLE, title_item)

            # Quality column (parsed from title)
            quality = parse_quality(title)
            quality_item = QTableWidgetItem(quality)
            self.results_table.setItem(row, self.COL_QUALITY, quality_item)

            # Size column (sortable by numeric value)
            size = result.get("size") or 0
            size_str = format_size(size)
            size_item = NumericTableWidgetItem(size_str)
            size_item.setData(Qt.UserRole, size)  # Store numeric value for sorting
            self.results_table.setItem(row, self.COL_SIZE, size_item)

            # Seeders column
            seeders = result.get("seeders")
            seeders_item = NumericTableWidgetItem(str(seeders) if seeders is not None else "")
            seeders_item.setData(Qt.UserRole, seeders if seeders is not None else -1)
            self.results_table.setItem(row, self.COL_SEEDERS, seeders_item)

            # Leechers column
            leechers = result.get("leechers")
            leechers_item = NumericTableWidgetItem(str(leechers) if leechers is not None else "")
            leechers_item.setData(Qt.UserRole, leechers if leechers is not None else -1)
            self.results_table.setItem(row, self.COL_LEECHERS, leechers_item)

            # Grabs column
            grabs = result.get("grabs")
            grabs_item = NumericTableWidgetItem(str(grabs) if grabs is not None else "")
            grabs_item.setData(Qt.UserRole, grabs if grabs is not None else -1)
            self.results_table.setItem(row, self.COL_GRABS, grabs_item)

            # Indexer column
            indexer = result.get("indexer", "Unknown")
            indexer_item = QTableWidgetItem(indexer)
            self.results_table.setItem(row, self.COL_INDEXER, indexer_item)

            # Download button
            download_btn = QPushButton("Download")
            download_btn.setProperty("guid", result.get("guid"))
            download_btn.setProperty("indexerId", result.get("indexerId"))
            download_btn.setProperty("title", title)  # Store title for later
            download_btn.clicked.connect(lambda checked, btn=download_btn: self._download_from_button(btn))
            self.results_table.setCellWidget(row, self.COL_DOWNLOAD, download_btn)

            # Apply background color to row (same color for same title group)
            # Re-apply downloaded state (dark red text) if GUID was previously downloaded
            guid = result.get("guid")
            indexer_id = result.get("indexerId")
            release_key = (guid, indexer_id)
            is_downloaded = guid is not None and indexer_id is not None and release_key in self._downloaded_release_keys
            for col in range(self.COL_DOWNLOAD):  # Don't color button column
                item = self.results_table.item(row, col)
                if item:
                    item.setBackground(title_colors[title_key])
                    if is_downloaded:
                        item.setForeground(QColor(139, 0, 0))

    def _collect_row_download_item(self, row: int) -> Optional[Dict]:
        """Extract download info from a table row"""
        button = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
        if not button:
            return None
        guid = button.property("guid")
        indexer_id = button.property("indexerId")
        title = button.property("title")
        # Accept indexer_id=0 as valid; only reject missing id or empty guid.
        if guid in (None, "") or indexer_id is None:
            return None
        return {"guid": guid, "indexer_id": indexer_id, "title": title}

    def _download_from_button(self, btn):
        """Find the button's current row and download that release"""
        try:
            for row in range(self.results_table.rowCount()):
                if self.results_table.cellWidget(row, self.COL_DOWNLOAD) is btn:
                    self.download_release(row)
                    return
        except Exception as e:
            logger.error(f"Failed to download from button: {e}")

    def download_release(self, row: int):
        """Download a single release via the background queue"""
        item = self._collect_row_download_item(row)
        if not item:
            return
        # Skip if already downloaded
        release_key = (item["guid"], item["indexer_id"])
        if release_key in self._downloaded_release_keys:
            self.status_label.setText(f"Already downloaded: {item.get('title', 'Unknown')}")
            return
        self.start_download_queue([item])

    @safe_slot
    def download_selected(self):
        """Download all selected releases via the background queue"""
        selected_rows = sorted(set(idx.row() for idx in self.results_table.selectedIndexes()))
        if not selected_rows:
            self.status_label.setText("No rows selected")
            return
        items = []
        for row in selected_rows:
            item = self._collect_row_download_item(row)
            if item:
                items.append(item)
        if items:
            self.start_download_queue(items)

    @safe_slot
    def download_all(self):
        """Download all visible (non-hidden, non-downloaded) releases in the table via the background queue"""
        items = []
        for row in range(self.results_table.rowCount()):
            if self.results_table.isRowHidden(row):
                continue
            # Skip already-downloaded rows using the authoritative GUID set
            btn = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
            if btn:
                release_key = (btn.property("guid"), btn.property("indexerId"))
                if release_key in self._downloaded_release_keys:
                    continue
            item = self._collect_row_download_item(row)
            if item:
                items.append(item)
        if items:
            self.start_download_queue(items)

    @safe_slot
    def select_best_per_group(self):
        """Select the best release per title group (highest seeders, fallback largest size)"""
        self.results_table.clearSelection()
        groups = {}  # title_key -> (best_row, best_seeders, best_size)

        for row in range(self.results_table.rowCount()):
            if self.results_table.isRowHidden(row):
                continue
            title_item = self.results_table.item(row, self.COL_TITLE)
            if not title_item:
                continue

            title_key = title_item.text()[: self.title_match_chars].lower()
            seeders_item = self.results_table.item(row, self.COL_SEEDERS)
            size_item = self.results_table.item(row, self.COL_SIZE)
            raw_seeders = seeders_item.data(Qt.UserRole) if seeders_item else -1
            size = size_item.data(Qt.UserRole) if size_item else 0
            # Treat -1 (NZB/unknown) as very high so usenet isn't penalized
            seeders = raw_seeders if raw_seeders >= 0 else float("inf")

            if title_key not in groups:
                groups[title_key] = (row, seeders, size)
            else:
                _, prev_seeders, prev_size = groups[title_key]
                # Prefer higher seeders; if equal, prefer larger size
                if (seeders, size) > (prev_seeders, prev_size):
                    groups[title_key] = (row, seeders, size)

        # Select the best rows
        count = 0
        sel_model = self.results_table.selectionModel()
        for title_key, (row, _, _) in groups.items():
            for col in range(self.COL_COUNT):
                idx = self.results_table.model().index(row, col)
                sel_model.select(idx, sel_model.SelectionFlag.Select)
            count += 1

        self.status_label.setText(f"Selected best release from {count} title groups")
        self.log(f"Select best per group: {count} groups")

    def start_download_queue(self, items: List[Dict]):
        """Start background download worker with a list of items, or append to running queue"""
        if not self.prowlarr:
            self.status_label.setText("Prowlarr client not initialized")
            return

        # Normalize this request to unique release keys so progress totals stay accurate.
        deduped_items = []
        seen_keys = set()
        for item in items:
            key = (item.get("guid"), item.get("indexer_id"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped_items.append(item)
        items = deduped_items
        if not items:
            self.status_label.setText("No new items to queue")
            return

        # If a download is already running, append new items to the existing queue
        if self.download_worker and self.download_worker.isRunning():
            added_items = self.download_worker.add_items(items)
            added = len(added_items)
            if added == 0:
                self.status_label.setText("All selected items are already queued")
                return

            # Update progress bar maximum to include new items
            self.download_progress.setMaximum(self.download_progress.maximum() + added)

            # Extend GUID → row mapping for new items
            for item in added_items:
                for r in range(self.results_table.rowCount()):
                    btn = self.results_table.cellWidget(r, self.COL_DOWNLOAD)
                    if btn and btn.property("guid") == item["guid"] and btn.property("indexerId") == item["indexer_id"]:
                        self._release_key_to_row[(item["guid"], item["indexer_id"])] = r
                        break

            self.log(f"Added {added} item(s) to download queue")
            self.status_label.setText(f"Added {added} item(s) to download queue")
            return

        total = len(items)
        self.log(f"Starting download queue: {total} item(s)")

        # Configure progress bar
        self.download_progress.setMaximum(total)
        self.download_progress.setValue(0)

        # Disable sorting during download to keep GUID->row mapping valid
        self.results_table.setSortingEnabled(False)

        # Disable search during download queue processing, enable Cancel
        self.search_btn.setEnabled(False)


        # Track downloaded title keys for targeted Everything recheck
        self._downloaded_title_keys = set()

        # Build GUID → row mapping for sort-safe row lookup
        self._release_key_to_row = {}
        for item in items:
            for r in range(self.results_table.rowCount()):
                btn = self.results_table.cellWidget(r, self.COL_DOWNLOAD)
                if btn and btn.property("guid") == item["guid"] and btn.property("indexerId") == item["indexer_id"]:
                    self._release_key_to_row[(item["guid"], item["indexer_id"])] = r
                    break

        # Create and start worker
        self.download_worker = DownloadWorker(self.prowlarr, items)
        self._track_worker(self.download_worker)
        self.download_worker.progress.connect(self.on_download_progress)
        self.download_worker.item_downloaded.connect(self.on_item_downloaded)
        self.download_worker.queue_done.connect(self.on_download_queue_finished)
        try:
            self.download_worker.start()
        except Exception as e:
            logger.error(f"Failed to start download worker: {e}")
            self.download_worker = None
            self.results_table.setSortingEnabled(True)
            self.search_btn.setEnabled(True)
            self.status_label.setText(f"Failed to start downloads: {e}")
            return

    @safe_slot
    def on_download_progress(self, current: int, total: int, title: str):
        """Update progress bar and status during batch download"""
        self.download_progress.setValue(current)
        self.status_label.setText(f"Downloading {current}/{total} [ {title} ]")

    def _find_row_by_release_key(self, guid: str, indexer_id: int) -> int:
        """Find table row by (guid, indexer_id), using cached mapping first then scanning."""
        release_key = (guid, indexer_id)
        row = self._release_key_to_row.get(release_key, -1)
        if row >= 0:
            btn = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
            if btn and btn.property("guid") == guid and btn.property("indexerId") == indexer_id:
                return row
        # Fallback: scan table (handles post-sort row changes).
        for r in range(self.results_table.rowCount()):
            btn = self.results_table.cellWidget(r, self.COL_DOWNLOAD)
            if btn and btn.property("guid") == guid and btn.property("indexerId") == indexer_id:
                self._release_key_to_row[release_key] = r
                return r
        return -1

    @safe_slot
    def on_item_downloaded(self, guid: str, indexer_id: int, success: bool):
        """Handle individual item download result, identified by (guid, indexer_id)."""
        row = self._find_row_by_release_key(guid, indexer_id)
        if row < 0:
            self.log(f"Download result for unknown release key: ({guid}, {indexer_id})")
            return

        button = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
        title = button.property("title") if button else "Unknown"

        # Get indexer name for history
        indexer_item = self.results_table.item(row, self.COL_INDEXER)
        indexer = indexer_item.text() if indexer_item else "Unknown"

        if success:
            self.log(f"Downloaded: {title}")
            self._write_download_history(title, indexer, True)
            release_key = (guid, indexer_id)
            self._downloaded_release_keys.add(release_key)
            # Make row text dark red to indicate downloaded
            for col in range(self.COL_DOWNLOAD):
                item = self.results_table.item(row, col)
                if item:
                    item.setForeground(QColor(139, 0, 0))  # Dark red
            # Track title key for targeted Everything recheck
            title_key = title[: self.title_match_chars].lower()
            self._downloaded_title_keys.add(title_key)
        else:
            self.log(f"Failed to download: {title}")
            self._write_download_history(title, indexer, False)

    @safe_slot
    def on_download_queue_finished(self):
        """Handle download queue completion"""
        self.download_worker = None
        self.download_progress.setMaximum(1)
        self.download_progress.setValue(0)

        # Re-enable sorting (was disabled during download to protect GUID cache)
        self.results_table.setSortingEnabled(True)

        # Re-enable search and disable cancel (was swapped during download)
        self.search_btn.setEnabled(True)


        # Re-enable download buttons based on current state
        self.update_download_button_states()

        timestamp = datetime.now().strftime("%H:%M:%S")
        self.status_label.setText(f"[{timestamp}] Download queue complete")
        self.log("Download queue finished")

        # Schedule Everything recheck only for downloaded title groups
        # Optimized: single timer rechecks all keys instead of one timer per key
        if self.everything and self._downloaded_title_keys:
            title_keys = self._downloaded_title_keys.copy()
            self._downloaded_title_keys = set()
            gen = self._search_generation

            # Batch recheck: single worker for all title keys
            def recheck_all_downloaded():
                self._recheck_everything_for_titles(title_keys, gen)

            self.log(f"Scheduling Everything recheck in {self.everything_recheck_delay}ms for {len(title_keys)} title groups...")
            self._schedule_timer(self.everything_recheck_delay, recheck_all_downloaded)

    def _write_download_history(self, title: str, indexer: str, success: bool):
        """
        Append a download record to the persistent download history log
        Implements automatic log rotation when file exceeds 10 MB
        """
        try:
            history_file = DOWNLOAD_HISTORY_PATH
            max_size = 10 * 1024 * 1024  # 10 MB

            # Check if rotation needed
            if os.path.exists(history_file) and os.path.getsize(history_file) > max_size:
                # Rotate: .log -> .log.1, .log.1 -> .log.2, etc. (keep 5 files)
                for i in range(4, 0, -1):
                    old_file = f"{history_file}.{i}"
                    new_file = f"{history_file}.{i + 1}"
                    if os.path.exists(old_file):
                        os.replace(old_file, new_file)
                os.replace(history_file, f"{history_file}.1")
                logger.info(f"Rotated download history log (exceeded {max_size / 1024 / 1024:.1f} MB)")

            # Append new record (escape tabs/newlines to preserve TSV format)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = "OK" if success else "FAIL"
            safe_title = title.replace('\t', ' ').replace('\n', ' ').replace('\r', '')
            safe_indexer = indexer.replace('\t', ' ').replace('\n', ' ').replace('\r', '')
            with open(history_file, "a", encoding="utf-8") as f:
                f.write(f"{timestamp}\t{status}\t{safe_indexer}\t{safe_title}\n")
        except Exception as e:
            logger.error(f"Failed to write download history: {e}")

    @safe_slot
    def _open_download_history(self):
        """Open the download history log file (cross-platform)"""
        history_path = DOWNLOAD_HISTORY_PATH
        if not os.path.exists(history_path):
            self.status_label.setText("No download history yet")
            return

        try:
            if sys.platform == 'win32':
                os.startfile(history_path)
            elif sys.platform == 'darwin':  # macOS
                subprocess.run(['open', history_path], check=True)
            else:  # Linux and others
                subprocess.run(['xdg-open', history_path], check=True)
        except Exception as e:
            logger.error(f"Failed to open download history: {e}")
            self.status_label.setText(f"Cannot open file: {e}")

    @safe_slot
    def update_download_button_states(self):
        """Enable/disable download buttons based on current table state and selection"""
        def is_row_downloadable(row: int) -> bool:
            if self.results_table.isRowHidden(row):
                return False
            btn = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
            if not btn:
                return False
            guid = btn.property("guid")
            indexer_id = btn.property("indexerId")
            if guid is None or indexer_id is None:
                return False
            key = (guid, indexer_id)
            # Only enable actions for rows that are still actionable.
            return key not in self._downloaded_release_keys

        # Download All: enabled only when at least one visible row is truly actionable.
        has_visible_downloadable = any(is_row_downloadable(row) for row in range(self.results_table.rowCount()))
        self.download_all_btn.setEnabled(has_visible_downloadable)

        # Download Selected: enabled only when selected rows include an actionable row.
        selected_rows = set(idx.row() for idx in self.results_table.selectedIndexes())
        has_selected_downloadable = any(is_row_downloadable(row) for row in selected_rows)
        self.download_selected_btn.setEnabled(has_selected_downloadable)

    def get_current_row_title(self) -> Optional[str]:
        """Get title from currently selected row"""
        current_row = self.results_table.currentRow()
        if current_row >= 0:
            title_item = self.results_table.item(current_row, self.COL_TITLE)
            if title_item:
                return title_item.text()
        return None

    VIDEO_EXTENSIONS = {
        ".mkv",
        ".mp4",
        ".avi",
        ".wmv",
        ".flv",
        ".mov",
        ".webm",
        ".m4v",
        ".ts",
        ".mpg",
        ".mpeg",
        ".m2ts",
    }

    def _find_video_file(self, everything_results: list) -> Optional[str]:
        """Find the first video file path from Everything search results"""
        logger.debug(f"_find_video_file: checking {len(everything_results)} results")
        for i, item in enumerate(everything_results):
            try:
                file_path, _size = item
                _, ext = os.path.splitext(file_path)
                if ext.lower() in MainWindow.VIDEO_EXTENSIONS:
                    logger.debug(f"_find_video_file: FOUND at index {i}: {file_path}")
                    return file_path
            except Exception as e:
                logger.warning(f"_find_video_file: ERROR at index {i}: {e} item={repr(item)}")
        logger.debug(f"_find_video_file: NO VIDEO FOUND")
        return None

    def _get_video_path_for_row(self, row: int) -> Optional[str]:
        """Get stored video file path for a row by looking up its title in the dict"""
        title_item = self.results_table.item(row, self.COL_TITLE)
        if not title_item:
            return None
        return self._video_paths.get(title_item.text())

    @safe_slot
    def _on_cell_double_clicked(self, row: int, column: int):
        """Download release on double-click"""
        self.download_release(row)

    @safe_slot
    def _show_header_context_menu(self, pos):
        """Show right-click context menu on table header to toggle column visibility"""
        menu = QMenu(self)
        for col in range(self.COL_COUNT):
            if col == self.COL_TITLE:
                continue  # Title column is always visible
            name = self.COL_HEADERS[col]
            action = menu.addAction(name)
            action.setCheckable(True)
            action.setChecked(not self.results_table.isColumnHidden(col))
            action.toggled.connect(lambda checked, c=col: self._toggle_column_visibility(c, not checked))
        menu.exec(self.results_table.horizontalHeader().mapToGlobal(pos))

    @safe_slot
    def _show_context_menu(self, pos):
        """Show right-click context menu on results table"""
        row = self.results_table.rowAt(pos.y())
        if row < 0:
            return

        menu = QMenu(self)

        # Download
        download_action = menu.addAction("Download (Space)")
        download_action.triggered.connect(lambda: self.download_release(row))

        menu.addSeparator()

        # Copy title
        copy_action = menu.addAction("Copy Title (C)")
        copy_action.triggered.connect(lambda: self._context_copy_title(row))

        # Web search
        web_action = menu.addAction("Web Search (G)")
        web_action.triggered.connect(lambda: self._context_web_search(row))

        # Play video
        play_action = menu.addAction("Play Video (P)")
        video_path = self._get_video_path_for_row(row)
        play_action.setEnabled(video_path is not None)
        def _play_video():
            try:
                if video_path:
                    os.startfile(video_path)
            except Exception as e:
                logger.error(f"Failed to play video: {e}")
        play_action.triggered.connect(_play_video)

        # Everything search
        if self.everything:
            everything_action = menu.addAction("Search Everything (S)")
            title_item = self.results_table.item(row, self.COL_TITLE)
            title = title_item.text() if title_item else None
            everything_action.setEnabled(title is not None)
            def _search_everything():
                try:
                    if title:
                        self.everything.launch_search(title)
                except Exception as e:
                    logger.error(f"Failed to launch Everything: {e}")
            everything_action.triggered.connect(_search_everything)

        # Custom commands
        for key, label in [(Qt.Key_F2, "F2"), (Qt.Key_F3, "F3"), (Qt.Key_F4, "F4")]:
            cmd = self.custom_commands.get(key, "")
            if cmd:
                action = menu.addAction(f"Custom Command {label}")
                action.triggered.connect(lambda checked=False, k=key, c=cmd: self._run_custom_command(k, c))

        menu.exec(self.results_table.viewport().mapToGlobal(pos))

    def _context_copy_title(self, row: int):
        """Copy title from specific row to clipboard"""
        try:
            title_item = self.results_table.item(row, self.COL_TITLE)
            if title_item:
                QApplication.clipboard().setText(title_item.text())
                self.log(f"Copied to clipboard: {title_item.text()}")
                self.status_label.setText("Title copied to clipboard")
        except Exception as e:
            logger.error(f"Failed to copy title: {e}")

    def _context_web_search(self, row: int):
        """Open web search for specific row's title"""
        try:
            title_item = self.results_table.item(row, self.COL_TITLE)
            if title_item:
                url = self.web_search_url.replace("{query}", quote(title_item.text()))
                webbrowser.open(url)
                self.log(f"Opened web search for: {title_item.text()}")
        except Exception as e:
            logger.error(f"Failed to open web search: {e}")

    @safe_slot
    def _toggle_find_bar(self):
        """Toggle the find bar visibility"""
        if self.find_bar.isVisible():
            self._close_find_bar()
        else:
            self.find_bar.setVisible(True)
            self.find_input.setFocus()
            self.find_input.selectAll()

    @safe_slot
    def _close_find_bar(self):
        """Hide the find bar and return focus to table"""
        self.find_bar.setVisible(False)
        self.results_table.setFocus()

    @safe_slot
    def _find_next(self):
        """Find next matching row in the table"""
        self._find_in_table(forward=True)

    @safe_slot
    def _find_prev(self):
        """Find previous matching row in the table"""
        self._find_in_table(forward=False)

    def _find_in_table(self, forward: bool = True):
        """Search table titles for the find text, selecting the next/prev match"""
        text = self.find_input.text().strip().lower()
        if not text:
            return

        row_count = self.results_table.rowCount()
        if row_count == 0:
            return

        current = self.results_table.currentRow()
        start = (current + (1 if forward else -1)) % row_count

        for i in range(row_count):
            row = (start + (i if forward else -i)) % row_count
            if self.results_table.isRowHidden(row):
                continue
            title_item = self.results_table.item(row, self.COL_TITLE)
            if title_item and text in title_item.text().lower():
                self.results_table.setCurrentCell(row, self.COL_TITLE)
                self.results_table.scrollToItem(title_item)
                self.status_label.setText(f"Found: row {row + 1}")
                return

        self.status_label.setText(f"Not found: '{self.find_input.text()}'")

    def eventFilter(self, obj, event):
        """Handle Esc key in find bar to close it"""
        if obj == self.find_input and event.type() == event.Type.KeyPress:
            if event.key() == Qt.Key_Escape:
                self._close_find_bar()
                return True
            if event.key() == Qt.Key_Return and event.modifiers() & Qt.ShiftModifier:
                self._find_prev()
                return True
        return super().eventFilter(obj, event)

    def table_key_press(self, event):
        """
        Handle keyboard shortcuts in results table
        Space: Download current row and move to next
        S: Launch Everything.exe with release title
        C: Copy release title to clipboard
        G: Open web search with release title
        P: Play video file found by Everything
        """
        if event.key() == Qt.Key_Space:
            # Download current release and move to next visible row
            current_row = self.results_table.currentRow()
            if current_row >= 0:
                self.download_release(current_row)
                next_row = current_row + 1
                while next_row < self.results_table.rowCount() and self.results_table.isRowHidden(next_row):
                    next_row += 1
                if next_row < self.results_table.rowCount():
                    self.results_table.setCurrentCell(next_row, self.COL_AGE)
                event.accept()
                return

        elif event.key() == Qt.Key_S:
            # Launch Everything.exe with search
            if self.everything:
                title = self.get_current_row_title()
                if title:
                    self.everything.launch_search(title)
                    self.log(f"Launched Everything search for: {title}")
                    event.accept()
                    return
            else:
                self.status_label.setText("Everything not initialized yet")
                event.accept()
                return

        elif event.key() == Qt.Key_C:
            # Copy title to clipboard
            title = self.get_current_row_title()
            if title:
                clipboard = QApplication.clipboard()
                clipboard.setText(title)
                self.log(f"Copied to clipboard: {title}")
                self.status_label.setText("Title copied to clipboard")
                event.accept()
                return

        elif event.key() == Qt.Key_G:
            # Open web search
            title = self.get_current_row_title()
            if title:
                url = self.web_search_url.replace("{query}", quote(title))
                webbrowser.open(url)
                self.log(f"Opened web search for: {title}")
                event.accept()
                return

        elif event.key() == Qt.Key_P:
            # Play video file found by Everything
            current_row = self.results_table.currentRow()
            if current_row >= 0:
                video_path = self._get_video_path_for_row(current_row)
                if video_path:
                    self.log(f"Playing: {video_path}")
                    self.status_label.setText(f"Playing: {os.path.basename(video_path)}")
                    os.startfile(video_path)
                else:
                    self.status_label.setText("No video file found in Everything results")
                event.accept()
                return

        elif event.key() in self.custom_commands:
            cmd = self.custom_commands[event.key()]
            if cmd:
                self._run_custom_command(event.key(), cmd)
            else:
                key_name = {Qt.Key_F2: "F2", Qt.Key_F3: "F3", Qt.Key_F4: "F4"}.get(event.key(), "?")
                self.status_label.setText(f"No custom command configured for {key_name} (set custom_command_{key_name} in config.toml)")
            event.accept()
            return

        elif event.key() == Qt.Key_A and event.modifiers() & Qt.ControlModifier:
            # Select all visible (non-hidden) rows
            selection_model = self.results_table.selectionModel()
            selection = QItemSelection()
            col_count = self.results_table.columnCount()
            model = self.results_table.model()
            for row in range(self.results_table.rowCount()):
                if not self.results_table.isRowHidden(row):
                    top_left = model.index(row, 0)
                    bottom_right = model.index(row, col_count - 1)
                    selection.select(top_left, bottom_right)
            selection_model.select(selection, QItemSelectionModel.SelectionFlag.ClearAndSelect)
            visible = len(selection.indexes()) // max(col_count, 1)
            self.status_label.setText(f"Selected {visible} visible rows")
            event.accept()
            return

        elif event.key() == Qt.Key_Tab:
            # Jump to next title group
            if event.modifiers() & Qt.ShiftModifier:
                self._jump_title_group(forward=False)
            else:
                self._jump_title_group(forward=True)
            event.accept()
            return

        # Pass other keys to default handler
        QTableWidget.keyPressEvent(self.results_table, event)

    def _jump_title_group(self, forward: bool = True):
        """Jump to the first row of the next/previous title group"""
        current_row = self.results_table.currentRow()
        if current_row < 0:
            return

        row_count = self.results_table.rowCount()
        if row_count == 0:
            return

        # Get current title group key
        current_title_item = self.results_table.item(current_row, self.COL_TITLE)
        if not current_title_item:
            return
        current_key = current_title_item.text()[: self.title_match_chars].lower()

        # Search for next row with a different title group key
        step = 1 if forward else -1
        row = current_row + step
        while 0 <= row < row_count:
            if not self.results_table.isRowHidden(row):
                title_item = self.results_table.item(row, self.COL_TITLE)
                if title_item:
                    key = title_item.text()[: self.title_match_chars].lower()
                    if key != current_key:
                        self.results_table.setCurrentCell(row, self.COL_TITLE)
                        self.results_table.scrollToItem(title_item)
                        return
            row += step

    def _run_custom_command(self, key, cmd_template: str):
        """Run a custom command with {title} and {video} placeholders"""
        current_row = self.results_table.currentRow()
        if current_row < 0:
            self.status_label.setText("No row selected")
            return

        title = self.get_current_row_title() or ""
        video = self._get_video_path_for_row(current_row) or ""
        key_name = {Qt.Key_F2: "F2", Qt.Key_F3: "F3", Qt.Key_F4: "F4"}.get(key, "?")

        # Build argument list safely (no shell=True)
        import shlex

        cmd = cmd_template.replace("{title}", title).replace("{video}", video)

        self.log(f"{key_name} command: {cmd}")
        self.status_label.setText(f"Running {key_name} command...")

        try:
            args = shlex.split(cmd, posix=False)
            subprocess.Popen(args)
        except Exception as e:
            self.log(f"{key_name} command failed: {e}")
            self.status_label.setText(f"{key_name} command failed: {e}")

    def start_spinner(self):
        """Switch activity bar to indeterminate (marquee) mode"""
        self.activity_bar.setRange(0, 0)

    def stop_spinner(self):
        """Switch activity bar back to idle (full bar)"""
        self.activity_bar.setRange(0, 1)
        self.activity_bar.setValue(1)

    @safe_slot
    def update_status(self, message: str):
        """Update status bar message and log"""
        self.status_label.setText(message)
        self.log(message)

    def log(self, message: str):
        """Log message to file and log window"""
        logger.info(message)
        if self.log_window:
            self.log_window.append_log(message)

    @safe_slot
    def toggle_log_window(self):
        """Show or hide the log window"""
        if self.log_window.isVisible():
            self.log_window.hide()
        else:
            self.log_window.show()

    def closeEvent(self, event):
        """
        Handle application close event
        Save preferences before exiting
        """
        # Wait for any running workers to finish, terminate if needed
        self._wait_worker(self.init_worker, "InitWorker", 3000)
        self._wait_worker(self.current_worker, "SearchWorker", 3000)
        self._wait_worker(self.everything_check_worker, "EverythingCheckWorker", 3000)
        self._wait_worker(self.download_worker, "DownloadWorker", 3000)

        # Final cleanup: wait for all tracked workers
        for worker in self._all_workers:
            if worker and worker.isRunning():
                if not worker.wait(2000):
                    logger.error(f"Worker still running at shutdown, force terminating")
                    worker.terminate()
                    worker.wait(500)

        # Stop splitter save timer
        if self.splitter_save_timer.isActive():
            self.splitter_save_timer.stop()
        if self.config_save_timer.isActive():
            self.config_save_timer.stop()

        # Stop all pending timers to prevent firing after window closes
        # Copy list first: a timer's cleanup_wrapper could modify _pending_timers during iteration
        timers_snapshot = list(self._pending_timers)
        timer_count = len(timers_snapshot)
        self._pending_timers.clear()
        for timer in timers_snapshot:
            if timer and timer.isActive():
                timer.stop()
        logger.info(f"Stopped {timer_count} pending timers")

        # Ensure preferences section exists
        if "preferences" not in self.config:
            self.config["preferences"] = {}

        # Update preferences (preserve existing ones like splitter_sizes)
        self.config["preferences"]["search_history"] = self.search_history
        self.config["preferences"]["selected_indexers"] = self._get_checked_indexer_ids()
        self.config["preferences"]["selected_categories"] = self._get_checked_category_ids()

        # Save final splitter sizes and column widths
        self.config["preferences"]["splitter_sizes"] = self.splitter.sizes()
        self.config["preferences"]["hide_existing"] = self.hide_existing_checkbox.isChecked()
        self._save_column_widths()

        # Save hidden columns
        hidden_cols = [self.COL_HEADERS[col] for col in range(self.COL_COUNT) if self.results_table.isColumnHidden(col)]
        self.config["preferences"]["hidden_columns"] = hidden_cols

        # Save with retry logic (critical - don't lose user preferences)
        if self._save_config_with_retry():
            self.log("Application closing, preferences saved")
        else:
            self.log("ERROR: Failed to save preferences after retries")

        # Close log window (no parent, so must close explicitly)
        if self.log_window:
            self.log_window.close()

        event.accept()


def main():
    """Application entry point"""
    # Ensure config exists
    ensure_config_exists()

    # Setup logging
    setup_logging()

    app = QApplication(sys.argv)

    # Create and show main window
    window = MainWindow()
    window.showMaximized()

    # Start Qt event loop
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
