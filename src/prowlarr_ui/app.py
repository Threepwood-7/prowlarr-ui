#!/usr/bin/env python3
"""
Prowlarr Search Client - PySide6 GUI Application
Searches Prowlarr indexers and integrates with Everything for duplicate detection
"""

import logging
import math
import os
import sys
import time
import traceback
import webbrowser
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import ClassVar, TypedDict, cast
from urllib.parse import quote

from PySide6.QtCore import (
    QEvent,
    QObject,
    QPoint,
    QStringListModel,
    Qt,
    QTimer,
)
from PySide6.QtGui import (
    QAction,
    QCloseEvent,
    QColor,
    QIcon,
    QKeyEvent,
    QKeySequence,
    QPainter,
    QPen,
    QPixmap,
    QStandardItem,
    QStandardItemModel,
)
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QCompleter,
    QDialog,
    QDialogButtonBox,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QSplitter,
    QStatusBar,
    QTableWidget,
    QTextBrowser,
    QTreeView,
    QVBoxLayout,
    QWidget,
)
from threep_commons.desktop import open_path_in_default_app
from threep_commons.formatters import format_size
from threep_commons.logging import resolve_log_path, setup_logging_from_identity
from threep_commons.paths import configure_qsettings
from threep_commons.qt.slots import safe_slot
from threep_commons.settings import QSettingsValueStore

from .api.everything_search import EverythingSearch
from .api.prowlarr_client import ProwlarrClient
from .app_results_navigation import (
    close_find_bar as close_results_find_bar,
)
from .app_results_navigation import (
    find_in_table as find_results_in_table,
)
from .app_results_navigation import (
    find_next as find_next_result,
)
from .app_results_navigation import (
    find_prev as find_prev_result,
)
from .app_results_navigation import (
    handle_find_event,
)
from .app_results_navigation import (
    jump_title_group as jump_result_title_group,
)
from .app_results_navigation import (
    run_custom_command as run_results_custom_command,
)
from .app_results_navigation import (
    table_key_press as handle_results_table_key_press,
)
from .app_results_navigation import (
    toggle_find_bar as toggle_results_find_bar,
)
from .app_results_rendering import (
    build_palette_colors,
    reapply_result_row_colors,
    render_results_table,
    update_results_status,
)
from .app_ui_layout import build_center_panel, build_left_panel, setup_main_window_ui
from .constants import APP_IDENTITY, SETTINGS_APP_NAME
from .ui.help_text import HELP_HTML
from .ui.log_window import LogWindow
from .ui.setup_wizard import run_setup_wizard
from .utils.config import (
    config_store_file_path,
    ensure_config_exists,
    get_missing_required_config,
    load_config,
    save_config,
    validate_config,
)
from .workers.download_worker import DownloadWorker
from .workers.everything_worker import EverythingCheckWorker
from .workers.init_worker import InitWorker
from .workers.search_worker import SearchWorker

DOWNLOAD_HISTORY_PATH = str(resolve_log_path(APP_IDENTITY, "download_history.log"))

logger = logging.getLogger(__name__)

PREFS_NAMESPACE_KEYS = {
    "search_history",
    "bookmarks",
    "selected_indexers",
    "selected_categories",
}
__all__ = ["EverythingSearch", "InitWorker", "MainWindow", "safe_slot"]

type ReleaseDict = dict[str, object]
type IndexerDict = dict[str, object]
type CategoryDict = dict[str, object]
type ReleaseKey = tuple[str, int]
type EverythingMatches = list[tuple[str, int]]
type EverythingBatch = list[tuple[int, EverythingMatches]]
type WorkerThread = InitWorker | SearchWorker | EverythingCheckWorker | DownloadWorker


class DeferredEverythingRecheck(TypedDict):
    """Deferred Everything recheck payload carried across worker boundaries."""

    title_keys: set[str]
    generation: int


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
    COL_HEADERS: ClassVar[list[str]] = [
        "Age",
        "Title",
        "Quality",
        "Size",
        "Seeders",
        "Leechers",
        "Grabs",
        "Indexer",
        "Download",
    ]

    activity_bar: QProgressBar
    categories_model: QStandardItemModel
    categories_tree: QTreeView
    completer: QCompleter
    download_all_btn: QPushButton
    download_progress: QProgressBar
    download_selected_btn: QPushButton
    filter_max_age: QSpinBox
    filter_min_size: QSpinBox
    filter_title_input: QLineEdit
    find_bar: QWidget
    find_input: QLineEdit
    hide_existing_checkbox: QCheckBox
    indexers_model: QStandardItemModel
    indexers_tree: QTreeView
    load_all_btn: QPushButton
    prowlarr_page_number_spinbox: QSpinBox
    prowlarr_page_size_spinbox: QSpinBox
    query_input: QLineEdit
    results_table: QTableWidget
    search_btn: QPushButton
    splitter: QSplitter
    status_bar: QStatusBar
    status_label: QLabel

    @staticmethod
    def _object_dict(value: object) -> dict[str, object]:
        """Normalize one object to a plain string-key dict when possible."""
        if not isinstance(value, Mapping):
            return {}
        mapping = cast("Mapping[object, object]", value)
        return {str(key): entry for key, entry in mapping.items()}

    @staticmethod
    def _int_value(value: object, default: int) -> int:
        """Coerce one object to int with a safe fallback."""
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return default
        return default

    @staticmethod
    def _float_value(value: object, default: float) -> float:
        """Coerce one object to float with a safe fallback."""
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, int | float):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                return default
        return default

    @staticmethod
    def _text_value(value: object, default: str = "") -> str:
        """Normalize one object to text for UI display and command generation."""
        return str(value or default)

    @staticmethod
    def _completer_model(completer: QCompleter) -> QStringListModel:
        """Return the string-list model used by the search-history completer."""
        return cast("QStringListModel", completer.model())

    def _config_section(self, section_name: str) -> dict[str, object]:
        """Fetch one top-level config section as a plain object dict."""
        return self._object_dict(self.config.get(section_name, {}))

    def _configure_main_window(self) -> None:
        """Apply the static window chrome used before runtime initialization."""
        self.setWindowTitle("Prowlarr Search Client")
        self.setGeometry(100, 100, 1400, 800)
        self.setWindowIcon(self._create_globe_icon())

    def _load_runtime_configuration(self) -> tuple[dict[str, object], list[str]]:
        """Load config, validate it, and return the settings section plus warnings."""
        self.config = load_config()
        config_warnings = validate_config(self.config)
        for warning in config_warnings:
            logger.warning(f"Config: {warning}")
        return self._config_section("settings"), config_warnings

    def _initialize_runtime_settings(self, settings: Mapping[str, object]) -> None:
        """Load scalar runtime settings from the validated config."""
        self.title_match_chars = self._int_value(
            settings.get("title_match_chars", 42), 42
        )
        self.everything_search_chars = self._int_value(
            settings.get("everything_search_chars", 42), 42
        )
        self.everything_recheck_delay = self._int_value(
            settings.get("everything_recheck_delay", 6000), 6000
        )
        self.web_search_url = self._text_value(
            settings.get("web_search_url", "https://www.google.com/search?q={query}"),
            "https://www.google.com/search?q={query}",
        )
        self.everything_integration_method = self._text_value(
            settings.get("everything_integration_method", "sdk"),
            "sdk",
        )
        self.prowlarr_page_size = self._int_value(
            settings.get("prowlarr_page_size", 100), 100
        )
        self.everything_max_results = self._int_value(
            settings.get("everything_max_results", 5), 5
        )
        self.custom_commands = {
            Qt.Key.Key_F2: self._text_value(settings.get("custom_command_F2", "")),
            Qt.Key.Key_F3: self._text_value(settings.get("custom_command_F3", "")),
            Qt.Key.Key_F4: self._text_value(settings.get("custom_command_F4", "")),
        }
        self.everything_batch_size = self._int_value(
            settings.get("everything_batch_size", 10), 10
        )

    def _initialize_service_clients(self, settings: Mapping[str, object]) -> None:
        """Create lightweight service clients and defer heavyweight ones."""
        try:
            prowlarr_config = self._config_section("prowlarr")
            self.prowlarr = ProwlarrClient(
                self._text_value(
                    prowlarr_config.get("host", "http://localhost:9696"),
                    "http://localhost:9696",
                ),
                self._text_value(prowlarr_config.get("api_key", "")),
                timeout=self._int_value(settings.get("api_timeout", 300), 300),
                retries=self._int_value(settings.get("api_retries", 2), 2),
                http_basic_auth_username=self._text_value(
                    prowlarr_config.get("http_basic_auth_username", "")
                ),
                http_basic_auth_password=self._text_value(
                    prowlarr_config.get("http_basic_auth_password", "")
                ),
            )
            logger.info("Prowlarr client initialized")
        except Exception as error:
            logger.error(f"Failed to initialize Prowlarr client: {error}")
            self.prowlarr = None

        self.everything = None

    def _initialize_preferences(self) -> None:
        """Create the shared preference store and load persisted UI history."""
        self.preferences_store = QSettingsValueStore.from_identity(
            APP_IDENTITY,
            app_name=SETTINGS_APP_NAME,
        )
        self.search_history = self.preferences_store.get_str_list(
            self._pref_key("search_history"),
            [],
        )

    def _initialize_runtime_state(self, settings: Mapping[str, object]) -> None:
        """Initialize runtime bookkeeping used by searches, downloads, and UI."""
        self.current_worker: SearchWorker | None = None
        self.everything_check_worker: EverythingCheckWorker | None = None
        self.download_worker: DownloadWorker | None = None
        self.current_results: list[ReleaseDict] = []
        self.current_offset = 0
        self._video_paths: dict[ReleaseKey, str] = {}
        self._search_generation = 0
        self._everything_check_generation = 0
        self._pending_everything_check_generation: int | None = None
        self._pending_everything_recheck: DeferredEverythingRecheck | None = None
        self._downloaded_release_keys: set[ReleaseKey] = set()
        self._downloaded_title_keys: set[str] = set()
        self._release_key_to_row: dict[ReleaseKey, int] = {}
        self._active_spinner_tags: dict[str, int] = {}
        self._table_sort_locks: set[str] = set()
        self._close_retry_pending = False
        self._close_retry_timer: QTimer | None = None
        self._shutdown_in_progress = False
        self._shutdown_interrupted_worker_ids: set[int] = set()
        self._download_queue_retry_limit = 12
        self._download_queue_stale_grace_seconds = self._float_value(
            settings.get("download_queue_stale_grace_seconds", 20.0),
            20.0,
        )
        self._download_queue_owner_since: float | None = None
        self._shutdown_force_after_seconds = self._float_value(
            settings.get("shutdown_force_after_seconds", 15.0),
            15.0,
        )
        self._shutdown_force_arm_seconds = self._float_value(
            settings.get("shutdown_force_arm_seconds", 8.0),
            8.0,
        )
        self._shutdown_started_monotonic: float | None = None
        self._shutdown_force_prompted = False
        self._shutdown_force_armed_until: float | None = None
        self._everything_check_stale_grace_seconds = self._float_value(
            settings.get("everything_check_stale_grace_seconds", 20.0),
            20.0,
        )
        self._everything_check_owner_since: float | None = None
        self._indexers_loaded = False
        self._categories_loaded = False
        self._indexers_item_changed_connected = False
        self._categories_item_changed_connected = False
        self._load_all_active = False
        self._load_all_results: list[ReleaseDict] = []
        self._load_all_page = 0
        self._all_workers: list[WorkerThread] = []
        self._pending_timers: list[QTimer] = []
        self._config_dirty = False
        self._prefs_dirty = False

    def _initialize_timers(self) -> None:
        """Create startup timers used for debounced saves and cleanup."""
        self.splitter_save_timer = QTimer()
        self.splitter_save_timer.setSingleShot(True)
        self.splitter_save_timer.timeout.connect(self.save_splitter_sizes)

        self.config_save_timer = QTimer()
        self.config_save_timer.setSingleShot(True)
        self.config_save_timer.timeout.connect(self._flush_config_save)

    def _log_config_warnings(self, config_warnings: list[str]) -> None:
        """Mirror config warnings into the in-app log window after UI setup."""
        for warning in config_warnings:
            self.log(f"WARNING: {warning}")

    def _start_background_initialization(self, settings: Mapping[str, object]) -> None:
        """Start the heavyweight background initialization worker."""
        self.init_worker = InitWorker(
            self.everything_integration_method,
            self.prowlarr,
            self._text_value(settings.get("everything_sdk_url", "")),
        )
        self.init_worker.init_done.connect(self._on_init_done)
        self.init_worker.start()

    def __init__(self) -> None:
        super().__init__()
        configure_qsettings(APP_IDENTITY)
        self._configure_main_window()
        settings, config_warnings = self._load_runtime_configuration()
        self._initialize_runtime_settings(settings)
        self._initialize_service_clients(settings)
        self._initialize_preferences()
        self._initialize_runtime_state(settings)
        self._initialize_timers()
        self.log_window = LogWindow(self)
        self.setup_ui()
        self.setup_menu()
        self._log_config_warnings(config_warnings)
        self._start_background_initialization(settings)

    @staticmethod
    def _pref_key(name: str) -> str:
        namespace = "prefs" if name in PREFS_NAMESPACE_KEYS else "ui"
        return f"{namespace}/{name}"

    def pref_key(self, name: str) -> str:
        """Expose the preference-key helper to layout collaborators."""
        return self._pref_key(name)

    def int_value(self, value: object, default: int) -> int:
        """Expose numeric coercion to extracted collaborators."""
        return self._int_value(value, default)

    def text_value(self, value: object, default: str = "") -> str:
        """Expose text coercion to extracted collaborators."""
        return self._text_value(value, default)

    def download_from_button(self, button: QPushButton) -> None:
        """Expose button-driven download dispatch to extracted collaborators."""
        self._download_from_button(button)

    def is_release_downloaded(self, guid: str, indexer_id: int) -> bool:
        """Expose release-download state for extracted row renderers."""
        return (
            bool(guid)
            and indexer_id >= 0
            and (guid, indexer_id) in self._downloaded_release_keys
        )

    def get_video_path_for_row(self, row: int) -> str | None:
        """Expose row-to-video-path lookup to extracted collaborators."""
        return self._get_video_path_for_row(row)

    def setup_ui(self) -> None:
        """Build the main window UI using the extracted layout helpers."""
        setup_main_window_ui(self)

    def create_left_panel(self) -> QWidget:
        """Build the left control panel using the extracted layout helpers."""
        return build_left_panel(self)

    def create_center_panel(self) -> QWidget:
        """Build the center results panel using the extracted layout helpers."""
        return build_center_panel(self)

    def on_search_return_pressed(self) -> None:
        """Forward the search-box return key handler."""
        self._on_search_return_pressed()

    def show_header_context_menu(self, pos: QPoint) -> None:
        """Forward the results-header context menu handler."""
        self._show_header_context_menu(pos)

    def show_context_menu(self, pos: QPoint) -> None:
        """Forward the results-table context menu handler."""
        self._show_context_menu(pos)

    def on_cell_double_clicked(self, row: int, column: int) -> None:
        """Forward double-click handling for result rows."""
        self._on_cell_double_clicked(row, column)

    def toggle_find_bar(self) -> None:
        """Forward the Ctrl+F find-bar toggle."""
        self._toggle_find_bar()

    def close_find_bar(self) -> None:
        """Forward the find-bar close action."""
        self._close_find_bar()

    def find_next(self) -> None:
        """Forward the next-match action for the find bar."""
        self._find_next()

    def find_prev(self) -> None:
        """Forward the previous-match action for the find bar."""
        self._find_prev()

    def _schedule_preferences_sync(self, delay_ms: int = 300):
        """Debounce INI sync to avoid frequent disk writes."""
        self._prefs_dirty = True
        self.config_save_timer.start(delay_ms)

    def _sync_preferences(self):
        try:
            self.preferences_store.sync()
            self._prefs_dirty = False
        except Exception as e:
            logger.error(f"Failed to sync preferences INI: {e}")
            if hasattr(self, "status_label"):
                self.status_label.setText(f"ERROR: Failed to sync preferences: {e}")

    def _persist_runtime_preferences(self):
        """Persist current runtime preference state to INI."""
        self.preferences_store.set_value(
            self._pref_key("search_history"),
            list(self.search_history),
        )
        # Avoid wiping saved selection when close occurs before async init
        # populates trees.
        if self._indexers_loaded:
            self.preferences_store.set_value(
                self._pref_key("selected_indexers"),
                self._get_checked_indexer_ids(),
            )
        if self._categories_loaded:
            self.preferences_store.set_value(
                self._pref_key("selected_categories"),
                self._get_checked_category_ids(),
            )
        self.preferences_store.set_value(
            self._pref_key("splitter_sizes"),
            [int(s) for s in self.splitter.sizes()],
        )
        self.preferences_store.set_value(
            self._pref_key("hide_existing"),
            bool(self.hide_existing_checkbox.isChecked()),
        )
        hidden_cols = [
            self.COL_HEADERS[col]
            for col in range(self.COL_COUNT)
            if self.results_table.isColumnHidden(col)
        ]
        self.preferences_store.set_value(self._pref_key("hidden_columns"), hidden_cols)
        self._save_column_widths()
        self._prefs_dirty = True

    @safe_slot
    def on_splitter_moved(self, pos: int, index: int):
        """Handle splitter moved - debounce and save sizes after user stops moving"""
        # Restart timer - will only save 500ms after user stops moving
        self.splitter_save_timer.start(500)

    @safe_slot
    def save_splitter_sizes(self):
        """Save splitter sizes to INI preferences"""
        sizes = self.splitter.sizes()
        self.preferences_store.set_value(
            self._pref_key("splitter_sizes"),
            [int(s) for s in sizes],
        )
        self._schedule_preferences_sync()
        logger.info(f"Splitter sizes saved: {sizes}")

    @safe_slot
    def on_hide_existing_toggled(self, checked: bool):
        """Handle Hide existing checkbox toggle - save preference and apply filter"""
        self.preferences_store.set_value(self._pref_key("hide_existing"), bool(checked))
        self._schedule_preferences_sync()
        self.apply_hide_existing_filter()

    def apply_hide_existing_filter(self):
        """Convenience wrapper - apply all filters including hide-existing"""
        self.apply_result_filters()

    @safe_slot
    def apply_result_filters(self):
        """Apply all row filters: hide-existing, title text, min size, max age"""
        hide_existing = self.hide_existing_checkbox.isChecked()
        title_filter = (
            self.filter_title_input.text().strip().lower()
            if hasattr(self, "filter_title_input")
            else ""
        )
        min_size_mb = (
            self.filter_min_size.value() if hasattr(self, "filter_min_size") else 0
        )
        max_age_days = (
            self.filter_max_age.value() if hasattr(self, "filter_max_age") else 0
        )

        min_size_bytes = min_size_mb * 1024 * 1024

        for row in range(self.results_table.rowCount()):
            hidden = False

            title_item = self.results_table.item(row, self.COL_TITLE)
            # Hide existing (Everything match)
            if (
                hide_existing
                and title_item
                and title_item.toolTip().startswith("Found in Everything")
            ):
                hidden = True

            # Title text filter
            if (
                not hidden
                and title_filter
                and title_item
                and title_filter not in title_item.text().lower()
            ):
                hidden = True

            # Min size filter
            if not hidden and min_size_bytes > 0:
                size_item = self.results_table.item(row, self.COL_SIZE)
                if size_item:
                    size_val = size_item.data(Qt.ItemDataRole.UserRole)
                    if size_val is not None and size_val < min_size_bytes:
                        hidden = True

            # Max age filter
            if not hidden and max_age_days > 0:
                age_item = self.results_table.item(row, self.COL_AGE)
                if age_item:
                    age_val = age_item.data(Qt.ItemDataRole.UserRole)
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
        if (
            hasattr(self, "query_input")
            and self.query_input.text().strip()
            and not self.current_worker
        ):
            self.fetch_page(value)

    @staticmethod
    def _create_globe_icon() -> QIcon:
        """Draw a simple globe icon (circle + meridians + parallels)"""
        size = 64
        pix = QPixmap(size, size)
        pix.fill(Qt.GlobalColor.transparent)
        p = QPainter(pix)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        pen = QPen(QColor(60, 130, 200), 2)
        p.setPen(pen)
        p.setBrush(QColor(180, 220, 255))
        m = 3  # margin
        p.drawEllipse(m, m, size - 2 * m, size - 2 * m)
        # Meridians (vertical ellipses)
        p.setBrush(Qt.BrushStyle.NoBrush)
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
        exit_action.setShortcuts([QKeySequence("Ctrl+Q"), QKeySequence("Alt+X")])
        exit_action.setShortcutContext(Qt.ShortcutContext.ApplicationShortcut)
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
        best_per_group_action.setStatusTip(
            "Highlight the best result in each title group based on size and seeders"
        )
        best_per_group_action.triggered.connect(self.select_best_per_group)
        view_menu.addAction(best_per_group_action)

        reset_sort_action = QAction("&Reset Sorting", self)
        reset_sort_action.setStatusTip(
            "Restore default sort order: Title ASC, Indexer DESC, Age ASC"
        )
        reset_sort_action.triggered.connect(self.apply_default_sort)
        view_menu.addAction(reset_sort_action)

        fit_columns_action = QAction("&Fit Columns", self)
        fit_columns_action.setStatusTip("Resize visible columns to fit their contents")
        fit_columns_action.triggered.connect(self._fit_columns)
        view_menu.addAction(fit_columns_action)

        reset_view_action = QAction("Reset &View", self)
        reset_view_action.setStatusTip(
            "Reset column widths, splitter position, and sort order to defaults"
        )
        reset_view_action.triggered.connect(self._reset_view)
        view_menu.addAction(reset_view_action)

        # Tools menu
        tools_menu = menubar.addMenu("&Tools")
        edit_ini_action = QAction("Edit &.ini File", self)
        edit_ini_action.setStatusTip(
            f"Open preferences INI file: {self.preferences_store.file_name()}"
        )
        edit_ini_action.triggered.connect(self._edit_preferences_ini_file)
        tools_menu.addAction(edit_ini_action)

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
        self._bookmarks = self.preferences_store.get_str_list(
            self._pref_key("bookmarks"),
            [],
        )
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
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        btn_box.accepted.connect(dlg.accept)
        layout.addWidget(btn_box)
        dlg.exec()

    @safe_slot
    def _on_init_done(
        self,
        everything: EverythingSearch | None,
        indexers: list[IndexerDict],
        error: str,
    ) -> None:
        """Handle background init completion - update UI on main thread"""
        self.everything = everything
        if self.everything:
            if self.everything_integration_method == "none":
                self.log("Everything integration disabled")
            else:
                self.log(
                    "Everything search integration initialized "
                    f"(method: {self.everything_integration_method})"
                )

        if error:
            self.log(error)
            self.status_label.setText(error)
            return

        if not self.prowlarr:
            self.status_label.setText("Prowlarr not configured")
            return

        if indexers:
            self.populate_indexers(indexers)
            categories: list[CategoryDict] = self.prowlarr.get_categories()
            self.populate_categories(categories)
            self.log(
                f"Loaded {len(indexers)} indexers and {len(categories)} categories"
            )
            self.status_label.setText(f"Ready - {len(indexers)} indexers loaded")
        else:
            self.status_label.setText("No indexers found")

    def populate_indexers(self, indexers: list[IndexerDict]) -> None:
        """Populate indexers tree with checkboxes"""
        self.indexers_model.clear()

        # Root "All" item
        root = QStandardItem("All")
        root.setCheckable(True)
        root.setCheckState(Qt.CheckState.Checked)
        self.indexers_model.appendRow(root)

        # Load saved indexer selection from INI (None = no saved preference).
        saved_indexers = self.preferences_store.get_int_list(
            self._pref_key("selected_indexers"),
            None,
        )

        # Add each enabled indexer as child
        for indexer in indexers:
            if not bool(indexer.get("enable", False)):
                continue
            indexer_id = self._int_value(indexer.get("id", 0), 0)
            item = QStandardItem(
                self._text_value(indexer.get("name", "Unknown"), "Unknown")
            )
            item.setCheckable(True)
            item.setData(indexer_id, Qt.ItemDataRole.UserRole)

            # Restore saved state or default to checked on first run
            if saved_indexers is not None:
                if indexer_id in saved_indexers:
                    item.setCheckState(Qt.CheckState.Checked)
                else:
                    item.setCheckState(Qt.CheckState.Unchecked)
            else:
                item.setCheckState(Qt.CheckState.Checked)

            root.appendRow(item)

        # Derive parent state from children so restored state is internally consistent.
        if root.rowCount() == 0:
            root.setCheckState(Qt.CheckState.Unchecked)
        else:
            all_checked = all(
                root.child(i).checkState() == Qt.CheckState.Checked
                for i in range(root.rowCount())
            )
            any_checked = any(
                root.child(i).checkState() == Qt.CheckState.Checked
                for i in range(root.rowCount())
            )
            root.setCheckState(
                Qt.CheckState.Checked
                if all_checked
                else (
                    Qt.CheckState.PartiallyChecked
                    if any_checked
                    else Qt.CheckState.Unchecked
                )
            )

        self.indexers_tree.expandAll()

        # Connect change handler (disconnect first to avoid duplicate connections)
        if self._indexers_item_changed_connected:
            try:
                self.indexers_model.itemChanged.disconnect(self.indexer_item_changed)
            except (RuntimeError, TypeError):
                self._indexers_item_changed_connected = False
        self.indexers_model.itemChanged.connect(self.indexer_item_changed)
        self._indexers_item_changed_connected = True
        self._indexers_loaded = True

    def populate_categories(self, categories: list[CategoryDict]) -> None:
        """Populate categories tree with checkboxes"""
        self.categories_model.clear()

        # Root "All" item
        root = QStandardItem("All")
        root.setCheckable(True)
        root.setCheckState(Qt.CheckState.Checked)
        self.categories_model.appendRow(root)

        # Load saved category selection from INI (None = no saved preference).
        saved_categories = self.preferences_store.get_int_list(
            self._pref_key("selected_categories"),
            None,
        )

        # Add each category with code in brackets
        for category in categories:
            category_id = self._int_value(category.get("id", 0), 0)
            category_name = self._text_value(category.get("name", "Unknown"), "Unknown")
            item = QStandardItem(f"{category_name} [{category_id}]")
            item.setCheckable(True)
            item.setData(category_id, Qt.ItemDataRole.UserRole)

            # Restore saved state or default to checked on first run
            if saved_categories is not None:
                if category_id in saved_categories:
                    item.setCheckState(Qt.CheckState.Checked)
                else:
                    item.setCheckState(Qt.CheckState.Unchecked)
            else:
                item.setCheckState(Qt.CheckState.Checked)

            root.appendRow(item)

        # Derive parent state from children so restored state is internally consistent.
        if root.rowCount() == 0:
            root.setCheckState(Qt.CheckState.Unchecked)
        else:
            all_checked = all(
                root.child(i).checkState() == Qt.CheckState.Checked
                for i in range(root.rowCount())
            )
            any_checked = any(
                root.child(i).checkState() == Qt.CheckState.Checked
                for i in range(root.rowCount())
            )
            root.setCheckState(
                Qt.CheckState.Checked
                if all_checked
                else (
                    Qt.CheckState.PartiallyChecked
                    if any_checked
                    else Qt.CheckState.Unchecked
                )
            )

        self.categories_tree.expandAll()

        # Connect change handler (disconnect first to avoid duplicate connections)
        if self._categories_item_changed_connected:
            try:
                self.categories_model.itemChanged.disconnect(self.category_item_changed)
            except (RuntimeError, TypeError):
                self._categories_item_changed_connected = False
        self.categories_model.itemChanged.connect(self.category_item_changed)
        self._categories_item_changed_connected = True
        self._categories_loaded = True

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
            all_checked = all(
                root.child(i).checkState() == Qt.CheckState.Checked
                for i in range(root.rowCount())
            )
            any_checked = any(
                root.child(i).checkState() == Qt.CheckState.Checked
                for i in range(root.rowCount())
            )
            root.setCheckState(
                Qt.CheckState.Checked
                if all_checked
                else (
                    Qt.CheckState.PartiallyChecked
                    if any_checked
                    else Qt.CheckState.Unchecked
                )
            )
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
            all_checked = all(
                root.child(i).checkState() == Qt.CheckState.Checked
                for i in range(root.rowCount())
            )
            any_checked = any(
                root.child(i).checkState() != Qt.CheckState.Unchecked
                for i in range(root.rowCount())
            )
            root.setCheckState(
                Qt.CheckState.Checked
                if all_checked
                else (
                    Qt.CheckState.PartiallyChecked
                    if any_checked
                    else Qt.CheckState.Unchecked
                )
            )
            self.categories_model.blockSignals(False)

    def get_selected_indexers(self) -> list[int] | None:
        """
        Get list of selected indexer IDs
        Returns:
            None: all selected (do not send explicit filter to API)
            []: none selected
            [ids...]: explicit subset
        """
        root = self.indexers_model.item(0)
        if not root:
            return None

        if root.rowCount() == 0:
            return []

        all_checked = all(
            root.child(i).checkState() == Qt.CheckState.Checked
            for i in range(root.rowCount())
        )
        if all_checked:
            return None

        # Return explicit checked indexers (possibly empty when user deselects all).
        selected: list[int] = []
        for i in range(root.rowCount()):
            child = root.child(i)
            if child.checkState() == Qt.CheckState.Checked:
                indexer_id = self._int_value(child.data(Qt.ItemDataRole.UserRole), -1)
                if indexer_id >= 0:
                    selected.append(indexer_id)

        return selected

    def get_selected_categories(self) -> list[int] | None:
        """
        Get list of selected category IDs
        Returns:
            None: all selected (do not send explicit filter to API)
            []: none selected
            [ids...]: explicit subset
        """
        root = self.categories_model.item(0)
        if not root:
            return None

        if root.rowCount() == 0:
            return []

        all_checked = all(
            root.child(i).checkState() == Qt.CheckState.Checked
            for i in range(root.rowCount())
        )
        if all_checked:
            return None

        # Return explicit checked categories (possibly empty when user deselects all).
        selected: list[int] = []
        for i in range(root.rowCount()):
            child = root.child(i)
            if child.checkState() == Qt.CheckState.Checked:
                category_id = self._int_value(child.data(Qt.ItemDataRole.UserRole), -1)
                if category_id >= 0:
                    selected.append(category_id)

        return selected

    def _resolve_search_scope(self) -> tuple[list[int] | None, list[int] | None] | None:
        """
        Resolve current indexer/category filter scope.
        Returns None after writing status when scope is explicitly empty.
        """
        indexer_ids = self.get_selected_indexers()
        if indexer_ids == []:
            self.status_label.setText("No indexers selected")
            return None

        categories = self.get_selected_categories()
        if categories == []:
            self.status_label.setText("No categories selected")
            return None

        return indexer_ids, categories

    def _get_checked_indexer_ids(self) -> list[int]:
        """Get explicit list of checked indexer IDs for saving preferences"""
        root = self.indexers_model.item(0)
        if not root:
            return []
        ids: list[int] = []
        for i in range(root.rowCount()):
            child = root.child(i)
            if child.checkState() == Qt.CheckState.Checked:
                indexer_id = self._int_value(child.data(Qt.ItemDataRole.UserRole), -1)
                if indexer_id >= 0:
                    ids.append(indexer_id)
        return ids

    def _get_checked_category_ids(self) -> list[int]:
        """Get explicit list of checked category IDs for saving preferences"""
        root = self.categories_model.item(0)
        if not root:
            return []
        ids: list[int] = []
        for i in range(root.rowCount()):
            child = root.child(i)
            if child.checkState() == Qt.CheckState.Checked:
                cat_id = self._int_value(child.data(Qt.ItemDataRole.UserRole), -1)
                if cat_id >= 0:
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
        action = QAction(query.replace("&", "&&"), self)
        action.setData(query)
        action.setStatusTip(f'Search for "{query}"')

        def trigger_bookmark(_checked: bool = False, q: str = query) -> None:
            self._search_bookmark(q)

        action.triggered.connect(trigger_bookmark)
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
            action_query = action.data()
            if action_query == query:
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
        """Persist bookmarks to INI preferences."""
        self.preferences_store.set_value(
            self._pref_key("bookmarks"),
            list(self._bookmarks),
        )
        self._schedule_preferences_sync()

    @safe_slot
    def _on_search_return_pressed(self):
        """Handle Enter in search box: search if text, show history if empty"""
        if not self.query_input.text().strip():
            self.completer.setCompletionPrefix("")
            self.completer.complete()
            return
        self.start_search()

    def _refresh_table_sort_state(self):
        """Apply table sorting state from lock owners."""
        if not hasattr(self, "results_table"):
            return
        self.results_table.setSortingEnabled(not bool(self._table_sort_locks))

    def _acquire_table_sort_lock(self, owner: str):
        """Disable sorting while owner mutates row order/identity."""
        if owner:
            self._table_sort_locks.add(owner)
        self._refresh_table_sort_state()

    def _release_table_sort_lock(self, owner: str):
        """Release owner lock and re-enable sorting only when no owners remain."""
        if owner:
            self._table_sort_locks.discard(owner)
        self._refresh_table_sort_state()

    def _clear_download_queue_ownership(self, reason: str = ""):
        """Clear stale download queue ownership and restore dependent UI state."""
        had_owner = (
            self.download_worker is not None
            or self._download_queue_owner_since is not None
        )
        self.download_worker = None
        self._download_queue_owner_since = None
        self._release_table_sort_lock("download")
        if hasattr(self, "search_btn"):
            self.search_btn.setEnabled(True)
        if hasattr(self, "download_progress"):
            self.download_progress.setMaximum(1)
            self.download_progress.setValue(0)
        if reason:
            logger.warning(reason)
            if hasattr(self, "status_label"):
                self.status_label.setText(reason)
        if had_owner and hasattr(self, "update_download_button_states"):
            self.update_download_button_states()

    def _clear_everything_check_ownership(self, reason: str = ""):
        """Clear stale Everything-check ownership and restore dependent UI state."""
        self.everything_check_worker = None
        self._everything_check_owner_since = None
        self._release_table_sort_lock("everything")
        self.stop_spinner("everything")
        if reason:
            logger.warning(reason)
            if hasattr(self, "status_label"):
                self.status_label.setText(reason)

    @staticmethod
    def _is_deleted_qt_wrapper_error(exc: Exception) -> bool:
        """Detect common PySide wrapper-lifetime failures."""
        if not isinstance(exc, RuntimeError):
            return False
        msg = str(exc).lower()
        return "deleted" in msg and (
            "wrapped c/c++ object" in msg
            or "internal c++ object" in msg
            or "c++ object" in msg
        )

    def _is_everything_check_active(self) -> bool:
        """
        Whether Everything check ownership is currently active.
        Includes stale-owner watchdog so missing check_done cannot stall checks forever.
        """
        worker = self.everything_check_worker
        if worker is None:
            self._everything_check_owner_since = None
            return False

        if self._everything_check_owner_since is None:
            self._everything_check_owner_since = time.monotonic()

        try:
            if hasattr(worker, "isRunning") and worker.isRunning():
                return True
        except Exception as e:
            if self._is_deleted_qt_wrapper_error(e):
                self._clear_everything_check_ownership(
                    "Recovered from deleted Everything worker wrapper"
                )
                return False
            return True

        elapsed = time.monotonic() - self._everything_check_owner_since
        if elapsed < self._everything_check_stale_grace_seconds:
            return True

        self._clear_everything_check_ownership(
            "Recovered from stale Everything worker ownership"
        )
        return False

    def _is_download_queue_active(self) -> bool:
        """
        Central gate to prevent row/state mutations while download queue
        ownership is active.
        Includes stale-owner watchdog so missing queue_done cannot block UI forever.
        """
        worker = self.download_worker
        if worker is None:
            self._download_queue_owner_since = None
            return False

        if self._download_queue_owner_since is None:
            self._download_queue_owner_since = time.monotonic()

        try:
            if hasattr(worker, "isRunning") and worker.isRunning():
                return True
        except Exception as e:
            if self._is_deleted_qt_wrapper_error(e):
                self._clear_download_queue_ownership(
                    "Recovered from deleted download worker wrapper"
                )
                return False
            return True

        elapsed = time.monotonic() - self._download_queue_owner_since
        if elapsed < self._download_queue_stale_grace_seconds:
            return True

        self._clear_download_queue_ownership(
            "Recovered from stale download queue ownership"
        )
        return False

    def _block_if_shutting_down(self) -> bool:
        """Return True when new actions should be rejected during close retries."""
        if not self._shutdown_in_progress:
            return False
        self.status_label.setText(
            "Shutdown in progress, waiting for background tasks to stop..."
        )
        return True

    @safe_slot
    def start_search(self):
        """Initiate a new search"""
        if self._block_if_shutting_down():
            return
        query = self.query_input.text().strip()
        if not query:
            self.status_label.setText("Please enter a search query")
            return

        # Keep table/model identity stable until the queue finishes processing items.
        if self._is_download_queue_active():
            self.status_label.setText(
                "Cannot start a new search while downloads are running"
            )
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
        self._completer_model(self.completer).setStringList(self.search_history)

        scope = self._resolve_search_scope()
        if scope is None:
            return
        indexer_ids, categories = scope

        if indexer_ids is None:
            indexer_info = "all"
        elif indexer_ids:
            indexer_info = f"{len(indexer_ids)} selected: {indexer_ids}"
        else:
            indexer_info = "none"

        if categories is None:
            category_info = "all"
        elif categories:
            category_info = f"{len(categories)} selected: {categories}"
        else:
            category_info = "none"
        self.log(
            f"Starting search: query='{query}', "
            f"page_size={self.prowlarr_page_size}, "
            f"indexers={indexer_info}, categories={category_info}"
        )

        # Disable download buttons and reset progress bar
        self.download_selected_btn.setEnabled(False)
        self.download_all_btn.setEnabled(False)
        self.download_progress.setMaximum(1)
        self.download_progress.setValue(0)

        # Clear previous results
        self.results_table.setRowCount(0)
        self._acquire_table_sort_lock("search")
        self.current_results = []
        self.current_offset = 0
        self._video_paths = {}
        self._downloaded_release_keys = set()
        self._search_generation += 1
        self._pending_everything_recheck = None

        # Reset page number to 1 for new search
        self.prowlarr_page_number_spinbox.blockSignals(True)
        self.prowlarr_page_number_spinbox.setValue(1)
        self.prowlarr_page_number_spinbox.blockSignals(False)

        # Create and start worker thread
        client = self.prowlarr

        try:
            self.current_worker = SearchWorker(
                client,
                query,
                indexer_ids,
                categories,
                0,
                self.prowlarr_page_size,
            )
        except Exception as e:
            logger.error(f"Failed to create search worker: {e}")
            self.current_worker = None
            self._release_table_sort_lock("search")
            self.status_label.setText(f"Failed to start search: {e}")
            return
        self._track_worker(self.current_worker)
        self.current_worker.search_done.connect(
            self._search_done_callback(self.current_worker)
        )
        self.current_worker.error.connect(
            self._search_error_callback(self.current_worker)
        )
        self.current_worker.progress.connect(
            self._search_progress_callback(self.current_worker)
        )
        try:
            self.current_worker.start()
        except Exception as e:
            logger.error(f"Failed to start search worker: {e}")
            self.current_worker = None
            self._release_table_sort_lock("search")
            self.status_label.setText(f"Failed to start search: {e}")
            return

        # Update UI state
        self.search_btn.setEnabled(False)
        self.load_all_btn.setEnabled(False)
        self.start_spinner("search")

    @safe_slot
    def start_load_all_pages(self):
        """Start fetching all pages of results sequentially"""
        if self._block_if_shutting_down():
            return
        # Prevent Load All from clearing or replacing rows while the
        # download queue is in flight.
        if self._is_download_queue_active():
            self.status_label.setText("Cannot load pages while downloads are running")
            return

        # If Load All is active, cancel it
        if self._load_all_active:
            self._load_all_active = False
            if self.current_worker and self.current_worker.isRunning():
                try:
                    self.current_worker.requestInterruption()
                except Exception as e:
                    logger.debug(f"Failed to interrupt Load All worker: {e}")
            # Prevent stale worker ownership if a replacement search starts
            # immediately.
            self.current_worker = None
            self.search_btn.setEnabled(True)
            self.load_all_btn.setText("Load A&ll")
            self.load_all_btn.setEnabled(True)
            self._release_table_sort_lock("search")
            self.stop_spinner("search")
            self.log("Load All: cancelled by user")
            self.status_label.setText("Load All cancelled")
            if self._load_all_results:
                self.current_results = list(self._load_all_results)
                self._load_all_results = []
                self.display_results(self.current_results)
                self.apply_default_sort()
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
        self._completer_model(self.completer).setStringList(self.search_history)

        scope = self._resolve_search_scope()
        if scope is None:
            return

        # Initialize multi-page state
        self._load_all_active = True
        self._load_all_results = []
        self._load_all_page = 1

        # Clear previous results
        self.results_table.setRowCount(0)
        self._acquire_table_sort_lock("search")
        self.current_results = []
        self.current_offset = 0
        self._video_paths = {}
        self._downloaded_release_keys = set()
        self._search_generation += 1
        self._pending_everything_recheck = None

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
        if self._block_if_shutting_down():
            self._load_all_active = False
            self.search_btn.setEnabled(True)
            self.load_all_btn.setText("Load A&ll")
            self.load_all_btn.setEnabled(True)
            self._release_table_sort_lock("search")
            self.stop_spinner("search")
            return
        query = self.query_input.text().strip()
        scope = self._resolve_search_scope()
        if scope is None:
            self._load_all_active = False
            self.search_btn.setEnabled(True)
            self.load_all_btn.setText("Load A&ll")
            self.load_all_btn.setEnabled(True)
            self._release_table_sort_lock("search")
            self.stop_spinner("search")
            return
        indexer_ids, categories = scope
        offset = (self._load_all_page - 1) * self.prowlarr_page_size

        self.status_label.setText(f"Loading page {self._load_all_page}...")
        client = self.prowlarr
        if client is None:
            self._load_all_active = False
            self.search_btn.setEnabled(True)
            self.load_all_btn.setText("Load A&ll")
            self.load_all_btn.setEnabled(True)
            self._release_table_sort_lock("search")
            self.status_label.setText("Prowlarr client not initialized")
            return

        try:
            self.current_worker = SearchWorker(
                client,
                query,
                indexer_ids,
                categories,
                offset,
                self.prowlarr_page_size,
            )
        except Exception as e:
            logger.error(f"Failed to create load-all worker: {e}")
            self.current_worker = None
            self._load_all_active = False
            self.search_btn.setEnabled(True)
            self.load_all_btn.setText("Load A&ll")
            self.load_all_btn.setEnabled(True)
            self._release_table_sort_lock("search")
            self.status_label.setText(f"Failed to load page: {e}")
            return
        self._track_worker(self.current_worker)
        self.current_worker.search_done.connect(
            self._search_done_callback(self.current_worker)
        )
        self.current_worker.error.connect(
            self._search_error_callback(self.current_worker)
        )
        self.current_worker.progress.connect(
            self._search_progress_callback(self.current_worker)
        )
        try:
            self.current_worker.start()
        except Exception as e:
            logger.error(f"Failed to start load-all worker: {e}")
            self.current_worker = None
            self._load_all_active = False
            self.search_btn.setEnabled(True)
            self.load_all_btn.setText("Load A&ll")
            self.load_all_btn.setEnabled(True)
            self._release_table_sort_lock("search")
            self.status_label.setText(f"Failed to load page: {e}")
            return

        self.search_btn.setEnabled(False)

        self.start_spinner("search")

    def fetch_page(self, page_number: int):
        """Fetch a specific page of results"""
        if self._block_if_shutting_down():
            return
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

        scope = self._resolve_search_scope()
        if scope is None:
            return
        indexer_ids, categories = scope

        # Calculate offset based on page number
        offset = (page_number - 1) * self.prowlarr_page_size

        self.log(
            f"Fetching page {page_number} "
            f"(offset: {offset}, page size: {self.prowlarr_page_size})"
        )

        # Clear previous results
        self.results_table.setRowCount(0)
        self._acquire_table_sort_lock("search")
        self.current_results = []
        self.current_offset = offset
        self._video_paths = {}
        self._search_generation += 1
        self._pending_everything_recheck = None

        # Create and start worker thread
        client = self.prowlarr

        try:
            self.current_worker = SearchWorker(
                client,
                query,
                indexer_ids,
                categories,
                offset,
                self.prowlarr_page_size,
            )
        except Exception as e:
            logger.error(f"Failed to create fetch worker: {e}")
            self.current_worker = None
            self._release_table_sort_lock("search")
            self.status_label.setText(f"Failed to fetch page: {e}")
            return
        self._track_worker(self.current_worker)
        self.current_worker.search_done.connect(
            self._search_done_callback(self.current_worker)
        )
        self.current_worker.error.connect(
            self._search_error_callback(self.current_worker)
        )
        self.current_worker.progress.connect(
            self._search_progress_callback(self.current_worker)
        )
        try:
            self.current_worker.start()
        except Exception as e:
            logger.error(f"Failed to start fetch worker: {e}")
            self.current_worker = None
            self._release_table_sort_lock("search")
            self.status_label.setText(f"Failed to fetch page: {e}")
            return

        # Update UI state
        self.search_btn.setEnabled(False)

        self.start_spinner("search")

    def _track_worker(self, worker: WorkerThread | None) -> None:
        """Track a worker for cleanup on app close"""

        def _safe_is_running(w: WorkerThread | None) -> bool:
            try:
                return bool(w and hasattr(w, "isRunning") and w.isRunning())
            except Exception:
                return False

        if worker and worker not in self._all_workers:
            # Prune finished workers to prevent memory leak
            self._all_workers = [w for w in self._all_workers if _safe_is_running(w)]
            self._all_workers.append(worker)

    @staticmethod
    def _download_worker_running(worker: DownloadWorker | None) -> bool:
        """Return True when the download worker is alive and accepting commands."""
        try:
            return bool(worker and hasattr(worker, "isRunning") and worker.isRunning())
        except Exception:
            return False

    def _normalize_download_queue_items(
        self,
        items: list[ReleaseDict],
    ) -> list[ReleaseDict]:
        """Deduplicate queue items by stable release identity."""
        deduped_items: list[ReleaseDict] = []
        seen_keys: set[ReleaseKey] = set()
        for item in items:
            key = (
                self._text_value(item.get("guid", "")),
                self._int_value(item.get("indexer_id"), -1),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped_items.append(item)
        return deduped_items

    def _schedule_download_queue_retry(
        self,
        items: list[ReleaseDict],
        retry_attempt: int,
        reason: str,
    ) -> None:
        """Retry queue startup with bounded backoff while shutdown completes."""
        next_attempt = retry_attempt + 1
        if next_attempt > self._download_queue_retry_limit:
            if not self._download_worker_running(self.download_worker):
                self.log(
                    "Download queue owner appears stale; resetting ownership and "
                    "retrying enqueue"
                )
                self._clear_download_queue_ownership()
                self.start_download_queue(list(items), 0)
                return
            self.log(
                "Download queue retry limit reached while worker is still shutting down"
            )
            self.status_label.setText(
                "Queue is busy shutting down; please retry in a moment"
            )
            return

        delay_ms = min(1000, 100 * (2 ** min(retry_attempt, 3)))
        self.log(
            f"{reason} (retry {next_attempt}/{self._download_queue_retry_limit} "
            f"in {delay_ms}ms)"
        )
        self.status_label.setText("Queue finishing, retrying enqueue...")
        pending_items = list(items)

        def retry_pending_queue() -> None:
            self.start_download_queue(pending_items, next_attempt)

        self._schedule_timer(delay_ms, retry_pending_queue)

    def _map_release_items_to_rows(self, items: list[ReleaseDict]) -> None:
        """Update stable release-key to row mappings for queue operations."""
        for item in items:
            item_guid = self._text_value(item.get("guid", ""), "")
            item_indexer_id = self._int_value(item.get("indexer_id"), -1)
            for row in range(self.results_table.rowCount()):
                btn = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
                if (
                    btn
                    and self._text_value(btn.property("guid"), "") == item_guid
                    and self._int_value(btn.property("indexerId"), -1)
                    == item_indexer_id
                ):
                    self._release_key_to_row[(item_guid, item_indexer_id)] = row
                    break

    def _append_items_to_download_queue(
        self,
        items: list[ReleaseDict],
        retry_attempt: int,
    ) -> bool:
        """Append items to an active queue worker when possible."""
        if self.download_worker is None:
            return False
        add_items = getattr(self.download_worker, "add_items", None)
        if not callable(add_items):
            self._schedule_download_queue_retry(
                items,
                retry_attempt,
                "Download worker is not ready to accept new items yet",
            )
            return True
        queue_add_items = cast(
            "Callable[[list[ReleaseDict]], list[ReleaseDict] | None]",
            add_items,
        )

        try:
            added_items = queue_add_items(items)
        except Exception as e:
            logger.warning(f"Download queue add_items failed: {e}")
            if self._is_deleted_qt_wrapper_error(
                e
            ) or not self._download_worker_running(self.download_worker):
                self.log(
                    "Download queue owner became invalid; resetting ownership and "
                    "retrying enqueue"
                )
                self._clear_download_queue_ownership()
                self.start_download_queue(list(items), 0)
                return True
            self._schedule_download_queue_retry(
                items,
                retry_attempt,
                "Download queue enqueue failed while worker is active",
            )
            return True
        if added_items is None:
            self._schedule_download_queue_retry(
                items,
                retry_attempt,
                "Download queue is finishing",
            )
            return True
        added = len(added_items)
        if added == 0:
            self.status_label.setText("All selected items are already queued")
            return True

        self.download_progress.setMaximum(self.download_progress.maximum() + added)
        self._map_release_items_to_rows(added_items)
        self.log(f"Added {added} item(s) to download queue")
        self.status_label.setText(f"Added {added} item(s) to download queue")
        return True

    def _prepare_new_download_queue(self, items: list[ReleaseDict]) -> int:
        """Prepare UI state and row mappings for a fresh download queue worker."""
        total = len(items)
        self.log(f"Starting download queue: {total} item(s)")
        self.download_progress.setMaximum(total)
        self.download_progress.setValue(0)
        self._acquire_table_sort_lock("download")
        self.search_btn.setEnabled(False)
        self._downloaded_title_keys = set()
        self._release_key_to_row = {}
        self._map_release_items_to_rows(items)
        return total

    def _create_download_worker(
        self,
        client: ProwlarrClient,
        items: list[ReleaseDict],
    ) -> DownloadWorker | None:
        """Create, wire, and start a download queue worker."""
        try:
            worker = DownloadWorker(client, items)
        except Exception as e:
            logger.error(f"Failed to create download worker: {e}")
            self._clear_download_queue_ownership()
            self.status_label.setText(f"Failed to start downloads: {e}")
            return None
        self.download_worker = worker
        self._download_queue_owner_since = time.monotonic()
        self._track_worker(worker)
        worker.progress.connect(self._download_progress_callback(worker))
        worker.item_downloaded.connect(self._download_item_callback(worker))
        worker.queue_done.connect(self._download_done_callback(worker))
        try:
            worker.start()
        except Exception as e:
            logger.error(f"Failed to start download worker: {e}")
            self._clear_download_queue_ownership()
            self.status_label.setText(f"Failed to start downloads: {e}")
            return None
        return worker

    def _collect_recheck_results(
        self,
        title_keys: set[str],
    ) -> list[ReleaseDict]:
        """Collect lightweight Everything recheck payloads for matching titles."""
        recheck_results: list[ReleaseDict] = []
        for check_row in range(self.results_table.rowCount()):
            check_title_item = self.results_table.item(check_row, self.COL_TITLE)
            if not check_title_item:
                continue
            check_title = check_title_item.text()
            check_title_key = check_title[: self.title_match_chars].lower()
            if check_title_key in title_keys:
                recheck_results.append({"title": check_title})
        return recheck_results

    def _recheck_title_map(
        self,
        recheck_results: list[ReleaseDict],
    ) -> dict[int, str]:
        """Map worker row indexes back to release titles for recheck batches."""
        return {
            index: self._text_value(result["title"], "Unknown")
            for index, result in enumerate(recheck_results)
        }

    def _snapshot_title_rows(self) -> dict[str, list[int]]:
        """Snapshot current table rows grouped by full title text."""
        title_to_rows: dict[str, list[int]] = {}
        for row in range(self.results_table.rowCount()):
            item = self.results_table.item(row, self.COL_TITLE)
            if not item:
                continue
            title_to_rows.setdefault(item.text(), []).append(row)
        return title_to_rows

    def _remap_recheck_batch(
        self,
        batch: EverythingBatch,
        recheck_title_map: dict[int, str],
        title_to_rows: dict[str, list[int]],
        sender_worker: EverythingCheckWorker,
    ) -> EverythingBatch:
        """Remap Everything worker row indexes back to live table rows."""
        if sender_worker is not self.everything_check_worker:
            return []
        remapped: EverythingBatch = []
        seen_rows: set[int] = set()
        for idx, results in batch:
            title = recheck_title_map.get(idx)
            if title is None:
                logger.warning(f"Recheck batch index {idx} not in title map")
                continue
            for row in title_to_rows.get(title, []):
                if row in seen_rows:
                    continue
                remapped.append((row, results))
                seen_rows.add(row)
        return remapped

    def _create_recheck_worker(
        self,
        recheck_results: list[ReleaseDict],
    ) -> EverythingCheckWorker | None:
        """Create and register an Everything recheck worker for the given rows."""
        everything = self.everything
        if everything is None:
            self._release_table_sort_lock("everything")
            return None
        try:
            worker = EverythingCheckWorker(
                everything,
                recheck_results,
                self.title_match_chars,
                self.everything_search_chars,
                self.everything_batch_size,
            )
        except Exception as e:
            logger.error(f"Failed to create recheck worker: {e}")
            self._release_table_sort_lock("everything")
            return None
        self.everything_check_worker = worker
        self._everything_check_owner_since = time.monotonic()
        self._track_worker(worker)
        return worker

    def _request_worker_interrupt(
        self,
        worker: WorkerThread | None,
        name: str,
    ) -> None:
        """Request cooperative interruption for a tracked worker."""
        if not worker:
            return
        try:
            worker.requestInterruption()
        except Exception as e:
            logger.debug(f"Failed to request interruption for {name}: {e}")

    @staticmethod
    def _worker_is_running(worker: WorkerThread | None) -> bool:
        """Return True when the worker still reports a running thread."""
        try:
            return bool(worker and worker.isRunning())
        except Exception:
            return False

    def _force_stop_worker(self, worker: WorkerThread | None, name: str) -> None:
        """Force-stop a worker after cooperative interruption failed."""
        if not worker:
            return
        self._request_worker_interrupt(worker, name)
        try:
            worker.terminate()
        except Exception as e:
            logger.debug(f"Failed to terminate {name}: {e}")
        try:
            worker.wait(250)
        except Exception as e:
            logger.debug(f"Failed forced wait for {name}: {e}")

    def _tracked_named_workers(self) -> list[tuple[str, WorkerThread | None]]:
        """Return the named worker fields that participate in close handling."""
        return [
            ("InitWorker", self.init_worker),
            ("SearchWorker", self.current_worker),
            ("EverythingCheckWorker", self.everything_check_worker),
            ("DownloadWorker", self.download_worker),
        ]

    def _interrupt_worker_once_if_running(
        self,
        worker: WorkerThread | None,
        name: str,
    ) -> None:
        """Request interruption once per worker identity during shutdown."""
        if not self._worker_is_running(worker):
            return
        worker_id = id(worker)
        if worker_id in self._shutdown_interrupted_worker_ids:
            return
        self._request_worker_interrupt(worker, name)
        self._shutdown_interrupted_worker_ids.add(worker_id)

    def _interrupt_all_tracked_workers(
        self,
        tracked_named_workers: list[tuple[str, WorkerThread | None]],
    ) -> None:
        """Interrupt named workers and additional tracked workers."""
        for name, worker in tracked_named_workers:
            self._interrupt_worker_once_if_running(worker, name)
        for worker in self._all_workers:
            self._interrupt_worker_once_if_running(worker, type(worker).__name__)

    def _collect_running_workers_for_close(
        self,
        tracked_named_workers: list[tuple[str, WorkerThread | None]],
        *,
        wait_ms: int,
    ) -> list[tuple[str, WorkerThread]]:
        """Collect currently running workers after an optional lightweight wait."""
        seen_workers: set[int] = set()
        still_running: list[tuple[str, WorkerThread]] = []
        for name, worker in tracked_named_workers:
            if not worker:
                continue
            seen_workers.add(id(worker))
            if wait_ms > 0:
                try:
                    worker.wait(wait_ms)
                except Exception as e:
                    logger.debug(f"Failed lightweight wait for {name}: {e}")
            if self._worker_is_running(worker):
                still_running.append((name, worker))
        for worker in self._all_workers:
            if not worker or id(worker) in seen_workers:
                continue
            if wait_ms > 0:
                try:
                    worker.wait(wait_ms)
                except Exception as e:
                    logger.debug(f"Failed lightweight wait for tracked worker: {e}")
            if self._worker_is_running(worker):
                still_running.append((type(worker).__name__, worker))
        return still_running

    def _handle_force_close_workers(
        self,
        still_running: list[tuple[str, WorkerThread]],
        event: QCloseEvent,
    ) -> bool:
        """Try force-stopping workers and return True when close must abort."""
        unresolved: list[str] = []
        force_seen: set[int] = set()
        for name, worker in still_running:
            worker_id = id(worker)
            if worker_id in force_seen:
                continue
            force_seen.add(worker_id)
            self._force_stop_worker(worker, name)
            if self._worker_is_running(worker):
                unresolved.append(name)

        unresolved = sorted(set(unresolved))
        if not unresolved:
            return False
        force_msg = (
            "Close aborted: workers still running after force-stop attempt: "
            + ", ".join(unresolved)
        )
        logger.error(force_msg)
        if hasattr(self, "status_label"):
            self.status_label.setText(force_msg)
        self._shutdown_in_progress = False
        self._shutdown_started_monotonic = None
        self._shutdown_force_prompted = False
        self._shutdown_force_armed_until = None
        self._shutdown_interrupted_worker_ids.clear()
        self.stop_spinner("shutdown")
        self._cancel_close_retry_timer()
        event.ignore()
        return True

    def _handle_graceful_close_wait(
        self,
        event: QCloseEvent,
    ) -> bool:
        """Handle the graceful close retry/force-arm flow."""
        self._shutdown_in_progress = True
        if "shutdown" not in self._active_spinner_tags:
            self.start_spinner("shutdown")
        elapsed = 0.0
        if self._shutdown_started_monotonic is not None:
            elapsed = max(0.0, time.monotonic() - self._shutdown_started_monotonic)
        if elapsed >= self._shutdown_force_after_seconds:
            arm_seconds = max(1.0, self._shutdown_force_arm_seconds)
            self._shutdown_force_prompted = True
            self._shutdown_force_armed_until = time.monotonic() + arm_seconds
            prompt = (
                "Background tasks did not stop after "
                f"{self._shutdown_force_after_seconds:.0f}s. "
                f"Close again within {arm_seconds:.0f}s to force stop."
            )
            logger.error(prompt)
            if hasattr(self, "status_label"):
                self.status_label.setText(prompt)
            self._shutdown_in_progress = False
            self._shutdown_started_monotonic = None
            self._shutdown_interrupted_worker_ids.clear()
            self.stop_spinner("shutdown")
            self._cancel_close_retry_timer()
            event.ignore()
            return True

        if not self._close_retry_pending:
            self._close_retry_pending = True
            self._close_retry_timer = self._schedule_timer(250, self._retry_close)
        event.ignore()
        return True

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
            self.status_label.setText(f"ERROR: Failed to save configuration: {e}")
            return False

    def _schedule_config_save(self, delay_ms: int = 300):
        """Mark config dirty and debounce disk writes to keep UI responsive."""
        self._config_dirty = True
        self.config_save_timer.start(delay_ms)

    def _flush_config_save(self):
        """Write pending config changes and/or sync INI preferences."""
        if self._config_dirty:
            try:
                save_config(self.config)
                self._config_dirty = False
            except Exception as e:
                logger.error(f"Failed to save config: {e}")
                if hasattr(self, "status_label"):
                    self.status_label.setText(
                        f"ERROR: Failed to save configuration: {e}"
                    )
        if self._prefs_dirty:
            self._sync_preferences()

    def _schedule_timer(self, delay_ms: int, callback: Callable[[], None]) -> QTimer:
        """
        Schedule a QTimer.singleShot with tracking for cleanup
        Prevents timers from firing after window closes
        """
        timer = QTimer()
        timer.setSingleShot(True)
        self._pending_timers.append(timer)

        # Wrap callback to auto-cleanup timer from tracking list
        def cleanup_wrapper() -> None:
            try:
                callback()
            except Exception:
                logger.error(
                    f"Exception in scheduled timer callback: {traceback.format_exc()}"
                )
            finally:
                if timer in self._pending_timers:
                    self._pending_timers.remove(timer)

        timer.timeout.connect(cleanup_wrapper)
        timer.start(delay_ms)
        return timer

    def _wait_worker(
        self, worker: WorkerThread | None, name: str, timeout_ms: int = 3000
    ) -> bool:
        """Wait for a worker thread to finish with cooperative cancellation first."""
        if not worker:
            return True
        try:
            # First ask the worker to stop cooperatively.
            if hasattr(worker, "requestInterruption"):
                worker.requestInterruption()
        except Exception as e:
            logger.debug(f"Failed to request interruption for {name}: {e}")

        try:
            if worker.wait(timeout_ms):
                return True
        except Exception as e:
            logger.error(f"Failed waiting for {name}: {e}")
            return False

        logger.warning(
            f"{name} did not stop within {timeout_ms}ms "
            "(cooperative cancellation timed out)"
        )
        return False

    def _search_done_callback(
        self, worker: SearchWorker
    ) -> Callable[[list[ReleaseDict], float], None]:
        """Bind one search worker completion signal to the current page handler."""

        def handle(results: list[ReleaseDict], elapsed: float) -> None:
            self.page_fetch_finished(results, elapsed, worker)

        return handle

    def _search_error_callback(self, worker: SearchWorker) -> Callable[[str], None]:
        """Bind one search worker error signal to the current error handler."""

        def handle(error: str) -> None:
            self.search_error(error, worker)

        return handle

    def _search_progress_callback(self, worker: SearchWorker) -> Callable[[str], None]:
        """Bind one search worker progress signal to the status updater."""

        def handle(message: str) -> None:
            self.on_search_progress(message, worker)

        return handle

    def _everything_batch_callback(
        self, worker: EverythingCheckWorker
    ) -> Callable[[EverythingBatch], None]:
        """Bind one Everything batch signal to the main-thread update handler."""

        def handle(batch: EverythingBatch) -> None:
            self.on_everything_batch_ready(batch, worker)

        return handle

    def _everything_done_callback(
        self, worker: EverythingCheckWorker
    ) -> Callable[[], None]:
        """Bind one Everything completion signal to the ownership clearer."""

        def handle() -> None:
            self.on_everything_check_finished(worker)

        return handle

    def _everything_progress_callback(
        self, worker: EverythingCheckWorker
    ) -> Callable[[int, int], None]:
        """Bind one Everything progress signal to the status updater."""

        def handle(checked: int, total: int) -> None:
            self._on_everything_progress(checked, total, worker)

        return handle

    def _download_progress_callback(
        self, worker: DownloadWorker
    ) -> Callable[[int, int, str], None]:
        """Bind one download queue progress signal to the row updater."""

        def handle(current: int, total: int, title: str) -> None:
            self.on_download_progress(current, total, title, worker)

        return handle

    def _download_item_callback(
        self, worker: DownloadWorker
    ) -> Callable[[str, int, bool], None]:
        """Bind one item-completion signal to the download result handler."""

        def handle(guid: str, indexer_id: int, success: bool) -> None:
            self.on_item_downloaded(guid, indexer_id, success, worker)

        return handle

    def _download_done_callback(self, worker: DownloadWorker) -> Callable[[], None]:
        """Bind one queue completion signal to the queue cleanup handler."""

        def handle() -> None:
            self.on_download_queue_finished(worker)

        return handle

    @safe_slot
    def page_fetch_finished(
        self,
        results: list[ReleaseDict],
        elapsed: float = 0.0,
        worker: SearchWorker | None = None,
    ) -> None:
        """Handle completed page fetch - loads only one page at a time"""
        if worker is not None and worker is not self.current_worker:
            return
        self.current_worker = None

        # Multi-page "Load All" mode: accumulate results and fetch next page
        if self._load_all_active:
            self._load_all_results.extend(results)
            self.log(
                f"Load All: page {self._load_all_page} returned {len(results)} "
                f"results (total: {len(self._load_all_results)})"
            )
            self.status_label.setText(
                f"Loading... page {self._load_all_page}, "
                f"{len(self._load_all_results)} results so far"
            )

            if len(results) >= self.prowlarr_page_size:
                # More pages available, fetch next
                self._load_all_page += 1
                self.prowlarr_page_number_spinbox.blockSignals(True)
                self.prowlarr_page_number_spinbox.setValue(self._load_all_page)
                self.prowlarr_page_number_spinbox.blockSignals(False)
                # Balance the page-level search spinner before starting the next fetch.
                self.stop_spinner("search")
                self._load_all_fetch_page()
                return
            else:
                # Last page reached, display all accumulated results
                self._load_all_active = False
                results = self._load_all_results
                self._load_all_results = []
                self.log(
                    f"Load All: complete, {len(results)} total results "
                    f"across {self._load_all_page} pages"
                )
        else:
            # Cancelled or single-page: show accumulated results if any
            if self._load_all_results:
                self._load_all_results.extend(results)
                results = self._load_all_results
                self._load_all_results = []
                self.log(
                    f"Load All: cancelled, showing {len(results)} results "
                    f"from {self._load_all_page} pages"
                )

        self.search_btn.setEnabled(True)
        self.load_all_btn.setText("Load A&ll")
        self.load_all_btn.setEnabled(True)
        self._release_table_sort_lock("search")

        self.stop_spinner("search")

        # Replace results with new page
        self.current_results = results

        self.log(
            f"Page {self.prowlarr_page_number_spinbox.value()}: "
            f"Received {len(results)} results in {elapsed:.1f}s"
        )
        self.display_results(results)
        self._restore_column_widths()

        # Build per-indexer stats
        indexer_counts: dict[str, int] = {}
        for r in results:
            name = self._text_value(r.get("indexer", "Unknown"), "Unknown")
            indexer_counts[name] = indexer_counts.get(name, 0) + 1
        if indexer_counts:
            parts = [
                f"{name}: {count}" for name, count in sorted(indexer_counts.items())
            ]
            indexer_summary = " | ".join(parts)
            self.log(f"Indexer stats: {indexer_summary}")
            self.status_label.setText(
                f"{len(results)} results in {elapsed:.1f}s  [{indexer_summary}]"
            )
        else:
            self.status_label.setText(f"No results ({elapsed:.1f}s)")

        # Update download button states based on visible rows and selection
        self.update_download_button_states()

        # Apply custom multi-column sort
        self.apply_default_sort()

    @safe_slot
    def search_error(self, error: str, worker: SearchWorker | None = None) -> None:
        """Handle search error"""
        if worker is not None and worker is not self.current_worker:
            return
        self.current_worker = None
        self.search_btn.setEnabled(True)
        self.load_all_btn.setText("Load A&ll")
        self.load_all_btn.setEnabled(True)
        self._release_table_sort_lock("search")

        self.stop_spinner("search")

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
    def on_search_progress(
        self, message: str, worker: SearchWorker | None = None
    ) -> None:
        """Update status for active search worker only."""
        if worker is not None and worker is not self.current_worker:
            return
        self.update_status(message)

    @safe_slot
    def apply_default_sort(self) -> None:
        """
        Apply custom multi-column sort: Title ASC, then Indexer DESC, then Age ASC
        Sorts the current_results and redisplays the table
        """
        if self._block_if_shutting_down():
            return

        if self._is_download_queue_active():
            self.status_label.setText(
                "Cannot reset sorting while downloads are running"
            )
            return

        if not self.current_results:
            return

        self.log("Applying default sort: Title ASC, then Indexer DESC, then Age ASC...")

        # Sort using cached tuple keys (avoids redundant .lower() calls)
        # Key: (title_lower ASC, indexer_lower_inverted DESC, age ASC)
        def sort_key(r: ReleaseDict) -> tuple[str, list[int], int]:
            title = self._text_value(r.get("title", ""), "")
            indexer = self._text_value(r.get("indexer", ""), "")
            return (
                title.lower(),
                # Invert string for descending: negate each char ordinal
                [-ord(c) for c in indexer.lower()],
                # Keep age positive so smaller day-counts sort first (true ASC).
                self._int_value(r.get("age", 0), 0),
            )

        self.current_results.sort(key=sort_key)

        # Clear and redisplay under a temporary render lock.
        self._acquire_table_sort_lock("render")
        try:
            self.results_table.setRowCount(0)
            self.display_results(self.current_results)
            # Clear sort indicator before lock release to prevent implicit auto-resort.
            self.results_table.horizontalHeader().setSortIndicator(
                -1, Qt.SortOrder.AscendingOrder
            )
        finally:
            self._release_table_sort_lock("render")

        self._update_status_bar_counts()

        # Start Everything check in background
        self.start_everything_check()

    def start_everything_check(self):
        """Start background Everything checking for all results"""
        if self._shutdown_in_progress:
            return
        # Skip if Everything not initialized or no results
        if not self.everything or not self.current_results:
            return

        # Treat worker ownership as active until check_done clears
        # everything_check_worker.
        if self._is_everything_check_active():
            if self._everything_check_generation != self._search_generation:
                self._pending_everything_check_generation = self._search_generation
            return

        # We are starting now, so clear any stale deferred request marker.
        self._pending_everything_check_generation = None

        # Create and start new worker (tag with generation for stale batch detection)
        self._everything_check_generation = self._search_generation
        results_snapshot = list(self.current_results)
        try:
            worker = EverythingCheckWorker(
                self.everything,
                results_snapshot,
                self.title_match_chars,
                self.everything_search_chars,
                self.everything_batch_size,
            )
        except Exception as e:
            logger.error(f"Failed to create Everything check worker: {e}")
            self.status_label.setText(f"Failed to start Everything check: {e}")
            return
        self.everything_check_worker = worker
        self._everything_check_owner_since = time.monotonic()
        self._track_worker(worker)
        worker.batch_ready.connect(self._everything_batch_callback(worker))
        worker.check_done.connect(self._everything_done_callback(worker))
        worker.progress.connect(self._everything_progress_callback(worker))
        self._acquire_table_sort_lock("everything")
        self.start_spinner("everything")

        try:
            worker.start()
        except Exception as e:
            logger.error(f"Failed to start Everything check worker: {e}")
            self._clear_everything_check_ownership()
            return
        logger.info(f"Started Everything check for {len(self.current_results)} results")

    @safe_slot
    def on_everything_batch_ready(
        self,
        batch: EverythingBatch,
        worker: EverythingCheckWorker | None = None,
    ) -> None:
        """Process a batch of Everything check results"""
        if worker is not None and worker is not self.everything_check_worker:
            return
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

            # Set tooltip with found results (FileName - Size), limited to
            # the configured maximum.
            # Get release size for comparison
            size_item = self.results_table.item(row, self.COL_SIZE)
            release_size = (
                self._int_value(size_item.data(Qt.ItemDataRole.UserRole), 0)
                if size_item
                else 0
            )

            tooltip_lines = [
                f"Found in Everything (release: {format_size(release_size)}):"
            ]
            for filename, size in everything_results[: self.everything_max_results]:
                if os.path.isdir(filename):
                    tooltip_lines.append(f"  {filename}")
                else:
                    size_str = format_size(size)
                    tooltip_lines.append(f"  {filename} - {size_str}")

            title_item.setToolTip("\n".join(tooltip_lines))

            # Store video file path keyed by stable release identity
            try:
                video = self._find_video_file(everything_results)
                if video:
                    release_key = self._get_release_key_for_row(row)
                    if release_key:
                        self._video_paths[release_key] = video
            except Exception as e:
                self.log(f"ERROR in _find_video_file: {e}")

            # Hide this row if "Hide existing" is checked
            if hide:
                self.results_table.setRowHidden(row, True)

    @safe_slot
    def _on_everything_progress(
        self,
        checked: int,
        total: int,
        worker: EverythingCheckWorker | None = None,
    ) -> None:
        """Update status bar with Everything check progress"""
        if worker is not None and worker is not self.everything_check_worker:
            return
        if self._everything_check_generation != self._search_generation:
            return
        self.status_label.setText(f"Checking Everything: {checked}/{total}")

    @safe_slot
    def on_everything_check_finished(
        self, worker: EverythingCheckWorker | None = None
    ) -> None:
        """Cleanup when Everything check completes"""
        if worker is not None and worker is not self.everything_check_worker:
            return

        finished_generation = self._everything_check_generation
        self.everything_check_worker = None
        self._everything_check_owner_since = None
        self._release_table_sort_lock("everything")

        self.stop_spinner("everything")
        logger.info("Everything check completed")
        # Re-apply all filters only when this completion belongs to current results.
        if finished_generation == self._search_generation:
            self.apply_result_filters()

        # If a newer generation requested a check while this worker was
        # active, run it now.
        pending_gen = self._pending_everything_check_generation
        if pending_gen is not None:
            self._pending_everything_check_generation = None
            if pending_gen == self._search_generation:
                self.start_everything_check()
                return

        # Replay deferred targeted rechecks once the in-flight worker is done.
        self._run_deferred_everything_recheck()

    def _queue_deferred_everything_recheck(
        self, title_keys: set[str], generation: int
    ) -> None:
        """Merge/queue a targeted recheck payload for later execution."""
        if not title_keys:
            return
        if (expected := self._pending_everything_recheck) and expected[
            "generation"
        ] == generation:
            expected["title_keys"].update(title_keys)
            return
        self._pending_everything_recheck = {
            "title_keys": set(title_keys),
            "generation": generation,
        }

    def _run_deferred_everything_recheck(self) -> None:
        """Run a deferred targeted recheck when safe."""
        if self._shutdown_in_progress:
            self._pending_everything_recheck = None
            return
        pending = self._pending_everything_recheck
        if not pending:
            return
        if pending["generation"] != self._search_generation:
            self._pending_everything_recheck = None
            return
        if self._is_everything_check_active():
            return
        self._pending_everything_recheck = None
        self._recheck_everything_for_titles(
            set(pending["title_keys"]),
            pending["generation"],
        )

    @safe_slot
    def on_sort_changed(self, _logical_index: int):
        """
        Handle sort order change
        Re-apply background colors to avoid similar colors in adjacent rows
        """
        # Give table a moment to complete sorting (tracked timer for cleanup)
        self._schedule_timer(50, self.reapply_row_colors)
        self._update_status_bar_counts()

    def get_palette_colors(self) -> list[QColor]:
        """Return the extracted 24-color palette used for grouped rows."""
        return build_palette_colors()

    def _update_status_bar_counts(self):
        """Update the status bar using the extracted results-view helper."""
        update_results_status(self)

    def reapply_row_colors(self):
        """Re-apply grouped row colors using the extracted results helper."""
        reapply_result_row_colors(self)

    def _recheck_everything_for_titles(
        self, title_keys: set[str], expected_generation: int | None = None
    ) -> None:
        """
        Re-check Everything for all rows matching any of the given title prefixes.
        Called after download with configurable delay.
        Runs on a background worker to avoid blocking the main thread.
        """
        if self._shutdown_in_progress:
            return
        # Skip if a new search has started since the recheck was scheduled
        if (
            expected_generation is not None
            and expected_generation != self._search_generation
        ):
            self.log("Skipping recheck (search generation changed)")
            return

        # Skip while another worker still owns Everything check lifecycle.
        if self._is_everything_check_active():
            generation = (
                expected_generation
                if expected_generation is not None
                else self._search_generation
            )
            self._queue_deferred_everything_recheck(set(title_keys), generation)
            self.log("Deferring recheck (Everything worker still running)")
            return

        recheck_results = self._collect_recheck_results(title_keys)
        if not recheck_results:
            return

        self.log(
            f"Re-checking Everything for {len(recheck_results)} rows across "
            f"{len(title_keys)} title groups..."
        )

        self._acquire_table_sort_lock("everything")
        self._everything_check_generation = self._search_generation
        worker = self._create_recheck_worker(recheck_results)
        if worker is None:
            return
        recheck_title_map = self._recheck_title_map(recheck_results)
        title_to_rows = self._snapshot_title_rows()

        def handle_recheck_batch(batch: EverythingBatch) -> None:
            try:
                remapped = self._remap_recheck_batch(
                    batch,
                    recheck_title_map,
                    title_to_rows,
                    worker,
                )
                if remapped:
                    self.on_everything_batch_ready(remapped, worker)
            except Exception as e:
                logger.error(f"Error in on_recheck_batch: {e}")

        worker.batch_ready.connect(handle_recheck_batch)
        worker.check_done.connect(self._everything_done_callback(worker))
        worker.progress.connect(self._everything_progress_callback(worker))
        self.start_spinner("everything")
        try:
            worker.start()
        except Exception as e:
            logger.error(f"Failed to start recheck worker: {e}")
            self._clear_everything_check_ownership()
            return

    def _toggle_column_visibility(self, col: int, hidden: bool):
        """Toggle column visibility and persist to INI preferences."""
        try:
            self.results_table.setColumnHidden(col, hidden)
            hidden_cols = [
                self.COL_HEADERS[c]
                for c in range(self.COL_COUNT)
                if self.results_table.isColumnHidden(c)
            ]
            self.preferences_store.set_value(
                self._pref_key("hidden_columns"), hidden_cols
            )
            self._schedule_preferences_sync()
        except Exception as e:
            logger.error(f"Failed to toggle column visibility: {e}")

    def _save_column_widths(self):
        """Save current column widths to INI preferences."""
        widths: list[int] = []
        for col in range(self.COL_COUNT):
            if col == self.COL_TITLE:
                continue  # Title column stretches, skip
            widths.append(self.results_table.columnWidth(col))
        self.preferences_store.set_value(self._pref_key("column_widths"), widths)

    def _restore_column_widths(self):
        """Restore column widths from saved INI preferences."""
        widths = (
            self.preferences_store.get_int_list(
                self._pref_key("column_widths"),
                [],
            )
            or []
        )
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
    def _fit_columns(self):
        """Resize visible columns to content and persist widths."""
        if self._block_if_shutting_down():
            return
        for col in range(self.COL_COUNT):
            if col == self.COL_TITLE or self.results_table.isColumnHidden(col):
                continue
            self.results_table.resizeColumnToContents(col)
        # Keep title as adaptive stretch column after fitting other columns.
        self.results_table.horizontalHeader().setSectionResizeMode(
            self.COL_TITLE, QHeaderView.ResizeMode.Stretch
        )
        self._save_column_widths()
        self._schedule_preferences_sync()
        self.status_label.setText("Fitted visible columns to content")

    @safe_slot
    def _reset_view(self):
        """Reset all columns to visible and default widths"""
        for col in range(self.COL_COUNT):
            self.results_table.setColumnHidden(col, False)
        self.results_table.resizeColumnsToContents()
        # Re-set Title to stretch mode
        self.results_table.horizontalHeader().setSectionResizeMode(
            self.COL_TITLE, QHeaderView.ResizeMode.Stretch
        )
        # Clear saved widths and hidden columns
        self.preferences_store.remove(self._pref_key("column_widths"))
        self.preferences_store.remove(self._pref_key("hidden_columns"))
        self._schedule_preferences_sync()
        self.status_label.setText("View reset: all columns visible, default widths")

    def display_results(self, results: list[ReleaseDict]) -> None:
        """Display search results in the extracted table renderer."""
        render_results_table(self, results)

    def _collect_row_download_item(self, row: int) -> ReleaseDict | None:
        """Extract download info from a table row"""
        button = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
        if not button:
            return None
        guid = self._text_value(button.property("guid"), "")
        indexer_id = self._int_value(button.property("indexerId"), -1)
        title = self._text_value(button.property("title"), "Unknown")
        # Accept indexer_id=0 as valid; only reject missing id or empty guid.
        if not guid or indexer_id < 0:
            return None
        return {"guid": guid, "indexer_id": indexer_id, "title": title}

    def _get_release_key_for_row(self, row: int) -> ReleaseKey | None:
        """Resolve stable release key from a row's download button metadata."""
        button = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
        if not button:
            return None
        guid = self._text_value(button.property("guid"), "")
        indexer_id = self._int_value(button.property("indexerId"), -1)
        if not guid or indexer_id < 0:
            return None
        return guid, indexer_id

    def _download_from_button(self, btn: QPushButton) -> None:
        """Find the button's current row and download that release"""
        try:
            for row in range(self.results_table.rowCount()):
                if self.results_table.cellWidget(row, self.COL_DOWNLOAD) is btn:
                    self.download_release(row)
                    return
        except Exception as e:
            logger.error(f"Failed to download from button: {e}")

    def download_release(self, row: int) -> None:
        """Download a single release via the background queue"""
        item = self._collect_row_download_item(row)
        if not item:
            return
        # Skip if already downloaded
        release_key = (item["guid"], item["indexer_id"])
        if release_key in self._downloaded_release_keys:
            self.status_label.setText(
                f"Already downloaded: {item.get('title', 'Unknown')}"
            )
            return
        self.start_download_queue([item])

    @safe_slot
    def download_selected(self) -> None:
        """Download all selected releases via the background queue"""
        selected_rows = sorted(
            {idx.row() for idx in self.results_table.selectedIndexes()}
        )
        if not selected_rows:
            self.status_label.setText("No rows selected")
            return
        items: list[ReleaseDict] = []
        for row in selected_rows:
            item = self._collect_row_download_item(row)
            if item:
                items.append(item)
        if items:
            self.start_download_queue(items)

    @safe_slot
    def download_all(self) -> None:
        """Download all visible, non-downloaded releases in the table."""
        items: list[ReleaseDict] = []
        for row in range(self.results_table.rowCount()):
            if self.results_table.isRowHidden(row):
                continue
            # Skip already-downloaded rows using the authoritative GUID set
            btn = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
            if btn:
                release_key = (
                    self._text_value(btn.property("guid"), ""),
                    self._int_value(btn.property("indexerId"), -1),
                )
                if release_key in self._downloaded_release_keys:
                    continue
            item = self._collect_row_download_item(row)
            if item:
                items.append(item)
        if items:
            self.start_download_queue(items)

    @safe_slot
    def select_best_per_group(self):
        """Select the best release per title group."""
        self.results_table.clearSelection()
        groups: dict[str, tuple[int, float, int]] = {}

        for row in range(self.results_table.rowCount()):
            if self.results_table.isRowHidden(row):
                continue
            title_item = self.results_table.item(row, self.COL_TITLE)
            if not title_item:
                continue

            title_key = title_item.text()[: self.title_match_chars].lower()
            seeders_item = self.results_table.item(row, self.COL_SEEDERS)
            size_item = self.results_table.item(row, self.COL_SIZE)
            raw_seeders = (
                self._int_value(seeders_item.data(Qt.ItemDataRole.UserRole), -1)
                if seeders_item
                else -1
            )
            size = (
                self._int_value(size_item.data(Qt.ItemDataRole.UserRole), 0)
                if size_item
                else 0
            )
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
        for _title_key, (row, _, _) in groups.items():
            for col in range(self.COL_COUNT):
                idx = self.results_table.model().index(row, col)
                sel_model.select(idx, sel_model.SelectionFlag.Select)
            count += 1

        self.status_label.setText(f"Selected best release from {count} title groups")
        self.log(f"Select best per group: {count} groups")

    def start_download_queue(
        self, items: list[ReleaseDict], retry_attempt: int = 0
    ) -> None:
        """Start or append to the background download queue."""
        if self._block_if_shutting_down():
            return
        client = self.prowlarr
        if client is None:
            self.status_label.setText("Prowlarr client not initialized")
            return

        items = self._normalize_download_queue_items(items)
        if not items:
            self.status_label.setText("No new items to queue")
            return

        if self._append_items_to_download_queue(items, retry_attempt):
            return

        self._prepare_new_download_queue(items)
        self._create_download_worker(client, items)

    @safe_slot
    def on_download_progress(
        self,
        current: int,
        total: int,
        title: str,
        worker: DownloadWorker | None = None,
    ) -> None:
        """Update progress bar and status during batch download"""
        if worker is not None and worker is not self.download_worker:
            return
        self.download_progress.setValue(current)
        self.status_label.setText(f"Downloading {current}/{total} [ {title} ]")

    def _find_row_by_release_key(self, guid: str, indexer_id: int) -> int:
        """Find a row by release key, using the cache before scanning."""
        release_key = (guid, indexer_id)
        row = self._release_key_to_row.get(release_key, -1)
        if row >= 0:
            btn = self.results_table.cellWidget(row, self.COL_DOWNLOAD)
            if (
                btn
                and btn.property("guid") == guid
                and btn.property("indexerId") == indexer_id
            ):
                return row
        # Fallback: scan table (handles post-sort row changes).
        for r in range(self.results_table.rowCount()):
            btn = self.results_table.cellWidget(r, self.COL_DOWNLOAD)
            if (
                btn
                and btn.property("guid") == guid
                and btn.property("indexerId") == indexer_id
            ):
                self._release_key_to_row[release_key] = r
                return r
        return -1

    @safe_slot
    def on_item_downloaded(
        self,
        guid: str,
        indexer_id: int,
        success: bool,
        worker: DownloadWorker | None = None,
    ) -> None:
        """Handle individual item download result, identified by (guid, indexer_id)."""
        if worker is not None and worker is not self.download_worker:
            return
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
    def on_download_queue_finished(self, worker: DownloadWorker | None = None) -> None:
        """Handle download queue completion"""
        if worker is not None and worker is not self.download_worker:
            return
        self.download_worker = None
        self._download_queue_owner_since = None
        self.download_progress.setMaximum(1)
        self.download_progress.setValue(0)

        # Release download sort lock (sorting stays disabled if other owners remain).
        self._release_table_sort_lock("download")

        # Re-enable search and disable cancel (was swapped during download)
        self.search_btn.setEnabled(True)

        # Re-enable download buttons based on current state
        self.update_download_button_states()

        timestamp = datetime.now().strftime("%H:%M:%S")
        self.status_label.setText(f"[{timestamp}] Download queue complete")
        self.log("Download queue finished")

        # Schedule Everything recheck only for downloaded title groups
        # Optimized: single timer rechecks all keys instead of one timer per key
        if self._shutdown_in_progress:
            self._downloaded_title_keys = set()
            return
        if self.everything and self._downloaded_title_keys:
            title_keys = self._downloaded_title_keys.copy()
            self._downloaded_title_keys = set()
            gen = self._search_generation

            # Batch recheck: single worker for all title keys
            def recheck_all_downloaded():
                self._recheck_everything_for_titles(title_keys, gen)

            self.log(
                "Scheduling Everything recheck in "
                f"{self.everything_recheck_delay}ms for "
                f"{len(title_keys)} title groups..."
            )
            self._schedule_timer(self.everything_recheck_delay, recheck_all_downloaded)

    def _write_download_history(self, title: str, indexer: str, success: bool):
        """
        Append a download record to the persistent download history log
        Implements automatic log rotation when file exceeds 10 MB
        """
        try:
            history_file = DOWNLOAD_HISTORY_PATH
            max_size = 10 * 1024 * 1024  # 10 MB
            os.makedirs(os.path.dirname(history_file), exist_ok=True)

            # Check if rotation needed
            if (
                os.path.exists(history_file)
                and os.path.getsize(history_file) > max_size
            ):
                # Rotate: .log -> .log.1, .log.1 -> .log.2, etc. (keep 5 files)
                for i in range(4, 0, -1):
                    old_file = f"{history_file}.{i}"
                    new_file = f"{history_file}.{i + 1}"
                    if os.path.exists(old_file):
                        os.replace(old_file, new_file)
                os.replace(history_file, f"{history_file}.1")
                logger.info(
                    "Rotated download history log "
                    f"(exceeded {max_size / 1024 / 1024:.1f} MB)"
                )

            # Append new record (escape tabs/newlines to preserve TSV format)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = "OK" if success else "FAIL"
            safe_title = title.replace("\t", " ").replace("\n", " ").replace("\r", "")
            safe_indexer = (
                indexer.replace("\t", " ").replace("\n", " ").replace("\r", "")
            )
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
            if not open_path_in_default_app(history_path):
                raise RuntimeError("No default opener available")
        except Exception as e:
            logger.error(f"Failed to open file: {e}")
            self.status_label.setText(f"Cannot open file: {e}")

    @safe_slot
    def _edit_preferences_ini_file(self):
        """Open the user preferences INI file in the system editor."""
        ini_path = self.preferences_store.file_name()
        try:
            # Ensure file exists on disk before opening.
            self.preferences_store.sync()
            ini_dir = os.path.dirname(os.path.abspath(ini_path))
            if ini_dir:
                os.makedirs(ini_dir, exist_ok=True)
            if not os.path.exists(ini_path):
                with open(ini_path, "a", encoding="utf-8"):
                    pass
            if not open_path_in_default_app(ini_path):
                raise RuntimeError("No default opener available")
        except Exception as e:
            logger.error(f"Failed to open preferences INI file: {e}")
            self.status_label.setText(f"Cannot open INI file: {e}")

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
        has_visible_downloadable = any(
            is_row_downloadable(row) for row in range(self.results_table.rowCount())
        )
        self.download_all_btn.setEnabled(has_visible_downloadable)

        # Download Selected: enabled only when selected rows include an actionable row.
        selected_rows = {idx.row() for idx in self.results_table.selectedIndexes()}
        has_selected_downloadable = any(
            is_row_downloadable(row) for row in selected_rows
        )
        self.download_selected_btn.setEnabled(has_selected_downloadable)

    def get_current_row_title(self) -> str | None:
        """Get title from currently selected row"""
        current_row = self.results_table.currentRow()
        if current_row >= 0:
            title_item = self.results_table.item(current_row, self.COL_TITLE)
            if title_item:
                return title_item.text()
        return None

    VIDEO_EXTENSIONS: ClassVar[set[str]] = {
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

    def _find_video_file(self, everything_results: EverythingMatches) -> str | None:
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
                logger.warning(
                    f"_find_video_file: ERROR at index {i}: {e} item={item!r}"
                )
        logger.debug("_find_video_file: NO VIDEO FOUND")
        return None

    def _get_video_path_for_row(self, row: int) -> str | None:
        """Get stored video file path for a row by release identity."""
        release_key = self._get_release_key_for_row(row)
        if not release_key:
            return None
        return self._video_paths.get(release_key)

    @safe_slot
    def _on_cell_double_clicked(self, row: int, _column: int):
        """Download release on double-click"""
        self.download_release(row)

    @safe_slot
    def _show_header_context_menu(self, pos: QPoint) -> None:
        """Show right-click context menu on table header to toggle column visibility"""
        menu = QMenu(self)
        for col in range(self.COL_COUNT):
            if col == self.COL_TITLE:
                continue  # Title column is always visible
            name = self.COL_HEADERS[col]
            action = menu.addAction(name)
            action.setCheckable(True)
            action.setChecked(not self.results_table.isColumnHidden(col))

            def toggle_column(checked: bool, c: int = col) -> None:
                self._toggle_column_visibility(c, not checked)

            action.toggled.connect(toggle_column)
        menu.exec(self.results_table.horizontalHeader().mapToGlobal(pos))

    @safe_slot
    def _show_context_menu(self, pos: QPoint) -> None:
        """Show right-click context menu on results table"""
        row = self.results_table.rowAt(pos.y())
        if row < 0:
            return

        menu = QMenu(self)

        # Download
        download_action = menu.addAction("Download (Space)")

        def trigger_download() -> None:
            self.download_release(row)

        download_action.triggered.connect(trigger_download)

        menu.addSeparator()

        # Copy title
        copy_action = menu.addAction("Copy Title (C)")

        def trigger_copy_title() -> None:
            self._context_copy_title(row)

        copy_action.triggered.connect(trigger_copy_title)

        # Web search
        web_action = menu.addAction("Web Search (G)")

        def trigger_web_search() -> None:
            self._context_web_search(row)

        web_action.triggered.connect(trigger_web_search)

        # Play video
        play_action = menu.addAction("Play Video (P)")
        video_path = self._get_video_path_for_row(row)
        play_action.setEnabled(video_path is not None)

        def _play_video():
            try:
                if video_path:
                    open_path_in_default_app(video_path)
            except Exception as e:
                logger.error(f"Failed to play video: {e}")

        play_action.triggered.connect(_play_video)

        # Everything search
        if self.everything:
            everything = self.everything
            everything_action = menu.addAction("Search Everything (S)")
            title_item = self.results_table.item(row, self.COL_TITLE)
            title = title_item.text() if title_item else None
            everything_action.setEnabled(title is not None)

            def _search_everything():
                try:
                    if title:
                        everything.launch_search(title)
                except Exception as e:
                    logger.error(f"Failed to launch Everything: {e}")

            everything_action.triggered.connect(_search_everything)

        # Custom commands
        for key, label in [
            (Qt.Key.Key_F2, "F2"),
            (Qt.Key.Key_F3, "F3"),
            (Qt.Key.Key_F4, "F4"),
        ]:
            cmd = self.custom_commands.get(key, "")
            if cmd:
                action = menu.addAction(f"Custom Command {label}")

                def trigger_custom_command(
                    _checked: bool = False,
                    k: Qt.Key = key,
                    c: str = cmd,
                ) -> None:
                    self._run_custom_command(k, c)

                action.triggered.connect(trigger_custom_command)

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
        """Toggle the find bar visibility."""
        toggle_results_find_bar(self)

    @safe_slot
    def _close_find_bar(self):
        """Hide the find bar and return focus to the results table."""
        close_results_find_bar(self)

    @safe_slot
    def _find_next(self):
        """Find the next matching row in the results table."""
        find_next_result(self)

    @safe_slot
    def _find_prev(self):
        """Find the previous matching row in the results table."""
        find_prev_result(self)

    def _find_in_table(self, forward: bool = True):
        """Search table titles for the find text and select the next match."""
        find_results_in_table(self, forward=forward)

    def eventFilter(self, obj: QObject, event: QEvent) -> bool:
        """Handle find-bar keyboard shortcuts before falling back to Qt."""
        if handle_find_event(self, obj, event):
            return True
        return super().eventFilter(obj, event)

    def table_key_press(self, event: QKeyEvent) -> None:
        """Handle keyboard shortcuts in the results table."""
        handle_results_table_key_press(self, event)

    def _jump_title_group(self, forward: bool = True):
        """Jump to the first row of the next or previous title group."""
        jump_result_title_group(self, forward=forward)

    def _run_custom_command(self, key: Qt.Key, cmd_template: str) -> None:
        """Run a custom command with title and video placeholders."""
        run_results_custom_command(self, key, cmd_template)

    def _refresh_spinner(self):
        """Apply spinner state from active operation tags."""
        if self._active_spinner_tags:
            self.activity_bar.setRange(0, 0)
        else:
            self.activity_bar.setRange(0, 1)
            self.activity_bar.setValue(1)

    def start_spinner(self, tag: str = "default"):
        """Mark an operation as active and show indeterminate spinner."""
        self._active_spinner_tags[tag] = self._active_spinner_tags.get(tag, 0) + 1
        self._refresh_spinner()

    def stop_spinner(self, tag: str = "default"):
        """Mark an operation as complete and hide spinner when no work remains."""
        count = self._active_spinner_tags.get(tag, 0)
        if count <= 1:
            self._active_spinner_tags.pop(tag, None)
        else:
            self._active_spinner_tags[tag] = count - 1
        self._refresh_spinner()

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

    def _cancel_close_retry_timer(self):
        """Stop pending close-retry timer if one exists."""
        timer = self._close_retry_timer
        if timer:
            try:
                if timer.isActive():
                    timer.stop()
            except Exception as e:
                logger.debug(f"Failed to stop close retry timer: {e}")
            try:
                if timer in self._pending_timers:
                    self._pending_timers.remove(timer)
            except Exception as exc:
                logger.debug(f"Failed to untrack close retry timer: {exc}")
        self._close_retry_pending = False
        self._close_retry_timer = None

    def closeEvent(self, event: QCloseEvent) -> None:
        """
        Handle application close event
        Save preferences before exiting
        """
        tracked_named_workers = self._tracked_named_workers()
        now = time.monotonic()
        force_armed = bool(
            self._shutdown_force_armed_until is not None
            and now <= self._shutdown_force_armed_until
        )
        if (
            self._shutdown_force_armed_until is not None
            and now > self._shutdown_force_armed_until
        ):
            # Force-close arm window expired; require a fresh graceful cycle.
            self._shutdown_force_armed_until = None
            self._shutdown_force_prompted = False
            force_armed = False

        first_shutdown_attempt = (not self._shutdown_in_progress) and (not force_armed)
        if first_shutdown_attempt:
            self._shutdown_started_monotonic = now
            self._shutdown_force_prompted = False
            self._shutdown_interrupted_worker_ids.clear()

        wait_ms = 75 if first_shutdown_attempt else 0
        self._interrupt_all_tracked_workers(tracked_named_workers)
        still_running = self._collect_running_workers_for_close(
            tracked_named_workers,
            wait_ms=wait_ms,
        )

        if still_running:
            unique_workers = sorted({name for name, _worker in still_running})
            wait_msg = (
                f"Waiting for background tasks to stop: {', '.join(unique_workers)}"
            )
            logger.warning(wait_msg)
            if hasattr(self, "status_label"):
                self.status_label.setText(wait_msg)

            if force_armed:
                if self._handle_force_close_workers(still_running, event):
                    return
            elif self._handle_graceful_close_wait(event):
                return

        self._shutdown_in_progress = False
        self._shutdown_started_monotonic = None
        self._shutdown_force_prompted = False
        self._shutdown_force_armed_until = None
        self._shutdown_interrupted_worker_ids.clear()
        self.stop_spinner("shutdown")
        self._cancel_close_retry_timer()

        # Stop splitter save timer
        if self.splitter_save_timer.isActive():
            self.splitter_save_timer.stop()
        if self.config_save_timer.isActive():
            self.config_save_timer.stop()

        # Stop all pending timers to prevent firing after window closes
        # Copy the list first because a timer cleanup callback may modify
        # _pending_timers during iteration.
        timers_snapshot = list(self._pending_timers)
        timer_count = len(timers_snapshot)
        self._pending_timers.clear()
        for timer in timers_snapshot:
            if timer and timer.isActive():
                timer.stop()
        logger.info(f"Stopped {timer_count} pending timers")

        # Ensure activity bar returns to idle state before teardown.
        self._active_spinner_tags.clear()
        self._refresh_spinner()
        self._table_sort_locks.clear()

        # Save user preferences to INI.
        self._persist_runtime_preferences()
        self._sync_preferences()

        # Save runtime config store (non-preference settings).
        if self._save_config_with_retry():
            self.log("Application closing, configuration saved")
        else:
            self.log("ERROR: Failed to save configuration after retries")

        # Close log window (no parent, so must close explicitly)
        if self.log_window:
            self.log_window.close()

        event.accept()

    def _retry_close(self):
        """Retry close after waiting for worker shutdown."""
        self._close_retry_pending = False
        self._close_retry_timer = None
        if not self._shutdown_in_progress:
            return
        self.close()


def main():
    """Application entry point"""
    configure_qsettings(APP_IDENTITY)

    # Ensure config keys exist in the shared settings store.
    ensure_config_exists()

    # Setup logging
    setup_logging_from_identity(APP_IDENTITY)

    app = QApplication(sys.argv)

    # First-run setup wizard for required runtime config.
    config_preview = load_config()
    missing = get_missing_required_config(config_preview)
    if missing:
        configured = run_setup_wizard(config_preview)
        if configured is None:
            print("Setup wizard cancelled; required config is missing.")
            print(f"Config INI path: {config_store_file_path()}")
            raise SystemExit(1)
        save_config(configured)

    # Create and show main window
    window = MainWindow()
    window.showMaximized()

    # Start Qt event loop
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
