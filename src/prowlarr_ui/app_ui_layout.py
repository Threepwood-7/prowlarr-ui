"""UI layout helpers for the Prowlarr main window."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import (
    QKeyEvent,
    QKeySequence,
    QMouseEvent,
    QShortcut,
    QStandardItemModel,
)
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QCompleter,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QSplitter,
    QStatusBar,
    QTableWidget,
    QTreeView,
    QVBoxLayout,
    QWidget,
)

if TYPE_CHECKING:
    from .app import MainWindow


def setup_main_window_ui(window: MainWindow) -> None:
    """Build the main window layout and restore saved splitter sizes."""
    central_widget = QWidget()
    window.setCentralWidget(central_widget)

    main_layout = QVBoxLayout()
    main_layout.setContentsMargins(0, 0, 0, 0)
    central_widget.setLayout(main_layout)

    window.activity_bar = QProgressBar()
    window.activity_bar.setFixedHeight(4)
    window.activity_bar.setTextVisible(False)
    window.activity_bar.setRange(0, 1)
    window.activity_bar.setValue(1)
    main_layout.addWidget(window.activity_bar)

    window.splitter = QSplitter(Qt.Orientation.Horizontal)
    window.splitter.addWidget(build_left_panel(window))
    window.splitter.addWidget(build_center_panel(window))
    main_layout.addWidget(window.splitter)

    saved_sizes = window.preferences_store.get_int_list(
        window.pref_key("splitter_sizes"),
        [300, 1100],
    ) or [300, 1100]
    window.splitter.setSizes(saved_sizes)
    window.splitter.splitterMoved.connect(window.on_splitter_moved)

    window.status_bar = QStatusBar()
    window.setStatusBar(window.status_bar)
    window.status_label = QLabel("Loading...")
    window.status_bar.addWidget(window.status_label, 1)


def build_left_panel(window: MainWindow) -> QWidget:
    """Create the left control panel with search and download controls."""
    panel = QWidget()
    layout = QVBoxLayout()
    layout.setContentsMargins(2, 2, 2, 2)
    layout.setSpacing(4)
    panel.setLayout(layout)

    layout.addWidget(_build_search_group(window))
    layout.addWidget(_build_pagination_group(window))
    layout.addWidget(_build_filters_group(window), 1)
    _add_download_controls(window, layout)
    return panel


def _build_search_group(window: MainWindow) -> QGroupBox:
    """Build the query input and search action group."""
    search_group = QGroupBox("Search")
    search_layout = QVBoxLayout()
    search_layout.setContentsMargins(6, 6, 6, 6)
    search_layout.setSpacing(4)
    search_group.setLayout(search_layout)

    search_label = QLabel("Search &Query:")
    search_layout.addWidget(search_label)
    window.query_input = QLineEdit()
    search_label.setBuddy(window.query_input)
    window.query_input.setPlaceholderText("Enter search query...")
    window.query_input.returnPressed.connect(window.on_search_return_pressed)

    window.completer = QCompleter(window.search_history)
    window.completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
    window.query_input.setCompleter(window.completer)

    def show_completer_on_focus(event: QMouseEvent) -> None:
        QLineEdit.mousePressEvent(window.query_input, event)
        if not window.query_input.text():
            window.completer.complete()

    window.query_input.mousePressEvent = show_completer_on_focus
    original_key_press = window.query_input.keyPressEvent

    def search_key_press(event: QKeyEvent) -> None:
        if event.key() == Qt.Key.Key_Down and not window.query_input.text():
            window.completer.setCompletionPrefix("")
            window.completer.complete()
            return
        original_key_press(event)

    window.query_input.keyPressEvent = search_key_press
    search_layout.addWidget(window.query_input)
    window.query_input.setToolTip(
        "Enter a search term and press Enter to search Prowlarr indexers\n"
        "Press Down or Enter when empty to browse search history"
    )

    button_layout = QHBoxLayout()
    button_layout.setSpacing(4)
    window.search_btn = QPushButton("&Search")
    window.search_btn.clicked.connect(window.start_search)
    window.search_btn.setToolTip("Search Prowlarr with the current query and filters")
    button_layout.addWidget(window.search_btn)

    window.load_all_btn = QPushButton("Load A&ll")
    window.load_all_btn.clicked.connect(window.start_load_all_pages)
    window.load_all_btn.setToolTip("Fetch all pages of results sequentially")
    button_layout.addWidget(window.load_all_btn)
    search_layout.addLayout(button_layout)
    return search_group


def _build_pagination_group(window: MainWindow) -> QGroupBox:
    """Build the pagination controls for page size and page number."""
    pagination_group = QGroupBox("Pagination")
    pagination_layout = QGridLayout()
    pagination_layout.setContentsMargins(6, 6, 6, 6)
    pagination_layout.setSpacing(4)
    pagination_group.setLayout(pagination_layout)

    page_size_label = QLabel("&Max Page Size:")
    pagination_layout.addWidget(page_size_label, 0, 0)
    window.prowlarr_page_size_spinbox = QSpinBox()
    page_size_label.setBuddy(window.prowlarr_page_size_spinbox)
    window.prowlarr_page_size_spinbox.setMinimum(10)
    window.prowlarr_page_size_spinbox.setMaximum(10000)
    window.prowlarr_page_size_spinbox.setSingleStep(100)
    window.prowlarr_page_size_spinbox.setValue(window.prowlarr_page_size)
    window.prowlarr_page_size_spinbox.valueChanged.connect(
        window.on_prowlarr_page_size_changed
    )
    window.prowlarr_page_size_spinbox.setToolTip(
        "Maximum number of results to fetch per page from Prowlarr"
    )
    pagination_layout.addWidget(window.prowlarr_page_size_spinbox, 1, 0)

    page_num_label = QLabel("Page &Number:")
    pagination_layout.addWidget(page_num_label, 0, 1)
    window.prowlarr_page_number_spinbox = QSpinBox()
    page_num_label.setBuddy(window.prowlarr_page_number_spinbox)
    window.prowlarr_page_number_spinbox.setMinimum(1)
    window.prowlarr_page_number_spinbox.setMaximum(300)
    window.prowlarr_page_number_spinbox.setSingleStep(1)
    window.prowlarr_page_number_spinbox.setValue(1)
    window.prowlarr_page_number_spinbox.valueChanged.connect(
        window.on_prowlarr_page_number_changed
    )
    window.prowlarr_page_number_spinbox.setToolTip(
        "Page number for paginated results (search again to apply)"
    )
    pagination_layout.addWidget(window.prowlarr_page_number_spinbox, 1, 1)
    return pagination_group


def _build_filters_group(window: MainWindow) -> QGroupBox:
    """Build the indexer/category trees and hide-existing toggle."""
    filters_group = QGroupBox("Filters")
    filters_layout = QVBoxLayout()
    filters_layout.setContentsMargins(6, 6, 6, 6)
    filters_layout.setSpacing(4)
    filters_group.setLayout(filters_layout)

    indexers_label = QLabel("&Indexers:")
    filters_layout.addWidget(indexers_label)
    window.indexers_tree = QTreeView()
    indexers_label.setBuddy(window.indexers_tree)
    window.indexers_model = QStandardItemModel()
    window.indexers_tree.setModel(window.indexers_model)
    window.indexers_tree.setHeaderHidden(True)
    window.indexers_tree.setToolTip(
        "Select which Prowlarr indexers to search\nUse 'All' to toggle all at once"
    )
    filters_layout.addWidget(window.indexers_tree, 1)

    categories_label = QLabel("Ca&tegories:")
    filters_layout.addWidget(categories_label)
    window.categories_tree = QTreeView()
    categories_label.setBuddy(window.categories_tree)
    window.categories_model = QStandardItemModel()
    window.categories_tree.setModel(window.categories_model)
    window.categories_tree.setHeaderHidden(True)
    window.categories_tree.setToolTip(
        "Filter results by category (Movies, TV, Audio, etc.)\n"
        "Use 'All' to toggle all at once"
    )
    filters_layout.addWidget(window.categories_tree, 2)

    window.hide_existing_checkbox = QCheckBox("Hide &existing")
    saved_hide = window.preferences_store.get_bool(
        window.pref_key("hide_existing"),
        False,
    )
    window.hide_existing_checkbox.setChecked(saved_hide)
    window.hide_existing_checkbox.toggled.connect(window.on_hide_existing_toggled)
    window.hide_existing_checkbox.setToolTip(
        "Hide results that already exist on disk (detected via Everything)"
    )
    filters_layout.addWidget(window.hide_existing_checkbox)
    return filters_group


def _add_download_controls(window: MainWindow, layout: QVBoxLayout) -> None:
    """Append the download action row and progress bar to the left panel."""
    download_layout = QHBoxLayout()
    download_layout.setSpacing(4)

    window.download_selected_btn = QPushButton("&Download Selected")
    window.download_selected_btn.clicked.connect(window.download_selected)
    window.download_selected_btn.setEnabled(False)
    window.download_selected_btn.setToolTip(
        "Download highlighted rows (Ctrl+Click to multi-select)"
    )
    download_layout.addWidget(window.download_selected_btn)

    window.download_all_btn = QPushButton("Download &All")
    window.download_all_btn.clicked.connect(window.download_all)
    window.download_all_btn.setEnabled(False)
    window.download_all_btn.setToolTip("Download all visible (non-hidden) results")
    download_layout.addWidget(window.download_all_btn)
    layout.addLayout(download_layout)

    window.download_progress = QProgressBar()
    window.download_progress.setTextVisible(True)
    window.download_progress.setFormat("%v/%m")
    window.download_progress.setMaximum(1)
    window.download_progress.setValue(0)
    layout.addWidget(window.download_progress)


def build_center_panel(window: MainWindow) -> QWidget:
    """Create the center results panel, filter bar, and find bar."""
    panel = QWidget()
    layout = QVBoxLayout()
    layout.setContentsMargins(0, 0, 0, 0)
    panel.setLayout(layout)

    layout.addLayout(_build_result_filter_bar(window))
    _configure_results_table(window)
    layout.addWidget(window.results_table)
    _build_find_bar(window, layout)
    return panel


def _build_result_filter_bar(window: MainWindow) -> QHBoxLayout:
    """Build the inline result-filter controls above the results table."""
    filter_layout = QHBoxLayout()
    filter_layout.setContentsMargins(4, 2, 4, 2)

    filter_label = QLabel("Filte&r:")
    filter_layout.addWidget(filter_label)
    window.filter_title_input = QLineEdit()
    filter_label.setBuddy(window.filter_title_input)
    window.filter_title_input.setPlaceholderText("Title contains...")
    window.filter_title_input.setToolTip(
        "Filter results by title (case-insensitive, Alt+R)"
    )
    window.filter_title_input.textChanged.connect(window.apply_result_filters)
    filter_layout.addWidget(window.filter_title_input, 1)

    filter_layout.addWidget(QLabel("Min Size:"))
    window.filter_min_size = QSpinBox()
    window.filter_min_size.setRange(0, 999999)
    window.filter_min_size.setSuffix(" MB")
    window.filter_min_size.setToolTip("Minimum file size in MB (0 = no minimum)")
    window.filter_min_size.valueChanged.connect(window.apply_result_filters)
    filter_layout.addWidget(window.filter_min_size)

    filter_layout.addWidget(QLabel("Max Age:"))
    window.filter_max_age = QSpinBox()
    window.filter_max_age.setRange(0, 99999)
    window.filter_max_age.setSuffix(" days")
    window.filter_max_age.setToolTip("Maximum age in days (0 = no limit)")
    window.filter_max_age.valueChanged.connect(window.apply_result_filters)
    filter_layout.addWidget(window.filter_max_age)

    clear_filter_btn = QPushButton("Clear")
    clear_filter_btn.setToolTip("Clear all filters")
    clear_filter_btn.clicked.connect(window.clear_result_filters)
    filter_layout.addWidget(clear_filter_btn)
    return filter_layout


def _configure_results_table(window: MainWindow) -> None:
    """Create and configure the sortable results table."""
    window.results_table = QTableWidget()
    window.results_table.setColumnCount(window.COL_COUNT)
    window.results_table.setHorizontalHeaderLabels(window.COL_HEADERS)

    header = window.results_table.horizontalHeader()
    header.setStretchLastSection(False)
    header.setSectionResizeMode(window.COL_TITLE, QHeaderView.ResizeMode.Stretch)

    window.results_table.setAlternatingRowColors(False)
    window.results_table.setSelectionBehavior(
        QAbstractItemView.SelectionBehavior.SelectRows
    )
    window.results_table.setSelectionMode(
        QAbstractItemView.SelectionMode.ExtendedSelection
    )
    window.results_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    window.results_table.setSortingEnabled(True)

    header.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
    header.customContextMenuRequested.connect(window.show_header_context_menu)
    _restore_hidden_columns(window)
    header.sectionClicked.connect(window.on_sort_changed)

    window.results_table.itemSelectionChanged.connect(
        window.update_download_button_states
    )
    window.results_table.keyPressEvent = window.table_key_press
    window.results_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
    window.results_table.customContextMenuRequested.connect(window.show_context_menu)
    window.results_table.cellDoubleClicked.connect(window.on_cell_double_clicked)


def _restore_hidden_columns(window: MainWindow) -> None:
    """Restore persisted hidden-column preferences for the results table."""
    hidden_cols = window.preferences_store.get_str_list(
        window.pref_key("hidden_columns"),
        [],
    )
    for col_name in hidden_cols:
        if col_name in window.COL_HEADERS:
            col_idx = window.COL_HEADERS.index(col_name)
            if col_idx != window.COL_TITLE:
                window.results_table.setColumnHidden(col_idx, True)


def _build_find_bar(window: MainWindow, layout: QVBoxLayout) -> None:
    """Create the hidden find bar used for in-table title search."""
    window.find_bar = QWidget()
    find_layout = QHBoxLayout()
    find_layout.setContentsMargins(4, 2, 4, 2)
    find_layout.addWidget(QLabel("Find:"))

    window.find_input = QLineEdit()
    window.find_input.setPlaceholderText(
        "Search in titles... (Enter=next, Shift+Enter=prev, Esc=close)"
    )
    window.find_input.returnPressed.connect(window.find_next)
    find_layout.addWidget(window.find_input, 1)

    find_prev_btn = QPushButton("<")
    find_prev_btn.setFixedWidth(30)
    find_prev_btn.setToolTip("Find previous (Shift+Enter)")
    find_prev_btn.clicked.connect(window.find_prev)
    find_layout.addWidget(find_prev_btn)

    find_next_btn = QPushButton(">")
    find_next_btn.setFixedWidth(30)
    find_next_btn.setToolTip("Find next (Enter)")
    find_next_btn.clicked.connect(window.find_next)
    find_layout.addWidget(find_next_btn)

    find_close_btn = QPushButton("X")
    find_close_btn.setFixedWidth(30)
    find_close_btn.setToolTip("Close find bar (Esc)")
    find_close_btn.clicked.connect(window.close_find_bar)
    find_layout.addWidget(find_close_btn)

    window.find_bar.setLayout(find_layout)
    window.find_bar.setVisible(False)
    layout.addWidget(window.find_bar)

    find_shortcut = QShortcut(QKeySequence("Ctrl+F"), window)
    find_shortcut.activated.connect(window.toggle_find_bar)
    window.find_input.installEventFilter(window)
