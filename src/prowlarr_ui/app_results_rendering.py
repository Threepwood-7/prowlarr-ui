"""Results-table rendering helpers for the Prowlarr main window."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QPushButton, QTableWidgetItem
from threep_commons.formatters import format_age, format_size
from threep_commons.quality import parse_quality

from .ui.widgets import NumericTableWidgetItem

if TYPE_CHECKING:
    from .app import MainWindow, ReleaseDict


def render_results_table(window: MainWindow, results: list[ReleaseDict]) -> None:
    """Populate the results table and restore title-group coloring."""
    _warn_about_large_result_sets(window, len(results))

    colors = window.get_palette_colors()
    title_colors: dict[str, QColor] = {}
    color_index = 0

    for result in results:
        row = window.results_table.rowCount()
        window.results_table.insertRow(row)

        title = window.text_value(result.get("title", "Unknown"), "Unknown")
        title_key = title[: window.title_match_chars].lower()
        color_index = _ensure_title_color(
            title_key,
            title_colors,
            colors,
            color_index,
        )
        _populate_result_row(window, row, result, title, title_key, title_colors)


def _warn_about_large_result_sets(window: MainWindow, result_count: int) -> None:
    """Warn when a very large result set may slow the Qt table."""
    if result_count <= 5000:
        return
    window.log(
        f"WARNING: Displaying {result_count} results may cause UI "
        "slowdown. Consider using filters."
    )
    window.status_label.setText(f"Loading {result_count} results (may be slow)...")


def _ensure_title_color(
    title_key: str,
    title_colors: dict[str, QColor],
    colors: list[QColor],
    color_index: int,
) -> int:
    """Assign a stable palette color to one title group when first seen."""
    if title_key not in title_colors:
        title_colors[title_key] = colors[color_index % len(colors)]
        return color_index + 1
    return color_index


def _populate_result_row(
    window: MainWindow,
    row: int,
    result: ReleaseDict,
    title: str,
    title_key: str,
    title_colors: dict[str, QColor],
) -> None:
    """Fill one table row and restore downloaded-row styling."""
    _set_age_item(window, row, result)
    window.results_table.setItem(row, window.COL_TITLE, QTableWidgetItem(title))
    window.results_table.setItem(
        row,
        window.COL_QUALITY,
        QTableWidgetItem(parse_quality(title)),
    )

    _set_size_item(window, row, result)
    _set_count_item(window, row, window.COL_SEEDERS, result.get("seeders"))
    _set_count_item(window, row, window.COL_LEECHERS, result.get("leechers"))
    _set_count_item(window, row, window.COL_GRABS, result.get("grabs"))

    indexer = window.text_value(result.get("indexer", "Unknown"), "Unknown")
    window.results_table.setItem(row, window.COL_INDEXER, QTableWidgetItem(indexer))

    guid = window.text_value(result.get("guid", ""))
    indexer_id = window.int_value(result.get("indexerId", -1), -1)
    download_button = _build_download_button(window, guid, indexer_id, title)
    window.results_table.setCellWidget(row, window.COL_DOWNLOAD, download_button)
    _apply_row_styling(
        window,
        row,
        title_colors[title_key],
        guid,
        indexer_id,
    )


def _set_age_item(window: MainWindow, row: int, result: ReleaseDict) -> None:
    """Populate the sortable age cell for one result row."""
    age = window.int_value(result.get("age", 0), 0)
    age_item = NumericTableWidgetItem(format_age(age))
    age_item.setData(Qt.ItemDataRole.UserRole, age)
    window.results_table.setItem(row, window.COL_AGE, age_item)


def _set_size_item(window: MainWindow, row: int, result: ReleaseDict) -> None:
    """Populate the sortable size cell for one result row."""
    size = window.int_value(result.get("size", 0), 0)
    size_item = NumericTableWidgetItem(format_size(size))
    size_item.setData(Qt.ItemDataRole.UserRole, size)
    window.results_table.setItem(row, window.COL_SIZE, size_item)


def _set_count_item(
    window: MainWindow,
    row: int,
    column: int,
    raw_value: object | None,
) -> None:
    """Populate one sortable count cell, preserving missing values."""
    value = None if raw_value is None else window.int_value(raw_value, -1)
    item = NumericTableWidgetItem(str(value) if value is not None else "")
    item.setData(Qt.ItemDataRole.UserRole, value if value is not None else -1)
    window.results_table.setItem(row, column, item)


def _build_download_button(
    window: MainWindow,
    guid: str,
    indexer_id: int,
    title: str,
) -> QPushButton:
    """Create the per-row download action button with release metadata."""
    download_button = QPushButton("Download")
    download_button.setProperty("guid", guid)
    download_button.setProperty("indexerId", indexer_id)
    download_button.setProperty("title", title)

    def click_download(
        _checked: bool = False,
        button: QPushButton = download_button,
    ) -> None:
        window.download_from_button(button)

    download_button.clicked.connect(click_download)
    return download_button


def _apply_row_styling(
    window: MainWindow,
    row: int,
    background: QColor,
    guid: str,
    indexer_id: int,
) -> None:
    """Apply title-group backgrounds and downloaded-row foreground styling."""
    is_downloaded = window.is_release_downloaded(guid, indexer_id)
    for column in range(window.COL_DOWNLOAD):
        item = window.results_table.item(row, column)
        if item is None:
            continue
        item.setBackground(background)
        if is_downloaded:
            item.setForeground(QColor(139, 0, 0))
