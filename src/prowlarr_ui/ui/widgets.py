"""Custom UI widgets"""

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QTableWidgetItem


class NumericTableWidgetItem(QTableWidgetItem):
    """Table widget item that sorts numerically using UserRole data instead of text"""

    def __lt__(self, other: object) -> bool:
        # Compare using numeric data stored in UserRole
        if not isinstance(other, QTableWidgetItem):
            return False
        self_data = self.data(Qt.ItemDataRole.UserRole)
        other_data = other.data(Qt.ItemDataRole.UserRole)

        # Handle None/missing data (None sorts last, both-None = equal)
        if self_data is None and other_data is None:
            return False
        if self_data is None:
            return False
        if other_data is None:
            return True

        return bool(self_data < other_data)
