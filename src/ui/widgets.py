"""Custom UI widgets"""
from PySide6.QtWidgets import QTableWidgetItem
from PySide6.QtCore import Qt


class NumericTableWidgetItem(QTableWidgetItem):
    """Table widget item that sorts numerically using UserRole data instead of text"""
    def __lt__(self, other):
        # Compare using numeric data stored in UserRole
        self_data = self.data(Qt.UserRole)
        other_data = other.data(Qt.UserRole)

        # Handle None/missing data (None sorts last, both-None = equal)
        if self_data is None and other_data is None:
            return False
        if self_data is None:
            return False
        if other_data is None:
            return True

        return self_data < other_data
