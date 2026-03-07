"""Floating log viewer window"""

import logging
import os
from datetime import datetime

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QFont, QIcon, QPainter, QPen, QPixmap, QTextCursor
from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSpinBox,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
from threep_commons.logging import resolve_log_path

from prowlarr_ui.constants import APP_IDENTITY

LOG_FILE_PATH = str(resolve_log_path(APP_IDENTITY, APP_IDENTITY.default_log_filename))

logger = logging.getLogger(__name__)

MAX_LOG_LINES = 64000


class LogWindow(QWidget):
    """
    Floating log viewer window
    Can be toggled from main menu
    """

    def __init__(self, parent=None):
        super().__init__(None, Qt.Window)  # No parent = own taskbar icon
        self.setWindowTitle("Log Viewer")
        self.setWindowIcon(self._create_notebook_icon())

        # Position on right half of screen, with 200px margin top and bottom
        screen = QApplication.primaryScreen().availableGeometry()
        margin = 160
        h = screen.height() - 2 * margin
        self.setGeometry(screen.x() + screen.width() // 2, screen.y() + margin, screen.width() // 2, h)

        layout = QVBoxLayout()
        self.setLayout(layout)

        # Read-only text area for log messages
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self._set_font_size(10)
        layout.addWidget(self.log_text)

        # Button bar
        button_layout = QHBoxLayout()

        clear_btn = QPushButton("C&lear")
        clear_btn.clicked.connect(self.log_text.clear)
        button_layout.addWidget(clear_btn)

        save_btn = QPushButton("&Save to File")
        save_btn.clicked.connect(self.save_to_file)
        button_layout.addWidget(save_btn)

        copy_btn = QPushButton("&Copy to Clipboard")
        copy_btn.clicked.connect(self.copy_to_clipboard)
        button_layout.addWidget(copy_btn)

        stay_on_top_btn = QPushButton("Toggle Stay on &Top")
        stay_on_top_btn.clicked.connect(self.toggle_stay_on_top)
        button_layout.addWidget(stay_on_top_btn)

        open_log_btn = QPushButton("&Open Log File")
        open_log_btn.clicked.connect(self.open_log_file)
        button_layout.addWidget(open_log_btn)

        # Font size spinner
        button_layout.addWidget(QLabel("Font:"))
        self.font_size_spinbox = QSpinBox()
        self.font_size_spinbox.setRange(6, 30)
        self.font_size_spinbox.setValue(10)
        self.font_size_spinbox.setSuffix("pt")
        self.font_size_spinbox.valueChanged.connect(self._set_font_size)
        button_layout.addWidget(self.font_size_spinbox)

        layout.addLayout(button_layout)

    def _set_font_size(self, size: int):
        """Set log text font to Courier New at given size"""
        self.log_text.setFont(QFont("Courier New", size))

    def _trim_lines(self):
        """Remove oldest lines if line count exceeds MAX_LOG_LINES"""
        doc = self.log_text.document()
        excess = doc.blockCount() - MAX_LOG_LINES
        if excess > 0:
            cursor = QTextCursor(doc)
            cursor.movePosition(QTextCursor.Start)
            for _ in range(excess):
                cursor.movePosition(QTextCursor.Down, QTextCursor.KeepAnchor)
            cursor.movePosition(QTextCursor.StartOfLine, QTextCursor.KeepAnchor)
            cursor.removeSelectedText()
            cursor.deleteChar()  # remove trailing newline

    def append_log(self, message: str):
        """Add timestamped log message"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
        self._trim_lines()

    def save_to_file(self):
        """Save log contents to a file"""
        try:
            filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(self.log_text.toPlainText())
            self.append_log(f"Log saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save log: {e}")
            self.append_log(f"Failed to save log: {e}")

    def copy_to_clipboard(self):
        """Copy log contents to system clipboard"""
        try:
            clipboard = QApplication.clipboard()
            clipboard.setText(self.log_text.toPlainText())
            self.append_log("Log copied to clipboard")
        except Exception as e:
            logger.error(f"Failed to copy to clipboard: {e}")

    def open_log_file(self):
        """Open the log file in the system's default text editor"""
        try:
            log_path = LOG_FILE_PATH
            if os.path.exists(log_path):
                os.startfile(log_path)
            else:
                self.append_log(f"Log file not found: {log_path}")
        except Exception as e:
            logger.error(f"Failed to open log file: {e}")
            self.append_log(f"Failed to open log file: {e}")

    def toggle_stay_on_top(self):
        """Toggle window always-on-top flag"""
        try:
            if self.windowFlags() & Qt.WindowStaysOnTopHint:
                self.setWindowFlags(self.windowFlags() & ~Qt.WindowStaysOnTopHint)
            else:
                self.setWindowFlags(self.windowFlags() | Qt.WindowStaysOnTopHint)
            self.show()  # Required after changing window flags
        except Exception as e:
            logger.error(f"Failed to toggle stay on top: {e}")

    @staticmethod
    def _create_notebook_icon() -> QIcon:
        """Draw a simple notebook icon (page with lines and spiral binding)"""
        size = 64
        pix = QPixmap(size, size)
        pix.fill(Qt.transparent)
        p = QPainter(pix)
        p.setRenderHint(QPainter.Antialiasing)
        # Page background
        p.setPen(QPen(QColor(80, 80, 80), 2))
        p.setBrush(QColor(255, 255, 240))
        p.drawRoundedRect(10, 4, 50, 56, 3, 3)
        # Ruled lines
        p.setPen(QPen(QColor(180, 200, 220), 1))
        for y in range(18, 54, 8):
            p.drawLine(18, y, 54, y)
        # Spiral binding dots on left edge
        p.setPen(Qt.NoPen)
        p.setBrush(QColor(100, 100, 100))
        for y in range(12, 56, 10):
            p.drawEllipse(7, y, 6, 6)
        p.end()
        return QIcon(pix)
