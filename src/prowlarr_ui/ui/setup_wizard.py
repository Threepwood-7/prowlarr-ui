"""First-run setup wizard for prowlarr-ui runtime configuration."""

from __future__ import annotations

from typing import Any

from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from prowlarr_ui.api.prowlarr_client import ProwlarrClient
from prowlarr_ui.utils.config import get_default_config


class ProwlarrSetupWizardDialog(QDialog):
    def __init__(self, initial: dict[str, Any], parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Prowlarr UI Setup Wizard")
        self.resize(680, 420)

        defaults = get_default_config()
        prowlarr = initial.get("prowlarr", {}) if isinstance(initial, dict) else {}
        settings = initial.get("settings", {}) if isinstance(initial, dict) else {}

        root = QVBoxLayout(self)
        root.addWidget(
            QLabel("Configure Prowlarr connection and core settings. Values are saved to the shared settings INI.")
        )

        form = QFormLayout()
        self.txt_host = QLineEdit(str(prowlarr.get("host", defaults["prowlarr"]["host"]) or ""))
        self.txt_api_key = QLineEdit(str(prowlarr.get("api_key", "") or ""))
        self.txt_http_user = QLineEdit(
            str(prowlarr.get("http_basic_auth_username", defaults["prowlarr"]["http_basic_auth_username"]) or "")
        )
        self.txt_http_password = QLineEdit(
            str(prowlarr.get("http_basic_auth_password", defaults["prowlarr"]["http_basic_auth_password"]) or "")
        )
        self.txt_http_password.setEchoMode(QLineEdit.EchoMode.Password)

        self.spn_page_size = QSpinBox()
        self.spn_page_size.setRange(1, 10000)
        self.spn_page_size.setValue(int(settings.get("prowlarr_page_size", defaults["settings"]["prowlarr_page_size"])))

        self.spn_everything_results = QSpinBox()
        self.spn_everything_results.setRange(1, 100)
        self.spn_everything_results.setValue(
            int(settings.get("everything_max_results", defaults["settings"]["everything_max_results"]))
        )

        self.spn_api_timeout = QSpinBox()
        self.spn_api_timeout.setRange(1, 300)
        self.spn_api_timeout.setValue(int(settings.get("api_timeout", defaults["settings"]["api_timeout"])))

        form.addRow("Prowlarr Host", self.txt_host)
        form.addRow("Prowlarr API Key", self.txt_api_key)
        form.addRow("HTTP Basic Auth User", self.txt_http_user)
        form.addRow("HTTP Basic Auth Password", self.txt_http_password)
        form.addRow("Prowlarr Page Size", self.spn_page_size)
        form.addRow("Everything Max Results", self.spn_everything_results)
        form.addRow("API Timeout (s)", self.spn_api_timeout)
        root.addLayout(form)

        btn_test_connection = QPushButton("Test Connection")
        btn_test_connection.clicked.connect(self._on_test_connection)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._on_accept)
        buttons.rejected.connect(self.reject)
        row = QHBoxLayout()
        row.addWidget(btn_test_connection)
        row.addStretch(1)
        row.addWidget(buttons)
        root.addLayout(row)

    def _on_accept(self) -> None:
        host = str(self.txt_host.text() or "").strip()
        api_key = str(self.txt_api_key.text() or "").strip()
        if not host:
            QMessageBox.warning(self, "Validation", "Prowlarr host is required.")
            return
        if not (host.startswith("http://") or host.startswith("https://")):
            QMessageBox.warning(self, "Validation", "Prowlarr host must start with http:// or https://")
            return
        if not api_key or api_key == "YOUR_API_KEY_HERE":
            QMessageBox.warning(self, "Validation", "Prowlarr API key is required.")
            return
        self.accept()

    def _on_test_connection(self) -> None:
        host = str(self.txt_host.text() or "").strip()
        api_key = str(self.txt_api_key.text() or "").strip()
        http_user = str(self.txt_http_user.text() or "").strip()
        http_password = str(self.txt_http_password.text() or "").strip()
        timeout = int(self.spn_api_timeout.value())
        if not host:
            QMessageBox.warning(self, "Connection Test", "Prowlarr host is required.")
            return
        if not (host.startswith("http://") or host.startswith("https://")):
            QMessageBox.warning(self, "Connection Test", "Prowlarr host must start with http:// or https://")
            return
        if not api_key or api_key == "YOUR_API_KEY_HERE":
            QMessageBox.warning(self, "Connection Test", "Prowlarr API key is required.")
            return

        try:
            client = ProwlarrClient(
                host=host,
                api_key=api_key,
                timeout=timeout,
                retries=0,
                http_basic_auth_username=http_user,
                http_basic_auth_password=http_password,
            )
            indexers = client.get_indexers()
            QMessageBox.information(
                self,
                "Connection Test",
                f"Prowlarr connection successful.\nIndexers visible: {len(indexers)}",
            )
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Connection Test Failed",
                f"Prowlarr connection failed:\n{exc}",
            )

    def to_config(self) -> dict[str, Any]:
        defaults = get_default_config()
        settings_defaults = defaults["settings"]
        return {
            "prowlarr": {
                "host": str(self.txt_host.text() or "").strip(),
                "api_key": str(self.txt_api_key.text() or "").strip(),
                "http_basic_auth_username": str(self.txt_http_user.text() or "").strip(),
                "http_basic_auth_password": str(self.txt_http_password.text() or "").strip(),
            },
            "settings": {
                **settings_defaults,
                "prowlarr_page_size": int(self.spn_page_size.value()),
                "everything_max_results": int(self.spn_everything_results.value()),
                "api_timeout": int(self.spn_api_timeout.value()),
            },
        }


def run_setup_wizard(initial: dict[str, Any], parent: QWidget | None = None) -> dict[str, Any] | None:
    dialog = ProwlarrSetupWizardDialog(initial, parent=parent)
    if dialog.exec() != QDialog.DialogCode.Accepted:
        return None
    return dialog.to_config()
