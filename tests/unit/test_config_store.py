from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QSettings
from threep_commons.paths import configure_qsettings

from prowlarr_ui.constants import APP_IDENTITY, SETTINGS_APP_NAME, SETTINGS_ORG_NAME
from prowlarr_ui.utils.config import ensure_config_exists, load_config, save_config


def _settings_file(config_dir: Path) -> Path:
    configure_qsettings(APP_IDENTITY, str(config_dir))
    settings = QSettings(
        QSettings.Format.IniFormat,
        QSettings.Scope.UserScope,
        SETTINGS_ORG_NAME,
        SETTINGS_APP_NAME,
    )
    settings.sync()
    return Path(settings.fileName())


def test_ensure_config_exists_seeds_config_namespace(
    monkeypatch, tmp_path: Path
) -> None:
    config_dir = tmp_path / "cfg"
    monkeypatch.setenv("CONFIG_DIR", str(config_dir))
    path = _settings_file(config_dir)
    if path.exists():
        path.unlink()

    ensure_config_exists()
    settings = QSettings(
        QSettings.Format.IniFormat,
        QSettings.Scope.UserScope,
        SETTINGS_ORG_NAME,
        SETTINGS_APP_NAME,
    )
    keys = settings.allKeys()

    assert keys
    assert all(key.startswith("config/") for key in keys)


def test_load_and_save_config_roundtrip(monkeypatch, tmp_path: Path) -> None:
    config_dir = tmp_path / "cfg"
    monkeypatch.setenv("CONFIG_DIR", str(config_dir))

    payload = {
        "prowlarr": {"host": "http://example:9696", "api_key": "abc"},
        "settings": {"prowlarr_page_size": 25, "everything_max_results": 3},
    }
    save_config(payload)
    loaded = load_config()

    assert loaded["prowlarr"]["host"] == "http://example:9696"
    assert loaded["prowlarr"]["api_key"] == "abc"
    assert loaded["settings"]["prowlarr_page_size"] == 25
    assert loaded["settings"]["everything_max_results"] == 3
