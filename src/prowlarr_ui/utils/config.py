"""Configuration management utilities backed by QSettings."""

from __future__ import annotations

import copy
import logging
import os
from typing import Any

from PySide6.QtCore import QSettings

logger = logging.getLogger(__name__)

APP_SLUG = "prowlarr-ui"
CONFIG_SETTINGS_ORG_NAME = "ProwlarrUI"
CONFIG_SETTINGS_APP_NAME = "Prowlarr Search Client Config"

SECRET_ENV_TO_KEYS = (
    ("PROWLARR_UI_API_KEY", ("prowlarr", "api_key")),
    ("PROWLARR_UI_HTTP_BASIC_AUTH_PASSWORD", ("prowlarr", "http_basic_auth_password")),
)

DEFAULT_CONFIG: dict[str, Any] = {
    "prowlarr": {
        "host": "http://localhost:9696",
        "api_key": "YOUR_API_KEY_HERE",
        "http_basic_auth_username": "",
        "http_basic_auth_password": "",
    },
    "settings": {
        "title_match_chars": 42,
        "everything_search_chars": 42,
        "everything_recheck_delay": 6000,
        "web_search_url": "https://www.google.com/search?q={query}",
        "everything_integration_method": "sdk",
        "prowlarr_page_size": 100,
        "everything_max_results": 5,
        "everything_batch_size": 10,
        "api_timeout": 300,
        "api_retries": 2,
        "everything_sdk_url": "",
        "download_queue_stale_grace_seconds": 20.0,
        "shutdown_force_after_seconds": 15.0,
        "shutdown_force_arm_seconds": 8.0,
        "everything_check_stale_grace_seconds": 20.0,
        "custom_command_F2": "",
        "custom_command_F3": "",
        "custom_command_F4": "",
    },
}

# (key, expected_type, default)
CONFIG_SCHEMA: tuple[tuple[str, type, Any], ...] = (
    ("prowlarr/host", str, DEFAULT_CONFIG["prowlarr"]["host"]),
    ("prowlarr/api_key", str, DEFAULT_CONFIG["prowlarr"]["api_key"]),
    (
        "prowlarr/http_basic_auth_username",
        str,
        DEFAULT_CONFIG["prowlarr"]["http_basic_auth_username"],
    ),
    (
        "prowlarr/http_basic_auth_password",
        str,
        DEFAULT_CONFIG["prowlarr"]["http_basic_auth_password"],
    ),
    ("settings/title_match_chars", int, DEFAULT_CONFIG["settings"]["title_match_chars"]),
    (
        "settings/everything_search_chars",
        int,
        DEFAULT_CONFIG["settings"]["everything_search_chars"],
    ),
    (
        "settings/everything_recheck_delay",
        int,
        DEFAULT_CONFIG["settings"]["everything_recheck_delay"],
    ),
    ("settings/web_search_url", str, DEFAULT_CONFIG["settings"]["web_search_url"]),
    (
        "settings/everything_integration_method",
        str,
        DEFAULT_CONFIG["settings"]["everything_integration_method"],
    ),
    (
        "settings/prowlarr_page_size",
        int,
        DEFAULT_CONFIG["settings"]["prowlarr_page_size"],
    ),
    (
        "settings/everything_max_results",
        int,
        DEFAULT_CONFIG["settings"]["everything_max_results"],
    ),
    (
        "settings/everything_batch_size",
        int,
        DEFAULT_CONFIG["settings"]["everything_batch_size"],
    ),
    ("settings/api_timeout", int, DEFAULT_CONFIG["settings"]["api_timeout"]),
    ("settings/api_retries", int, DEFAULT_CONFIG["settings"]["api_retries"]),
    (
        "settings/everything_sdk_url",
        str,
        DEFAULT_CONFIG["settings"]["everything_sdk_url"],
    ),
    (
        "settings/download_queue_stale_grace_seconds",
        float,
        DEFAULT_CONFIG["settings"]["download_queue_stale_grace_seconds"],
    ),
    (
        "settings/shutdown_force_after_seconds",
        float,
        DEFAULT_CONFIG["settings"]["shutdown_force_after_seconds"],
    ),
    (
        "settings/shutdown_force_arm_seconds",
        float,
        DEFAULT_CONFIG["settings"]["shutdown_force_arm_seconds"],
    ),
    (
        "settings/everything_check_stale_grace_seconds",
        float,
        DEFAULT_CONFIG["settings"]["everything_check_stale_grace_seconds"],
    ),
    (
        "settings/custom_command_F2",
        str,
        DEFAULT_CONFIG["settings"]["custom_command_F2"],
    ),
    (
        "settings/custom_command_F3",
        str,
        DEFAULT_CONFIG["settings"]["custom_command_F3"],
    ),
    (
        "settings/custom_command_F4",
        str,
        DEFAULT_CONFIG["settings"]["custom_command_F4"],
    ),
)


def _new_config_settings() -> QSettings:
    return QSettings(
        QSettings.Format.IniFormat,
        QSettings.Scope.UserScope,
        CONFIG_SETTINGS_ORG_NAME,
        CONFIG_SETTINGS_APP_NAME,
    )


def config_store_file_path() -> str:
    settings = _new_config_settings()
    settings.sync()
    return str(settings.fileName() or "").strip()


def _set_nested_value(target: dict[str, Any], key_path: tuple[str, ...], value: Any) -> None:
    current: dict[str, Any] = target
    for key in key_path[:-1]:
        next_value = current.get(key)
        if not isinstance(next_value, dict):
            next_value = {}
            current[key] = next_value
        current = next_value
    current[key_path[-1]] = value


def _deep_merge_dicts(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in overlay.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dicts(base_value, value)
        else:
            merged[key] = value
    return merged


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes", "on"}:
            return True
        if token in {"0", "false", "no", "off"}:
            return False
    return bool(default)


def _coerce_value(value: Any, expected_type: type, default: Any) -> Any:
    if expected_type is bool:
        return _coerce_bool(value, bool(default))
    if expected_type is int:
        try:
            return int(value)
        except (TypeError, ValueError, OverflowError):
            return int(default)
    if expected_type is float:
        try:
            return float(value)
        except (TypeError, ValueError, OverflowError):
            return float(default)
    if value is None:
        return default
    return str(value)


def _apply_secret_env_overrides(config: dict[str, Any]) -> None:
    for env_name, key_path in SECRET_ENV_TO_KEYS:
        env_value = os.environ.get(env_name, "")
        if env_value:
            _set_nested_value(config, key_path, env_value)


def get_default_config() -> dict[str, Any]:
    """Return default configuration template."""
    return copy.deepcopy(DEFAULT_CONFIG)


def ensure_config_exists() -> None:
    """Seed missing keys in QSettings with in-code defaults."""
    settings = _new_config_settings()
    changed = False
    for key, expected_type, default in CONFIG_SCHEMA:
        if settings.contains(key):
            continue
        settings.setValue(key, _coerce_value(default, expected_type, default))
        changed = True
    if changed:
        settings.sync()


def load_config() -> dict[str, Any]:
    """Load typed configuration from QSettings and apply env overrides."""
    settings = _new_config_settings()
    merged = get_default_config()
    for key, expected_type, default in CONFIG_SCHEMA:
        raw = settings.value(key, default)
        _set_nested_value(merged, tuple(key.split("/")), _coerce_value(raw, expected_type, default))
    _apply_secret_env_overrides(merged)
    return merged


def save_config(config: dict[str, Any]) -> None:
    """Persist known config keys to QSettings and sync immediately."""
    merged = _deep_merge_dicts(get_default_config(), config if isinstance(config, dict) else {})
    settings = _new_config_settings()

    for key, expected_type, default in CONFIG_SCHEMA:
        path = tuple(key.split("/"))
        current: Any = merged
        for part in path:
            if not isinstance(current, dict):
                current = default
                break
            current = current.get(part, default)
        settings.setValue(key, _coerce_value(current, expected_type, default))

    settings.sync()


def get_missing_required_config(config: dict[str, Any]) -> list[str]:
    """Return required-field validation failures for startup wizard gating."""
    missing: list[str] = []
    prowlarr = config.get("prowlarr", {}) if isinstance(config, dict) else {}
    host = str(prowlarr.get("host", "") if isinstance(prowlarr, dict) else "").strip()
    api_key = str(prowlarr.get("api_key", "") if isinstance(prowlarr, dict) else "").strip()
    if not host:
        missing.append("prowlarr.host is required")
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        missing.append("prowlarr.api_key is required")
    return missing


def validate_config(config: dict[str, Any]) -> list[str]:
    """Validate config values and return warnings. Clamps numeric ranges."""
    warnings: list[str] = []

    prowlarr = config.get("prowlarr", {})
    api_key = prowlarr.get("api_key", "") if isinstance(prowlarr, dict) else ""
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        warnings.append("Prowlarr API key is not set")

    host = prowlarr.get("host", "") if isinstance(prowlarr, dict) else ""
    if host and not (str(host).startswith("http://") or str(host).startswith("https://")):
        warnings.append(f"Prowlarr host should start with http:// or https:// (got: {host})")

    settings = config.get("settings", {})
    if not isinstance(settings, dict):
        config["settings"] = {}
        settings = config["settings"]

    defaults = get_default_config()["settings"]

    clamp_rules = {
        "title_match_chars": (1, 200, defaults.get("title_match_chars", 42)),
        "everything_search_chars": (1, 200, defaults.get("everything_search_chars", 42)),
        "prowlarr_page_size": (1, 10000, defaults.get("prowlarr_page_size", 100)),
        "everything_max_results": (1, 100, defaults.get("everything_max_results", 5)),
        "api_timeout": (1, 300, defaults.get("api_timeout", 300)),
        "api_retries": (0, 10, defaults.get("api_retries", 2)),
        "everything_recheck_delay": (0, 60000, defaults.get("everything_recheck_delay", 6000)),
        "everything_batch_size": (1, 1000, defaults.get("everything_batch_size", 10)),
        "download_queue_stale_grace_seconds": (0.1, 300.0, defaults.get("download_queue_stale_grace_seconds", 20.0)),
        "shutdown_force_after_seconds": (1.0, 300.0, defaults.get("shutdown_force_after_seconds", 15.0)),
        "shutdown_force_arm_seconds": (1.0, 60.0, defaults.get("shutdown_force_arm_seconds", 8.0)),
        "everything_check_stale_grace_seconds": (0.1, 300.0, defaults.get("everything_check_stale_grace_seconds", 20.0)),
    }

    for key, (min_val, max_val, default_val) in clamp_rules.items():
        if key not in settings:
            continue
        val = settings[key]
        if not isinstance(val, (int, float)):
            warnings.append(f"settings.{key} must be numeric, using default ({default_val})")
            settings[key] = default_val
            continue
        if val < min_val or val > max_val:
            clamped = max(min_val, min(max_val, val))
            warnings.append(
                f"settings.{key} = {val} out of range [{min_val}-{max_val}], clamped to {clamped}"
            )
            settings[key] = clamped

    valid_everything_methods = ("sdk", "http", "none")
    if settings.get("everything_integration_method", "sdk") not in valid_everything_methods:
        warnings.append("Invalid everything_integration_method, using 'sdk'")
        settings["everything_integration_method"] = "sdk"

    return warnings
