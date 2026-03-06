"""Configuration management utilities."""

import logging
import os
import shutil
import tempfile
from pathlib import Path

import tomlkit

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
APP_SLUG = "prowlarr-ui"
DEFAULTS_CONFIG_PATH = PROJECT_ROOT / "config" / "app.defaults.toml"
EXAMPLE_CONFIG_PATH = PROJECT_ROOT / "config" / "app.example.toml"
LOCAL_CONFIG_PATH = PROJECT_ROOT / "config" / "app.local.toml"
APP_CONFIG_PATH_ENV = "APP_CONFIG_PATH"
APP_SECRETS_PATH_ENV = "APP_SECRETS_PATH"
SECRET_ENV_TO_KEYS = (
    ("PROWLARR_UI_API_KEY", ("prowlarr", "api_key")),
    ("PROWLARR_UI_HTTP_BASIC_AUTH_PASSWORD", ("prowlarr", "http_basic_auth_password")),
)


def _deep_merge_dicts(base: dict, overlay: dict) -> dict:
    merged: dict = dict(base)
    for key, value in overlay.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dicts(base_value, value)
        else:
            merged[key] = value
    return merged


def _set_nested_value(target: dict, key_path: tuple[str, ...], value: str) -> None:
    current = target
    for key in key_path[:-1]:
        next_value = current.get(key)
        if not isinstance(next_value, dict):
            next_value = {}
            current[key] = next_value
        current = next_value
    current[key_path[-1]] = value


def _resolve_config_path(config_path: str | None = None) -> Path:
    explicit_path = config_path or os.environ.get(APP_CONFIG_PATH_ENV, "")
    if explicit_path:
        path = Path(explicit_path).expanduser()
        return path if path.is_absolute() else PROJECT_ROOT / path
    return LOCAL_CONFIG_PATH


def _resolve_secrets_path() -> Path:
    explicit_path = os.environ.get(APP_SECRETS_PATH_ENV, "")
    if explicit_path:
        path = Path(explicit_path).expanduser()
        return path if path.is_absolute() else PROJECT_ROOT / path
    if os.name == "nt":
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / APP_SLUG / "secrets.toml"
        return Path.home() / "AppData" / "Roaming" / APP_SLUG / "secrets.toml"
    return Path.home() / ".config" / APP_SLUG / "secrets.toml"


def _load_optional_toml(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as f:
            loaded = tomlkit.load(f)
        return loaded if isinstance(loaded, dict) else {}
    except Exception as e:
        logger.error("Error loading config file %s: %s", path, e)
        return {}


def _apply_secret_env_overrides(config: dict) -> None:
    for env_name, key_path in SECRET_ENV_TO_KEYS:
        env_value = os.environ.get(env_name, "")
        if env_value:
            _set_nested_value(config, key_path, env_value)


def get_default_config() -> dict:
    """Return default configuration template."""
    return {
        "prowlarr": {"host": "http://localhost:9696", "api_key": "YOUR_API_KEY_HERE"},
        "settings": {
            "title_match_chars": 42,
            "everything_search_chars": 42,
            "everything_recheck_delay": 6000,  # Delay in ms before rechecking Everything after download
            "web_search_url": "https://www.google.com/search?q={query}",
            "everything_integration_method": "sdk",  # 'sdk', 'http', or 'none'
            "prowlarr_page_size": 100,  # Page size for each query
            "everything_max_results": 5,
            "api_timeout": 300,
            "api_retries": 2,
        },
    }


def ensure_config_exists(config_path: str | None = None) -> None:
    """Create local config file from template if it doesn't exist."""
    path = _resolve_config_path(config_path)
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if EXAMPLE_CONFIG_PATH.exists():
            shutil.copy2(EXAMPLE_CONFIG_PATH, path)
        elif DEFAULTS_CONFIG_PATH.exists():
            shutil.copy2(DEFAULTS_CONFIG_PATH, path)
        else:
            with path.open("w", encoding="utf-8") as f:
                tomlkit.dump(get_default_config(), f)
        logger.info("Created local config at %s", path)
    except Exception as e:
        logger.error("Failed to create local config at %s: %s", path, e)


def load_config(config_path: str | None = None) -> dict:
    """Load layered configuration from defaults/local/secrets/env."""
    path = _resolve_config_path(config_path)
    defaults = _load_optional_toml(DEFAULTS_CONFIG_PATH)
    local_config = _load_optional_toml(path)
    secrets_config = _load_optional_toml(_resolve_secrets_path())

    merged = _deep_merge_dicts(defaults, local_config)
    merged = _deep_merge_dicts(merged, secrets_config)
    _apply_secret_env_overrides(merged)

    if not merged:
        logger.error("No configuration found; using in-code defaults")
        return get_default_config()
    logger.info("Loaded config from %s", path)
    return merged


def validate_config(config: dict) -> list:
    """Validate config values and return warnings. Clamps numeric ranges."""
    warnings = []

    prowlarr = config.get("prowlarr", {})
    api_key = prowlarr.get("api_key", "")
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        warnings.append(
            "Prowlarr API key is not set - update config/app.local.toml or secret sources"
        )

    host = prowlarr.get("host", "")
    if host and not (host.startswith("http://") or host.startswith("https://")):
        warnings.append(f"Prowlarr host should start with http:// or https:// (got: {host})")

    settings = config.get("settings", {})
    defaults = get_default_config()["settings"]

    clamp_rules = {
        "title_match_chars": (1, 200, defaults.get("title_match_chars", 42)),
        "everything_search_chars": (1, 200, defaults.get("everything_search_chars", 42)),
        "prowlarr_page_size": (1, 10000, defaults.get("prowlarr_page_size", 100)),
        "everything_max_results": (1, 100, defaults.get("everything_max_results", 5)),
        "api_timeout": (1, 300, defaults.get("api_timeout", 300)),
        "api_retries": (0, 10, 2),
        "everything_recheck_delay": (0, 60000, 4000),
        "everything_batch_size": (1, 1000, 10),
        "download_queue_stale_grace_seconds": (0.1, 300.0, 20.0),
        "shutdown_force_after_seconds": (1.0, 300.0, 15.0),
        "shutdown_force_arm_seconds": (1.0, 60.0, 8.0),
        "everything_check_stale_grace_seconds": (0.1, 300.0, 20.0),
    }

    for key, (min_val, max_val, default_val) in clamp_rules.items():
        if key in settings:
            val = settings[key]
            if not isinstance(val, (int, float)):
                warnings.append(
                    f"settings.{key} must be a number, using default ({default_val})"
                )
                settings[key] = default_val
            elif val < min_val or val > max_val:
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


def save_config(config: dict, config_path: str | None = None) -> None:
    """
    Save configuration to TOML file atomically.

    Temp file is created in same directory to keep atomic replace semantics on Windows.
    """
    path = _resolve_config_path(config_path)
    config_dir = str(path.parent.resolve())
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=config_dir,
            delete=False,
            suffix=".tmp",
            prefix=".config_",
            encoding="utf-8",
        ) as f:
            tmp_path = f.name
            tomlkit.dump(config, f)

        os.replace(tmp_path, str(path))
        logger.debug("Config saved to %s", path)
    except Exception as e:
        logger.error("Error saving config: %s", e)
        try:
            if "tmp_path" in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError as remove_error:
            logger.error("Failed to remove temp file: %s", remove_error)
