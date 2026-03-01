"""Configuration management utilities"""
import os
import tempfile
import tomlkit
import logging
from typing import Dict

logger = logging.getLogger(__name__)

# Anchor config path to project root (two levels up from src/utils/)
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'config.toml')


def get_default_config() -> Dict:
    """Return default configuration template"""
    return {
        'prowlarr': {
            'host': 'http://localhost:9696',
            'api_key': 'YOUR_API_KEY_HERE'
        },
        'settings': {
            'title_match_chars': 42,
            'everything_search_chars': 42,
            'everything_recheck_delay': 6000,  # Delay in ms before rechecking Everything after download
            'web_search_url': 'https://www.google.com/search?q={query}',
            'everything_integration_method': 'sdk',  # 'sdk', 'http', or 'none'
            'prowlarr_page_size': 100,  # Page size for each query
            'everything_max_results': 5
        }
    }


def ensure_config_exists(config_path: str = CONFIG_PATH) -> None:
    """Create config file if it doesn't exist"""
    if not os.path.exists(config_path):
        logger.warning(f"{config_path} not found, creating template")
        with open(config_path, 'w') as f:
            tomlkit.dump(get_default_config(), f)
        logger.info(f"Created template config at {config_path}")


def load_config(config_path: str = CONFIG_PATH) -> Dict:
    """Load configuration from TOML file (preserves comments for round-trip)"""
    try:
        with open(config_path, 'r') as f:
            config = tomlkit.load(f)
        logger.info(f"Loaded config from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        return get_default_config()
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return get_default_config()


def validate_config(config: Dict) -> list:
    """Validate config values and return a list of warning messages.
    Clamps out-of-range numeric values to valid defaults."""
    warnings = []

    # Check prowlarr section
    prowlarr = config.get('prowlarr', {})
    api_key = prowlarr.get('api_key', '')
    if not api_key or api_key == 'YOUR_API_KEY_HERE':
        warnings.append("Prowlarr API key is not set - update config.toml")

    host = prowlarr.get('host', '')
    if host and not (host.startswith('http://') or host.startswith('https://')):
        warnings.append(f"Prowlarr host should start with http:// or https:// (got: {host})")

    # Validate settings
    settings = config.get('settings', {})
    defaults = get_default_config()['settings']

    # Numeric range checks with clamping
    clamp_rules = {
        'title_match_chars': (1, 200, defaults.get('title_match_chars', 42)),
        'everything_search_chars': (1, 200, defaults.get('everything_search_chars', 42)),
        'prowlarr_page_size': (1, 10000, defaults.get('prowlarr_page_size', 100)),
        'everything_max_results': (1, 100, defaults.get('everything_max_results', 5)),
        'api_timeout': (1, 300, 30),
        'api_retries': (1, 10, 2),

        'everything_recheck_delay': (0, 60000, 4000),
        'everything_batch_size': (1, 1000, 10),
    }

    for key, (min_val, max_val, default_val) in clamp_rules.items():
        if key in settings:
            val = settings[key]
            if not isinstance(val, (int, float)):
                warnings.append(f"settings.{key} must be a number, using default ({default_val})")
                settings[key] = default_val
            elif val < min_val or val > max_val:
                clamped = max(min_val, min(max_val, val))
                warnings.append(f"settings.{key} = {val} out of range [{min_val}-{max_val}], clamped to {clamped}")
                settings[key] = clamped

    # Validate enum-like settings
    valid_everything_methods = ('sdk', 'http', 'none')
    if settings.get('everything_integration_method', 'sdk') not in valid_everything_methods:
        warnings.append(f"Invalid everything_integration_method, using 'sdk'")
        settings['everything_integration_method'] = 'sdk'

    return warnings


def save_config(config: Dict, config_path: str = CONFIG_PATH) -> None:
    """
    Save configuration to TOML file atomically (write to temp, then rename).
    Uses tomlkit to preserve comments and formatting from the original file.
    Temp file is created in same directory to ensure atomic replace works on Windows.
    """
    # Get directory of config file to ensure same filesystem
    config_dir = os.path.dirname(os.path.abspath(config_path)) or '.'

    try:
        # Create temp file in same directory (ensures same filesystem for atomic replace)
        with tempfile.NamedTemporaryFile(
            mode='w', dir=config_dir, delete=False, suffix='.tmp', prefix='.config_'
        ) as f:
            tmp_path = f.name
            tomlkit.dump(config, f)

        # Atomic replace (works reliably on Windows when same filesystem)
        os.replace(tmp_path, config_path)
        logger.debug(f"Config saved to {config_path}")
    except Exception as e:
        logger.error(f"Error saving config: {e}")
        # Clean up temp file on failure
        try:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError as remove_error:
            logger.error(f"Failed to remove temp file: {remove_error}")
