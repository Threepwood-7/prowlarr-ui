"""Utility functions for formatting data"""


def format_size(bytes_size) -> str:
    """Convert size in bytes to human-readable format"""
    if not bytes_size or bytes_size <= 0:
        return "0 B"

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} PB"


def format_age(days) -> str:
    """Convert age in days to readable format"""
    if days is None:
        return ""
    if days < 1:
        return "<1d"
    elif days < 365:
        return f"{days}d"
    else:
        years = days / 365.25
        return f"{years:.1f}y"
