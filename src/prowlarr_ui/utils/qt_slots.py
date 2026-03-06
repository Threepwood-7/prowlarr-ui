from __future__ import annotations

import functools
import logging
import traceback
from typing import Any, Callable

logger = logging.getLogger(__name__)


def safe_slot(func: Callable[..., Any]) -> Callable[..., Any]:
    """Catch and log exceptions in Qt signal handlers."""

    @functools.wraps(func)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return func(self, *args, **kwargs)
        except Exception:
            tb = traceback.format_exc()
            logger.error(f"Exception in {func.__name__}:\n{tb}")
            if hasattr(self, "log"):
                self.log(f"ERROR in {func.__name__}: {tb}")
            return None

    return wrapper
