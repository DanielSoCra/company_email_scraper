"""In-memory log capture for debug UI.

This module provides a custom logging handler that captures log entries
in a ring buffer, plus an endpoint to retrieve them for display in the UI.
"""

from __future__ import annotations

import logging
from collections import deque
from datetime import datetime
from typing import Any

# Ring buffer for recent log entries
_log_buffer: deque[dict[str, Any]] = deque(maxlen=500)

# Debug mode flag - can be toggled at runtime
DEBUG_UI_ENABLED = True


class DebugLogHandler(logging.Handler):
    """Custom handler that captures log records to an in-memory buffer."""

    def emit(self, record: logging.LogRecord) -> None:
        if not DEBUG_UI_ENABLED:
            return

        try:
            entry = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "level": record.levelname,
                "logger": record.name,
                "message": self.format(record),
            }
            _log_buffer.append(entry)
        except Exception:
            pass  # Never fail logging


def get_recent_logs(since_index: int = 0) -> tuple[list[dict[str, Any]], int]:
    """Get log entries added since the given index.

    Args:
        since_index: Only return entries after this index.

    Returns:
        Tuple of (list of log entries, current max index).
    """
    all_logs = list(_log_buffer)
    current_index = len(all_logs)

    if since_index >= current_index:
        return [], current_index

    return all_logs[since_index:], current_index


def clear_logs() -> None:
    """Clear the log buffer."""
    _log_buffer.clear()


def install_debug_handler() -> None:
    """Install the debug log handler on the root logger."""
    handler = DebugLogHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(name)s: %(message)s")
    handler.setFormatter(formatter)

    # Add to root logger to capture all logs
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
