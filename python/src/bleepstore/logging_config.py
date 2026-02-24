"""Structured logging configuration for BleepStore."""

import json
import logging
import sys
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects.

    Fields: timestamp, level, logger, message, plus any extras.
    """

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        # Include any extra fields attached to the record
        for key in ("method", "path", "status", "duration_ms", "request_id"):
            val = getattr(record, key, None)
            if val is not None:
                entry[key] = val
        return json.dumps(entry, default=str)


def configure_logging(level: str = "INFO", fmt: str = "text") -> None:
    """Configure root logging with the specified level and format.

    Args:
        level: Log level name (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        fmt: Format type — 'text' for human-readable, 'json' for structured.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Remove existing handlers
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(numeric_level)

    if fmt == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )

    root.addHandler(handler)
