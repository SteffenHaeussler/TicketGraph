"""Application logging configuration."""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar, Token
from typing import TextIO

from ticketflow import config

SUPPORTED_FIELDS = {
    "time",
    "level",
    "logger",
    "message",
    "ticket_id",
    "task_queue",
    "module",
    "function",
    "line",
}

_ticket_id: ContextVar[str] = ContextVar("ticket_id", default="")


def set_ticket_context(ticket_id: str) -> Token[str]:
    """Set the current ticket id for structured log records."""
    return _ticket_id.set(ticket_id)


def reset_ticket_context(token: Token[str]) -> None:
    """Restore the previous ticket id logging context."""
    _ticket_id.reset(token)


class ContextFilter(logging.Filter):
    """Inject request-scoped Ticketflow fields into log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Populate missing context fields before formatting."""
        if not hasattr(record, "ticket_id"):
            record.ticket_id = _ticket_id.get()
        if not hasattr(record, "task_queue"):
            record.task_queue = ""
        return True


class JsonFormatter(logging.Formatter):
    """Format log records as JSON with a configurable field set."""

    def __init__(self, fields: list[str]):
        """Create a formatter for the selected fields."""
        super().__init__()
        self.fields = fields

    def format(self, record: logging.LogRecord) -> str:
        """Render a log record as a JSON object."""
        return json.dumps(
            {field: self._value(record, field) for field in self.fields},
            default=str,
        )

    def _value(self, record: logging.LogRecord, field: str) -> object:
        if field == "time":
            return self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z")
        if field == "level":
            return record.levelname
        if field == "logger":
            return record.name
        if field == "message":
            return record.getMessage()
        if field == "ticket_id":
            return getattr(record, "ticket_id", "")
        if field == "task_queue":
            return getattr(record, "task_queue", "")
        if field == "module":
            return record.module
        if field == "function":
            return record.funcName
        if field == "line":
            return record.lineno
        raise ValueError(f"Unsupported log field: {field}")


class TextFormatter(JsonFormatter):
    """Format log records as compact pipe-delimited text."""

    def format(self, record: logging.LogRecord) -> str:
        """Render a log record as text."""
        return " | ".join(str(self._value(record, field)) for field in self.fields)


def setup_logging(
    log_format: str | None = None,
    level: str | int | None = None,
    fields: list[str] | None = None,
    stream: TextIO | None = None,
) -> None:
    """Configure root logging for Ticketflow processes."""
    resolved_format = (log_format or config.LOG_FORMAT).lower()
    if resolved_format not in {"json", "text"}:
        raise ValueError(f"Unsupported log format: {resolved_format}")

    resolved_fields = fields or config.LOG_FIELDS
    unsupported = sorted(set(resolved_fields) - SUPPORTED_FIELDS)
    if unsupported:
        raise ValueError(f"Unsupported log field: {unsupported[0]}")

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.addFilter(ContextFilter())
    if resolved_format == "json":
        handler.setFormatter(JsonFormatter(resolved_fields))
    else:
        handler.setFormatter(TextFormatter(resolved_fields))

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level or config.LOG_LEVEL)
