import io
import json
import logging

import pytest

from ticketflow.logging import reset_ticket_context, set_ticket_context, setup_logging


def test_json_logging_includes_only_configured_fields():
    stream = io.StringIO()
    setup_logging(
        log_format="json",
        level="INFO",
        fields=["level", "message", "ticket_id"],
        stream=stream,
    )

    logging.getLogger("ticketflow.test").info(
        "ticket created", extra={"ticket_id": "ticket-123", "task_queue": "ignored"}
    )

    payload = json.loads(stream.getvalue())
    assert payload == {
        "level": "INFO",
        "message": "ticket created",
        "ticket_id": "ticket-123",
    }


def test_text_logging_includes_configured_fields():
    stream = io.StringIO()
    setup_logging(
        log_format="text",
        level="INFO",
        fields=["level", "logger", "message", "task_queue"],
        stream=stream,
    )

    logging.getLogger("ticketflow.runner").info(
        "worker running", extra={"task_queue": "ticketflow"}
    )

    line = stream.getvalue().strip()
    assert "INFO" in line
    assert "ticketflow.runner" in line
    assert "worker running" in line
    assert "ticketflow" in line


def test_invalid_log_format_raises_value_error():
    with pytest.raises(ValueError, match="Unsupported log format"):
        setup_logging(log_format="xml")


def test_invalid_log_field_raises_value_error():
    with pytest.raises(ValueError, match="Unsupported log field"):
        setup_logging(fields=["level", "unknown"])


def test_ticket_context_is_scoped():
    stream = io.StringIO()
    setup_logging(
        log_format="json",
        level="INFO",
        fields=["message", "ticket_id"],
        stream=stream,
    )

    token = set_ticket_context("ticket-123")
    try:
        logging.getLogger("ticketflow.test").info("inside")
    finally:
        reset_ticket_context(token)
    logging.getLogger("ticketflow.test").info("outside")

    lines = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert lines == [
        {"message": "inside", "ticket_id": "ticket-123"},
        {"message": "outside", "ticket_id": ""},
    ]
