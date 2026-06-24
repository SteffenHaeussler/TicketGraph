"""Ticketflow package startup defaults."""

from __future__ import annotations

import os

_TRUTHY_ENV_VALUES = {"1", "true", "t", "yes", "y", "on"}


def _env_is_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _TRUTHY_ENV_VALUES


def _disable_langsmith_tracing_by_default() -> None:
    if _env_is_truthy("TICKETFLOW_LANGSMITH_TRACING"):
        return

    os.environ["LANGSMITH_TRACING_V2"] = "false"
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ.pop("LANGCHAIN_TRACING", None)
    os.environ.pop("LANGCHAIN_HANDLER", None)


_disable_langsmith_tracing_by_default()
