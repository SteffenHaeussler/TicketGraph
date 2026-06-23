"""Clock abstraction for deterministic workflow timer tests."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    """A source of timezone-aware current time."""

    def now(self) -> datetime:
        """Return the current time."""
        raise NotImplementedError


class SystemClock:
    """Production clock backed by the system UTC time."""

    def now(self) -> datetime:
        """Return the current UTC time."""
        return datetime.now(UTC)


def resolve_clock(clock: Clock | None) -> Clock:
    """Return ``clock`` or the production system clock."""
    return clock or SystemClock()
