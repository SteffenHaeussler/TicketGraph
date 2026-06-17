"""Durable Postgres-backed task queue helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from psycopg.types.json import Jsonb


def enqueue(
    conn: Any,
    *,
    queue_name: str,
    task_type: str,
    workflow_id: str,
    payload: Mapping[str, Any],
    idempotency_key: str,
    max_attempts: int = 3,
    available_at: datetime | None = None,
) -> int | None:
    """Insert a pending task unless its idempotency key already exists."""
    row = conn.execute(
        """
        INSERT INTO task_queue (
            queue_name,
            task_type,
            workflow_id,
            idempotency_key,
            payload,
            max_attempts,
            available_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, now()))
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING id
        """,
        (
            queue_name,
            task_type,
            workflow_id,
            idempotency_key,
            Jsonb(payload),
            max_attempts,
            available_at,
        ),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def complete(conn: Any, task_id: int, *, result: Mapping[str, Any]) -> str | None:
    """Mark a leased task done and store its result.

    Returns the resulting status (``"done"``), or ``None`` when no leased row
    matched -- e.g. the lease was already reclaimed by the janitor.
    """
    row = conn.execute(
        """
        UPDATE task_queue
        SET status = 'done',
            result = %s,
            error = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL
        WHERE id = %s AND status = 'leased'
        RETURNING status
        """,
        (Jsonb(result), task_id),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def fail(conn: Any, task_id: int, *, error: str) -> str | None:
    """Retry a leased task with exponential backoff, or mark it failed.

    If ``attempts < max_attempts`` and the task is not ``permanent`` the row
    returns to ``pending`` with ``available_at = now() + 2^attempts`` seconds;
    otherwise it becomes ``failed`` with ``error`` recorded. Returns the
    resulting status, or ``None`` when no leased row matched.
    """
    row = conn.execute(
        """
        UPDATE task_queue
        SET status = CASE
                WHEN attempts < max_attempts AND NOT permanent THEN 'pending'
                ELSE 'failed'
            END,
            available_at = CASE
                WHEN attempts < max_attempts AND NOT permanent
                THEN now() + interval '1 second' * power(2, attempts)
                ELSE available_at
            END,
            error = %s,
            lease_owner = NULL,
            lease_expires_at = NULL
        WHERE id = %s AND status = 'leased'
        RETURNING status, available_at
        """,
        (error, task_id),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def reclaim_expired(conn: Any) -> int:
    """Return expired leases to ``pending`` so they can be redelivered.

    Any task whose lease has elapsed (``status='leased'`` and
    ``lease_expires_at < now()``) goes back to ``pending`` with its lease
    released. Returns the number of tasks reclaimed. ``attempts`` is left as-is
    so a crashed worker's attempt still counts toward ``max_attempts``.
    """
    row = conn.execute(
        """
        WITH reclaimed AS (
            UPDATE task_queue
            SET status = 'pending',
                lease_owner = NULL,
                lease_expires_at = NULL
            WHERE status = 'leased' AND lease_expires_at < now()
            RETURNING id
        )
        SELECT count(*) FROM reclaimed
        """,
        (),
    ).fetchone()
    return int(row[0])
