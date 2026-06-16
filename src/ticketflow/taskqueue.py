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
