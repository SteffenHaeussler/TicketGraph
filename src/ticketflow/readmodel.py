"""Read-model helpers for terminal ticket results and legacy refunds."""

import sqlite3
from typing import Any

from psycopg.types.json import Jsonb

from ticketflow import config, db
from ticketflow.models import TicketResult


def _resolve(db_path: str | None) -> str:
    return db_path if db_path is not None else config.DB_PATH


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS refunds ("
        "ticket_id TEXT PRIMARY KEY, amount REAL NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS refund_attempts ("
        "ticket_id TEXT NOT NULL, attempt INTEGER NOT NULL)"
    )
    return conn


def save_result(
    result: TicketResult,
    *,
    database_url: str | None = None,
    pool: Any | None = None,
) -> None:
    """Store or replace a terminal ticket result in Postgres."""
    owned_pool = pool is None
    active_pool = pool or db.make_pool(database_url)
    try:
        if owned_pool:
            active_pool.open()
        with active_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO ticket_results (ticket_id, data)
                VALUES (%s, %s)
                ON CONFLICT (ticket_id) DO UPDATE
                SET data = EXCLUDED.data
                """,
                (result.ticket_id, Jsonb(result.model_dump(mode="json"))),
            )
            conn.commit()
    finally:
        if owned_pool:
            active_pool.close()


def record_refund(
    ticket_id: str, amount: float, attempt: int, db_path: str | None = None
) -> bool:
    """Log a refund attempt; return True only the first time a ticket is refunded.

    The ticket id is the idempotency key: duplicate activity runs land in
    refund_attempts but the refund itself is recorded at most once.
    """
    conn = _connect(_resolve(db_path))
    try:
        with conn:
            conn.execute(
                "INSERT INTO refund_attempts (ticket_id, attempt) VALUES (?, ?)",
                (ticket_id, attempt),
            )
            cursor = conn.execute(
                "INSERT OR IGNORE INTO refunds (ticket_id, amount) VALUES (?, ?)",
                (ticket_id, amount),
            )
            return cursor.rowcount == 1
    finally:
        conn.close()


def load_result(
    ticket_id: str,
    *,
    database_url: str | None = None,
    pool: Any | None = None,
) -> TicketResult | None:
    """Load a terminal ticket result from Postgres, if it exists."""
    owned_pool = pool is None
    active_pool = pool or db.make_pool(database_url)
    try:
        if owned_pool:
            active_pool.open()
        with active_pool.connection() as conn:
            row = conn.execute(
                "SELECT data FROM ticket_results WHERE ticket_id = %s", (ticket_id,)
            ).fetchone()
    finally:
        if owned_pool:
            active_pool.close()
    return TicketResult.model_validate(row[0]) if row else None


def clear(
    *,
    database_url: str | None = None,
    pool: Any | None = None,
) -> int:
    """Remove all persisted ticket results from Postgres and return the count."""
    owned_pool = pool is None
    active_pool = pool or db.make_pool(database_url)
    try:
        if owned_pool:
            active_pool.open()
        with active_pool.connection() as conn:
            row = conn.execute(
                """
                WITH deleted AS (
                    DELETE FROM ticket_results
                    RETURNING ticket_id
                )
                SELECT count(*) FROM deleted
                """
            ).fetchone()
            conn.commit()
    finally:
        if owned_pool:
            active_pool.close()
    assert row is not None
    return int(row[0])
