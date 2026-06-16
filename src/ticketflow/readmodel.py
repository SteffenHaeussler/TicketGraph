"""SQLite read model for final ticket results."""

import sqlite3
from pathlib import Path

from ticketflow import config
from ticketflow.models import TicketResult


def _resolve(db_path: str | None) -> str:
    return db_path if db_path is not None else config.DB_PATH


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS ticket_results ("
        "ticket_id TEXT PRIMARY KEY, data TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS refunds ("
        "ticket_id TEXT PRIMARY KEY, amount REAL NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS refund_attempts ("
        "ticket_id TEXT NOT NULL, attempt INTEGER NOT NULL)"
    )
    return conn


def save_result(result: TicketResult, db_path: str | None = None) -> None:
    """Store or replace a terminal ticket result."""
    conn = _connect(_resolve(db_path))
    try:
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO ticket_results (ticket_id, data) VALUES (?, ?)",
                (result.ticket_id, result.model_dump_json()),
            )
    finally:
        conn.close()


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


def load_result(ticket_id: str, db_path: str | None = None) -> TicketResult | None:
    """Load a terminal ticket result, if it exists."""
    path = _resolve(db_path)
    if not Path(path).exists():
        return None
    conn = _connect(path)
    try:
        row = conn.execute(
            "SELECT data FROM ticket_results WHERE ticket_id = ?", (ticket_id,)
        ).fetchone()
    finally:
        conn.close()
    return TicketResult.model_validate_json(row[0]) if row else None


def clear(db_path: str | None = None) -> int:
    """Remove all persisted ticket results and return the row count."""
    path = _resolve(db_path)
    if not Path(path).exists():
        return 0
    conn = _connect(path)
    try:
        with conn:
            cursor = conn.execute("DELETE FROM ticket_results")
            conn.execute("DELETE FROM refunds")
            conn.execute("DELETE FROM refund_attempts")
    finally:
        conn.close()
    return cursor.rowcount
