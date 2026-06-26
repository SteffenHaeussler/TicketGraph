"""Read-model helpers for terminal ticket results and legacy refunds."""

from typing import Any

from psycopg.types.json import Jsonb

from ticketflow import db, ledger
from ticketflow.models import TicketResult


def save_result(
    result: TicketResult,
    *,
    database_url: str | None = None,
    pool: Any | None = None,
) -> None:
    """Store or replace a terminal ticket result in Postgres."""
    with db.managed_pool(database_url=database_url, pool=pool) as active_pool:
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


def record_refund(
    ticket_id: str,
    amount: float,
    attempt: int,
    *,
    database_url: str | None = None,
    pool: Any | None = None,
) -> bool:
    """Log a refund attempt; return True only the first time a ticket is refunded.

    The ticket id is the idempotency key: duplicate activity runs land in
    refund_attempts but the refund itself is recorded at most once.
    """
    with db.managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            first = ledger.record_refund(conn, ticket_id, amount, attempt)
            conn.commit()
    return first


def refund_recorded(
    ticket_id: str,
    *,
    database_url: str | None = None,
    pool: Any | None = None,
) -> bool:
    """Return whether a refund is durably recorded for the ticket."""
    with db.managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            return ledger.refund_recorded(conn, ticket_id)


def load_result(
    ticket_id: str,
    *,
    database_url: str | None = None,
    pool: Any | None = None,
) -> TicketResult | None:
    """Load a terminal ticket result from Postgres, if it exists."""
    with db.managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            row = conn.execute(
                "SELECT data FROM ticket_results WHERE ticket_id = %s", (ticket_id,)
            ).fetchone()
    return TicketResult.model_validate(row[0]) if row else None


def clear(
    *,
    database_url: str | None = None,
    pool: Any | None = None,
) -> int:
    """Remove all persisted ticket results from Postgres and return the count."""
    with db.managed_pool(database_url=database_url, pool=pool) as active_pool:
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
    assert row is not None
    return int(row[0])
