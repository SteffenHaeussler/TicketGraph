"""Postgres refund ledger: at-most-once refund effects keyed by ticket id."""

from typing import Any


def record_refund(conn: Any, ticket_id: str, amount: float, attempt: int) -> bool:
    """Log a refund attempt; return True only the first time a ticket is refunded.

    The ticket id is the idempotency key: duplicate deliveries land in
    refund_attempts, but the refund itself is recorded at most once. The caller
    owns the transaction and is responsible for committing.
    """
    conn.execute(
        "INSERT INTO refund_attempts (ticket_id, attempt) VALUES (%s, %s)",
        (ticket_id, attempt),
    )
    row = conn.execute(
        """
        INSERT INTO refunds (ticket_id, amount)
        VALUES (%s, %s)
        ON CONFLICT (ticket_id) DO NOTHING
        RETURNING ticket_id
        """,
        (ticket_id, amount),
    ).fetchone()
    return row is not None


def refund_recorded(conn: Any, ticket_id: str) -> bool:
    """Return True if a refund has been recorded for the ticket.

    Reflects durable at-most-once state rather than the "first time?" answer from
    :func:`record_refund`, so a finalize retry can still tell that money moved.
    """
    row = conn.execute(
        "SELECT 1 FROM refunds WHERE ticket_id = %s", (ticket_id,)
    ).fetchone()
    return row is not None
