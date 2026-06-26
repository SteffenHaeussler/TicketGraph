"""Postgres ledgers for at-most-once terminal effects keyed by ticket id."""

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


def record_sent_reply(
    conn: Any,
    ticket_id: str,
    customer_email: str,
    reply_text: str,
    attempt: int,
) -> bool:
    """Log a reply attempt; return True only the first time a ticket is replied.

    The ticket id is the idempotency key: duplicate deliveries land in
    reply_attempts, but the sent reply itself is recorded at most once. The
    caller owns the transaction and is responsible for committing.
    """
    conn.execute(
        "INSERT INTO reply_attempts (ticket_id, attempt) VALUES (%s, %s)",
        (ticket_id, attempt),
    )
    row = conn.execute(
        """
        INSERT INTO sent_replies (ticket_id, customer_email, reply_text)
        VALUES (%s, %s, %s)
        ON CONFLICT (ticket_id) DO NOTHING
        RETURNING ticket_id
        """,
        (ticket_id, customer_email, reply_text),
    ).fetchone()
    return row is not None
