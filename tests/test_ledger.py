"""Tests for the Postgres refund ledger."""

import pytest

from ticketflow import db, ledger


class FakeCursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row


class FakeConnection:
    def __init__(self, rows: list[tuple[object, ...] | None]) -> None:
        self.rows = rows
        self.sql: list[str] = []
        self.params: list[tuple[object, ...]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> FakeCursor:
        self.sql.append(sql)
        self.params.append(params)
        return FakeCursor(self.rows.pop(0))


def test_record_refund_returns_true_on_first_insert() -> None:
    conn = FakeConnection(rows=[None, ("t-1",)])

    first = ledger.record_refund(conn, "t-1", 42.0, attempt=1)

    assert first is True
    assert "ON CONFLICT (ticket_id) DO NOTHING" in conn.sql[1]
    assert "RETURNING ticket_id" in conn.sql[1]
    assert conn.params[1] == ("t-1", 42.0)


def test_record_refund_returns_false_on_conflict() -> None:
    conn = FakeConnection(rows=[None, None])

    second = ledger.record_refund(conn, "t-1", 42.0, attempt=2)

    assert second is False


def test_record_refund_logs_attempt_before_refund() -> None:
    conn = FakeConnection(rows=[None, ("t-1",)])

    ledger.record_refund(conn, "t-1", 42.0, attempt=3)

    assert "INSERT INTO refund_attempts" in conn.sql[0]
    assert conn.params[0] == ("t-1", 3)


def test_refund_recorded_true_when_row_exists() -> None:
    conn = FakeConnection(rows=[("1",)])

    assert ledger.refund_recorded(conn, "t-1") is True
    assert "FROM refunds" in conn.sql[0]
    assert conn.params[0] == ("t-1",)


def test_refund_recorded_false_when_no_row() -> None:
    conn = FakeConnection(rows=[None])

    assert ledger.refund_recorded(conn, "t-1") is False


@pytest.mark.integration
def test_refund_recorded_reflects_ledger_state_against_real_postgres(
    postgres_pool: db.ConnectionPool,
) -> None:
    with postgres_pool.connection() as conn:
        before = ledger.refund_recorded(conn, "t-1")
        ledger.record_refund(conn, "t-1", 42.0, attempt=1)
        after = ledger.refund_recorded(conn, "t-1")
        conn.commit()

    assert before is False
    assert after is True


def test_record_sent_reply_returns_true_on_first_insert() -> None:
    conn = FakeConnection(rows=[None, ("t-1",)])

    first = ledger.record_sent_reply(
        conn,
        "t-1",
        "customer@example.com",
        "We handled it.",
        attempt=1,
    )

    assert first is True
    assert "ON CONFLICT (ticket_id) DO NOTHING" in conn.sql[1]
    assert "RETURNING ticket_id" in conn.sql[1]
    assert conn.params[1] == ("t-1", "customer@example.com", "We handled it.")


def test_record_sent_reply_returns_false_on_conflict() -> None:
    conn = FakeConnection(rows=[None, None])

    second = ledger.record_sent_reply(
        conn,
        "t-1",
        "customer@example.com",
        "We handled it.",
        attempt=2,
    )

    assert second is False


def test_record_sent_reply_logs_attempt_before_reply_insert() -> None:
    conn = FakeConnection(rows=[None, ("t-1",)])

    ledger.record_sent_reply(
        conn,
        "t-1",
        "customer@example.com",
        "We handled it.",
        attempt=3,
    )

    assert "INSERT INTO reply_attempts" in conn.sql[0]
    assert conn.params[0] == ("t-1", 3)


@pytest.mark.integration
def test_record_refund_is_at_most_once_against_real_postgres(
    postgres_pool: db.ConnectionPool,
) -> None:
    with postgres_pool.connection() as conn:
        first = ledger.record_refund(conn, "t-1", 42.0, attempt=1)
        second = ledger.record_refund(conn, "t-1", 42.0, attempt=2)
        refunds = conn.execute(
            "SELECT count(*) FROM refunds WHERE ticket_id = %s", ("t-1",)
        ).fetchone()
        attempts = conn.execute(
            "SELECT count(*) FROM refund_attempts WHERE ticket_id = %s", ("t-1",)
        ).fetchone()
        conn.commit()

    assert first is True
    assert second is False
    assert refunds == (1,)
    assert attempts == (2,)


@pytest.mark.integration
def test_record_refund_different_tickets_both_execute(
    postgres_pool: db.ConnectionPool,
) -> None:
    with postgres_pool.connection() as conn:
        first = ledger.record_refund(conn, "t-1", 42.0, attempt=1)
        second = ledger.record_refund(conn, "t-2", 13.0, attempt=1)
        conn.commit()

    assert first is True
    assert second is True


@pytest.mark.integration
def test_record_sent_reply_is_at_most_once_against_real_postgres(
    postgres_pool: db.ConnectionPool,
) -> None:
    with postgres_pool.connection() as conn:
        first = ledger.record_sent_reply(
            conn,
            "t-1",
            "customer@example.com",
            "We handled it.",
            attempt=1,
        )
        second = ledger.record_sent_reply(
            conn,
            "t-1",
            "customer@example.com",
            "We handled it.",
            attempt=2,
        )
        sent = conn.execute(
            "SELECT count(*) FROM sent_replies WHERE ticket_id = %s", ("t-1",)
        ).fetchone()
        attempts = conn.execute(
            "SELECT count(*) FROM reply_attempts WHERE ticket_id = %s", ("t-1",)
        ).fetchone()
        conn.commit()

    assert first is True
    assert second is False
    assert sent == (1,)
    assert attempts == (2,)
