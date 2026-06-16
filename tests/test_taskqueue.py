from datetime import UTC, datetime

import pytest

from ticketflow import db, taskqueue


class FakeCursor:
    def __init__(self, row: tuple[int] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[int] | None:
        return self.row


class FakeConnection:
    def __init__(self, rows: list[tuple[int] | None]) -> None:
        self.rows = rows
        self.sql: list[str] = []
        self.params: list[tuple[object, ...]] = []

    def execute(self, sql: str, params: tuple[object, ...]) -> FakeCursor:
        self.sql.append(sql)
        self.params.append(params)
        return FakeCursor(self.rows.pop(0))


def test_enqueue_inserts_task_with_idempotency_key() -> None:
    conn = FakeConnection(rows=[(42,)])
    available_at = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)

    task_id = taskqueue.enqueue(
        conn,
        queue_name="ticketflow-agent",
        task_type="classify_ticket",
        workflow_id="ticket-123",
        payload={"ticket_id": "ticket-123"},
        idempotency_key="ticket-123:classify",
        max_attempts=5,
        available_at=available_at,
    )

    assert task_id == 42
    assert "ON CONFLICT (idempotency_key) DO NOTHING" in conn.sql[0]
    assert "RETURNING id" in conn.sql[0]
    assert conn.params[0][0:4] == (
        "ticketflow-agent",
        "classify_ticket",
        "ticket-123",
        "ticket-123:classify",
    )
    assert conn.params[0][5:] == (5, available_at)


def test_enqueue_returns_none_when_idempotency_key_already_exists() -> None:
    conn = FakeConnection(rows=[None])

    task_id = taskqueue.enqueue(
        conn,
        queue_name="ticketflow-agent",
        task_type="classify_ticket",
        workflow_id="ticket-123",
        payload={"ticket_id": "ticket-123"},
        idempotency_key="ticket-123:classify",
    )

    assert task_id is None


@pytest.mark.integration
def test_enqueue_is_idempotent_against_real_postgres() -> None:
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    try:
        with pool.connection() as conn:
            conn.execute("DELETE FROM task_queue")
            first_id = taskqueue.enqueue(
                conn,
                queue_name="ticketflow-agent",
                task_type="classify_ticket",
                workflow_id="ticket-123",
                payload={"ticket_id": "ticket-123"},
                idempotency_key="ticket-123:classify",
            )
            second_id = taskqueue.enqueue(
                conn,
                queue_name="ticketflow-agent",
                task_type="classify_ticket",
                workflow_id="ticket-123",
                payload={"ticket_id": "ticket-123"},
                idempotency_key="ticket-123:classify",
            )
            row = conn.execute(
                "SELECT count(*) FROM task_queue WHERE idempotency_key = %s",
                ("ticket-123:classify",),
            ).fetchone()
            conn.commit()
    finally:
        pool.close()

    assert first_id is not None
    assert second_id is None
    assert row == (1,)
