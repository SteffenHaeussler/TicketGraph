import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest

from ticketflow import db, taskqueue


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


def test_complete_marks_leased_task_done_with_result() -> None:
    conn = FakeConnection(rows=[("done",)])

    status = taskqueue.complete(conn, 7, result={"answer": "42"})

    assert status == "done"
    assert "SET status = 'done'" in conn.sql[0]
    assert "result = %s" in conn.sql[0]
    assert "WHERE id = %s AND status = 'leased'" in conn.sql[0]
    assert conn.params[0][1] == 7


def test_complete_returns_none_when_no_leased_row_matched() -> None:
    conn = FakeConnection(rows=[None])

    status = taskqueue.complete(conn, 7, result={"answer": "42"})

    assert status is None


def test_fail_retries_with_exponential_backoff() -> None:
    conn = FakeConnection(rows=[("pending", datetime(2026, 6, 16, 12, 0, tzinfo=UTC))])

    status = taskqueue.fail(conn, 7, error="boom")

    assert status == "pending"
    assert "attempts < max_attempts AND NOT permanent" in conn.sql[0]
    assert "power(2, attempts)" in conn.sql[0]
    assert "WHERE id = %s AND status = 'leased'" in conn.sql[0]
    assert conn.params[0] == ("boom", 7)


def test_fail_gives_up_returns_failed_status() -> None:
    conn = FakeConnection(rows=[("failed", datetime(2026, 6, 16, 12, 0, tzinfo=UTC))])

    status = taskqueue.fail(conn, 7, error="boom")

    assert status == "failed"


def test_fail_returns_none_when_no_leased_row_matched() -> None:
    conn = FakeConnection(rows=[None])

    status = taskqueue.fail(conn, 7, error="boom")

    assert status is None


def test_reclaim_expired_returns_count() -> None:
    conn = FakeConnection(rows=[(2,)])

    reclaimed = taskqueue.reclaim_expired(conn)

    assert reclaimed == 2
    assert "SET status = 'pending'" in conn.sql[0]
    assert "WHERE status = 'leased' AND lease_expires_at < now()" in conn.sql[0]
    assert "lease_owner = NULL" in conn.sql[0]


def test_cancel_pending_marks_task_failed_and_permanent() -> None:
    conn = FakeConnection(rows=[(1,)])

    cancelled = taskqueue.cancel_pending(
        conn, "ticket-123:classify", reason="redispatched to fallback"
    )

    assert cancelled is True
    assert "SET status = 'failed'" in conn.sql[0]
    assert "permanent = true" in conn.sql[0]
    assert "WHERE idempotency_key = %s AND status = 'pending'" in conn.sql[0]
    assert conn.params[0] == ("redispatched to fallback", "ticket-123:classify")


def test_cancel_pending_returns_false_when_no_pending_task_matched() -> None:
    conn = FakeConnection(rows=[None])

    cancelled = taskqueue.cancel_pending(
        conn, "ticket-123:classify", reason="redispatched to fallback"
    )

    assert cancelled is False


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


def _open_clean_pool() -> db.ConnectionPool:
    """Bootstrap, open a pool, and truncate the queue for an isolated test."""
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    with pool.connection() as conn:
        conn.execute("DELETE FROM task_queue")
        conn.commit()
    return pool


def _enqueue(conn: object, *, max_attempts: int = 5) -> int:
    task_id = taskqueue.enqueue(
        conn,
        queue_name="ticketflow-agent",
        task_type="classify_ticket",
        workflow_id="ticket-123",
        payload={"ticket_id": "ticket-123"},
        idempotency_key="ticket-123:classify",
        max_attempts=max_attempts,
    )
    assert task_id is not None
    return task_id


@pytest.mark.integration
def test_complete_marks_dequeued_task_done_against_real_postgres() -> None:
    pool = _open_clean_pool()
    try:
        with pool.connection() as conn:
            task_id = _enqueue(conn)
            conn.commit()

        task = db.dequeue("ticketflow-agent", "worker-1", pool=pool)
        assert task is not None and task.id == task_id

        with pool.connection() as conn:
            status = taskqueue.complete(conn, task.id, result={"answer": "42"})
            row = conn.execute(
                "SELECT status, result, lease_owner FROM task_queue WHERE id = %s",
                (task.id,),
            ).fetchone()
            conn.commit()
    finally:
        pool.close()

    assert status == "done"
    assert row == ("done", {"answer": "42"}, None)


@pytest.mark.integration
def test_fail_backoff_schedule_is_observable_against_real_postgres() -> None:
    pool = _open_clean_pool()
    try:
        with pool.connection() as conn:
            _enqueue(conn, max_attempts=3)
            conn.commit()

        # First attempt: dequeue increments attempts to 1, then fail retries.
        first = db.dequeue("ticketflow-agent", "worker-1", pool=pool)
        assert first is not None and first.attempts == 1
        with pool.connection() as conn:
            status1 = taskqueue.fail(conn, first.id, error="boom")
            first_row = conn.execute(
                """
                SELECT status, lease_owner,
                       extract(epoch FROM (available_at - now()))
                FROM task_queue WHERE id = %s
                """,
                (first.id,),
            ).fetchone()
            # Make it immediately due again so the next dequeue can lease it
            # without waiting out the real backoff delay.
            conn.execute(
                "UPDATE task_queue SET available_at = now() WHERE id = %s",
                (first.id,),
            )
            conn.commit()

        # Second attempt: attempts becomes 2, so backoff should grow.
        second = db.dequeue("ticketflow-agent", "worker-1", pool=pool)
        assert second is not None and second.attempts == 2
        with pool.connection() as conn:
            status2 = taskqueue.fail(conn, second.id, error="boom")
            second_gap = conn.execute(
                "SELECT extract(epoch FROM (available_at - now())) "
                "FROM task_queue WHERE id = %s",
                (second.id,),
            ).fetchone()
            conn.commit()
    finally:
        pool.close()

    assert status1 == "pending"
    assert status2 == "pending"
    assert first_row is not None and second_gap is not None
    assert first_row[0] == "pending"
    assert first_row[1] is None  # lease released on retry
    gap1 = float(first_row[2])
    gap2 = float(second_gap[0])
    assert 1.0 < gap1 < 3.0  # ~2s = power(2, 1)
    assert 3.0 < gap2 < 5.0  # ~4s = power(2, 2)
    assert gap2 > gap1


@pytest.mark.integration
def test_fail_gives_up_when_attempts_exhausted_against_real_postgres() -> None:
    pool = _open_clean_pool()
    try:
        with pool.connection() as conn:
            _enqueue(conn, max_attempts=1)
            conn.commit()

        task = db.dequeue("ticketflow-agent", "worker-1", pool=pool)
        assert task is not None and task.attempts == 1
        with pool.connection() as conn:
            status = taskqueue.fail(conn, task.id, error="boom")
            row = conn.execute(
                "SELECT status, error FROM task_queue WHERE id = %s", (task.id,)
            ).fetchone()
            conn.commit()
    finally:
        pool.close()

    assert status == "failed"
    assert row == ("failed", "boom")


@pytest.mark.integration
def test_fail_permanent_task_goes_straight_to_failed_against_real_postgres() -> None:
    pool = _open_clean_pool()
    try:
        with pool.connection() as conn:
            task_id = _enqueue(conn, max_attempts=5)
            conn.execute(
                "UPDATE task_queue SET permanent = true WHERE id = %s", (task_id,)
            )
            conn.commit()

        task = db.dequeue("ticketflow-agent", "worker-1", pool=pool)
        assert task is not None and task.attempts == 1  # well below max_attempts
        with pool.connection() as conn:
            status = taskqueue.fail(conn, task.id, error="nope")
            row = conn.execute(
                "SELECT status FROM task_queue WHERE id = %s", (task.id,)
            ).fetchone()
            conn.commit()
    finally:
        pool.close()

    assert status == "failed"
    assert row == ("failed",)


@pytest.mark.integration
def test_complete_and_fail_ignore_non_leased_rows_against_real_postgres() -> None:
    pool = _open_clean_pool()
    try:
        with pool.connection() as conn:
            _enqueue(conn)
            conn.commit()

        task = db.dequeue("ticketflow-agent", "worker-1", pool=pool)
        assert task is not None
        with pool.connection() as conn:
            taskqueue.complete(conn, task.id, result={"x": 1})
            conn.commit()

        # The row is now 'done'; neither op should touch it again.
        with pool.connection() as conn:
            again = taskqueue.complete(conn, task.id, result={"x": 2})
            failed = taskqueue.fail(conn, task.id, error="boom")
            row = conn.execute(
                "SELECT status, result, error FROM task_queue WHERE id = %s",
                (task.id,),
            ).fetchone()
            conn.commit()
    finally:
        pool.close()

    assert again is None
    assert failed is None
    assert row == ("done", {"x": 1}, None)


@pytest.mark.integration
def test_reclaim_expired_makes_dropped_lease_redeliverable() -> None:
    pool = _open_clean_pool()
    try:
        with pool.connection() as conn:
            task_id = _enqueue(conn)
            conn.commit()

        # Worker leases the task, then "crashes" without completing it.
        leased = db.dequeue("ticketflow-agent", "worker-1", pool=pool)
        assert leased is not None and leased.id == task_id
        assert leased.attempts == 1

        with pool.connection() as conn:
            conn.execute(
                "UPDATE task_queue SET lease_expires_at = now() - interval '1 second' "
                "WHERE id = %s",
                (task_id,),
            )
            conn.commit()

        with pool.connection() as conn:
            reclaimed = taskqueue.reclaim_expired(conn)
            row = conn.execute(
                "SELECT status, lease_owner FROM task_queue WHERE id = %s",
                (task_id,),
            ).fetchone()
            conn.commit()

        # The reclaimed task is redelivered to a fresh worker.
        redelivered = db.dequeue("ticketflow-agent", "worker-2", pool=pool)
    finally:
        pool.close()

    assert reclaimed == 1
    assert row == ("pending", None)
    assert redelivered is not None and redelivered.id == task_id
    assert redelivered.attempts == 2  # second delivery


@pytest.mark.integration
def test_reclaim_expired_leaves_live_leases_alone() -> None:
    pool = _open_clean_pool()
    try:
        with pool.connection() as conn:
            task_id = _enqueue(conn)
            conn.commit()

        # Lease is valid for ~30s, so it has not expired.
        leased = db.dequeue("ticketflow-agent", "worker-1", pool=pool)
        assert leased is not None and leased.id == task_id

        with pool.connection() as conn:
            reclaimed = taskqueue.reclaim_expired(conn)
            row = conn.execute(
                "SELECT status, lease_owner FROM task_queue WHERE id = %s",
                (task_id,),
            ).fetchone()
            conn.commit()
    finally:
        pool.close()

    assert reclaimed == 0
    assert row == ("leased", "worker-1")


@pytest.mark.integration
def test_concurrent_dequeue_delivers_each_task_once() -> None:
    task_count = 50
    worker_count = 8
    pool = _open_clean_pool()
    try:
        enqueued_ids: list[int] = []
        with pool.connection() as conn:
            for i in range(task_count):
                task_id = taskqueue.enqueue(
                    conn,
                    queue_name="ticketflow-agent",
                    task_type="classify_ticket",
                    workflow_id=f"ticket-{i}",
                    payload={"ticket_id": f"ticket-{i}"},
                    idempotency_key=f"ticket-{i}:classify",
                )
                assert task_id is not None
                enqueued_ids.append(task_id)
            conn.commit()

        # All workers start contending at once to exercise FOR UPDATE SKIP LOCKED.
        barrier = threading.Barrier(worker_count)

        def drain(worker_id: str) -> list[int]:
            barrier.wait()
            ids: list[int] = []
            while True:
                task = db.dequeue("ticketflow-agent", worker_id, pool=pool)
                if task is None:
                    break
                ids.append(task.id)
            return ids

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            results = list(
                executor.map(drain, [f"worker-{w}" for w in range(worker_count)])
            )
    finally:
        pool.close()

    delivered = [task_id for ids in results for task_id in ids]
    assert len(delivered) == task_count  # nothing lost
    assert len(set(delivered)) == task_count  # nothing delivered twice
    assert set(delivered) == set(enqueued_ids)  # exactly the enqueued tasks
