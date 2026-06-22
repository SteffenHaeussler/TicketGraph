import asyncio
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from tests.helpers import (
    ScriptedAgent,
    billing_classification,
    make_ticket,
    refund_draft,
)
from ticketflow import agent_worker, config, db, taskqueue
from ticketflow.activities import TicketActivities
from ticketflow.agent.base import AgentOverloadedError, AgentPermanentError
from ticketflow.models import Classification, Ticket, TicketStatus


class FakeConnection:
    def __init__(self) -> None:
        self.commits = 0

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def commit(self) -> None:
        self.commits += 1


class FakePool:
    def __init__(self) -> None:
        self.connection_obj = FakeConnection()
        self.opened = False
        self.closed = False

    def connection(self, timeout: float | None = None) -> FakeConnection:
        _ = timeout
        return self.connection_obj

    def open(self) -> None:
        self.opened = True

    def close(self) -> None:
        self.closed = True


def queued_task(
    *,
    task_type: str,
    workflow_id: str = "ticket-123",
    payload: dict[str, Any] | None = None,
) -> db.QueuedTask:
    now = datetime(2026, 6, 22, 12, 0, tzinfo=UTC)
    return db.QueuedTask(
        id=7,
        queue_name=config.AGENT_TASK_QUEUE,
        task_type=task_type,
        workflow_id=workflow_id,
        payload=payload or {},
        idempotency_key=f"{workflow_id}:{task_type}",
        status="leased",
        attempts=1,
        max_attempts=3,
        available_at=now,
        enqueued_at=now,
        lease_owner="worker-1",
        lease_expires_at=now + timedelta(seconds=30),
        result=None,
        error=None,
        permanent=False,
    )


async def test_process_one_task_returns_false_when_queue_empty(monkeypatch) -> None:
    pool = FakePool()
    monkeypatch.setattr(agent_worker.db, "dequeue", lambda *args, **kwargs: None)

    processed = await agent_worker.process_one_task(
        pool, TicketActivities(ScriptedAgent(billing_classification(), refund_draft()))
    )

    assert processed is False
    assert pool.connection_obj.commits == 0


async def test_process_one_task_completes_classify_and_wakes_run(monkeypatch) -> None:
    ticket = make_ticket(id="ticket-123", subject="Refund", body="I need my money")
    agent = ScriptedAgent(billing_classification(model="primary"), refund_draft())
    task = queued_task(
        task_type="classify", payload={"ticket": ticket.model_dump(mode="json")}
    )
    pool = FakePool()
    completed: list[tuple[int, dict[str, Any]]] = []
    woken: list[str] = []

    monkeypatch.setattr(agent_worker.db, "dequeue", lambda *args, **kwargs: task)
    monkeypatch.setattr(
        agent_worker.taskqueue,
        "complete",
        lambda conn, task_id, *, result: completed.append((task_id, result)) or "done",
    )

    def wake_run(ticket_id: str, *, pool: object) -> None:
        _ = pool
        woken.append(ticket_id)

    monkeypatch.setattr(agent_worker.db, "wake_run", wake_run)

    processed = await agent_worker.process_one_task(
        pool, TicketActivities(agent), worker_id="worker-1"
    )

    assert processed is True
    assert agent.classify_calls == 1
    assert completed == [
        (
            7,
            {
                "category": "billing",
                "confidence": pytest.approx(0.9),
                "model": "primary",
            },
        )
    ]
    assert pool.connection_obj.commits == 1
    assert woken == ["ticket-123"]


async def test_process_one_task_completes_draft_and_wakes_run(monkeypatch) -> None:
    ticket = make_ticket(id="ticket-123")
    classification = billing_classification()
    draft = refund_draft(amount=12.34)
    agent = ScriptedAgent(classification, draft)
    task = queued_task(
        task_type="draft",
        payload={
            "ticket": ticket.model_dump(mode="json"),
            "classification": classification.model_dump(mode="json"),
        },
    )
    pool = FakePool()
    completed: list[dict[str, Any]] = []
    woken: list[str] = []

    monkeypatch.setattr(agent_worker.db, "dequeue", lambda *args, **kwargs: task)
    monkeypatch.setattr(
        agent_worker.taskqueue,
        "complete",
        lambda conn, task_id, *, result: completed.append(result) or "done",
    )

    def wake_run(ticket_id: str, *, pool: object) -> None:
        _ = pool
        woken.append(ticket_id)

    monkeypatch.setattr(agent_worker.db, "wake_run", wake_run)

    processed = await agent_worker.process_one_task(
        pool, TicketActivities(agent), worker_id="worker-1"
    )

    assert processed is True
    assert agent.draft_calls == 1
    assert completed == [draft.model_dump(mode="json")]
    assert pool.connection_obj.commits == 1
    assert woken == ["ticket-123"]


async def test_process_one_task_fails_unknown_task_and_wakes_run(monkeypatch) -> None:
    task = queued_task(task_type="finalize_ticket", payload={"ticket": {}})
    pool = FakePool()
    failures: list[tuple[int, str, bool]] = []
    woken: list[str] = []

    monkeypatch.setattr(agent_worker.db, "dequeue", lambda *args, **kwargs: task)
    monkeypatch.setattr(
        agent_worker.taskqueue,
        "fail",
        lambda conn, task_id, *, error, permanent=False: (
            failures.append((task_id, error, permanent)) or "failed"
        ),
    )

    def wake_run(ticket_id: str, *, pool: object) -> None:
        _ = pool
        woken.append(ticket_id)

    monkeypatch.setattr(agent_worker.db, "wake_run", wake_run)

    processed = await agent_worker.process_one_task(
        pool, TicketActivities(ScriptedAgent(billing_classification(), refund_draft()))
    )

    assert processed is True
    assert failures == [(7, "unexpected agent task_type 'finalize_ticket'", False)]
    assert pool.connection_obj.commits == 1
    assert woken == ["ticket-123"]


async def test_agent_exception_fails_without_waking(monkeypatch) -> None:
    class FailingAgent(ScriptedAgent):
        async def classify(self, ticket: Ticket) -> Classification:
            raise AgentOverloadedError("backend overloaded")

    ticket = make_ticket(id="ticket-123")
    task = queued_task(
        task_type="classify", payload={"ticket": ticket.model_dump(mode="json")}
    )
    pool = FakePool()
    failures: list[tuple[int, str, bool]] = []
    woken: list[str] = []

    monkeypatch.setattr(agent_worker.db, "dequeue", lambda *args, **kwargs: task)
    monkeypatch.setattr(
        agent_worker.taskqueue,
        "fail",
        lambda conn, task_id, *, error, permanent=False: (
            failures.append((task_id, error, permanent)) or "pending"
        ),
    )

    def wake_run(ticket_id: str, *, pool: object) -> None:
        _ = pool
        woken.append(ticket_id)

    monkeypatch.setattr(agent_worker.db, "wake_run", wake_run)

    processed = await agent_worker.process_one_task(
        pool,
        TicketActivities(FailingAgent(billing_classification(), refund_draft())),
    )

    assert processed is True
    assert failures == [(7, "backend overloaded", False)]
    assert pool.connection_obj.commits == 1
    assert woken == []


async def test_agent_permanent_error_marks_task_permanent_and_wakes_run(
    monkeypatch,
) -> None:
    class FailingAgent(ScriptedAgent):
        async def classify(self, ticket: Ticket) -> Classification:
            raise AgentPermanentError("invalid ticket input")

    ticket = make_ticket(id="ticket-123")
    task = queued_task(
        task_type="classify", payload={"ticket": ticket.model_dump(mode="json")}
    )
    pool = FakePool()
    failures: list[tuple[int, str, bool]] = []
    woken: list[str] = []

    monkeypatch.setattr(agent_worker.db, "dequeue", lambda *args, **kwargs: task)
    monkeypatch.setattr(
        agent_worker.taskqueue,
        "fail",
        lambda conn, task_id, *, error, permanent=False: (
            failures.append((task_id, error, permanent)) or "failed"
        ),
    )

    def wake_run(ticket_id: str, *, pool: object) -> None:
        _ = pool
        woken.append(ticket_id)

    monkeypatch.setattr(agent_worker.db, "wake_run", wake_run)

    processed = await agent_worker.process_one_task(
        pool,
        TicketActivities(FailingAgent(billing_classification(), refund_draft())),
    )

    assert processed is True
    assert failures == [(7, "invalid ticket input", True)]
    assert pool.connection_obj.commits == 1
    assert woken == ["ticket-123"]


async def test_agent_exception_wakes_run_when_retries_exhausted(monkeypatch) -> None:
    class FailingAgent(ScriptedAgent):
        async def classify(self, ticket: Ticket) -> Classification:
            raise AgentOverloadedError("backend overloaded")

    ticket = make_ticket(id="ticket-123")
    task = queued_task(
        task_type="classify", payload={"ticket": ticket.model_dump(mode="json")}
    )
    pool = FakePool()
    woken: list[str] = []

    monkeypatch.setattr(agent_worker.db, "dequeue", lambda *args, **kwargs: task)
    monkeypatch.setattr(
        agent_worker.taskqueue,
        "fail",
        lambda conn, task_id, *, error, permanent=False: "failed",
    )

    def wake_run(ticket_id: str, *, pool: object) -> None:
        _ = pool
        woken.append(ticket_id)

    monkeypatch.setattr(agent_worker.db, "wake_run", wake_run)

    processed = await agent_worker.process_one_task(
        pool,
        TicketActivities(FailingAgent(billing_classification(), refund_draft())),
    )

    assert processed is True
    assert woken == ["ticket-123"]


async def test_token_bucket_allows_first_token_then_waits_for_next() -> None:
    bucket = agent_worker.TokenBucket(rate_per_second=20)

    first_start = time.perf_counter()
    await bucket.acquire()
    first_elapsed = time.perf_counter() - first_start

    second_start = time.perf_counter()
    await bucket.acquire()
    second_elapsed = time.perf_counter() - second_start

    assert first_elapsed < 0.02
    assert second_elapsed >= 0.035


async def test_run_forever_limits_concurrent_processing(monkeypatch) -> None:
    stop = asyncio.Event()
    active = 0
    max_active = 0
    starts = 0

    async def fake_process_one_task(*args: object, **kwargs: object) -> bool:
        nonlocal active, max_active, starts
        active += 1
        starts += 1
        max_active = max(max_active, active)
        if starts >= 5:
            stop.set()
        await asyncio.sleep(0.01)
        active -= 1
        return True

    monkeypatch.setattr(agent_worker, "process_one_task", fake_process_one_task)

    await agent_worker.run_forever(
        FakePool(),
        TicketActivities(ScriptedAgent(billing_classification(), refund_draft())),
        worker_id="worker-1",
        max_per_second=1000,
        max_concurrent=2,
        poll_interval=0,
        stop=stop,
    )

    assert starts >= 5
    assert max_active <= 2


async def test_run_forever_unthrottled_skips_token_bucket(monkeypatch) -> None:
    stop = asyncio.Event()
    active = 0
    max_active = 0
    starts = 0

    def reject_token_bucket(*args: object, **kwargs: object) -> object:
        raise AssertionError("TokenBucket must not be built when unthrottled")

    monkeypatch.setattr(agent_worker, "TokenBucket", reject_token_bucket)

    async def fake_process_one_task(*args: object, **kwargs: object) -> bool:
        nonlocal active, max_active, starts
        active += 1
        starts += 1
        max_active = max(max_active, active)
        if starts >= 5:
            stop.set()
        await asyncio.sleep(0.01)
        active -= 1
        return True

    monkeypatch.setattr(agent_worker, "process_one_task", fake_process_one_task)

    await agent_worker.run_forever(
        FakePool(),
        TicketActivities(ScriptedAgent(billing_classification(), refund_draft())),
        worker_id="fallback-1",
        max_per_second=None,
        max_concurrent=2,
        poll_interval=0,
        stop=stop,
    )

    assert starts >= 5
    assert max_active <= 2


async def test_run_forever_unthrottled_backs_off_when_idle(monkeypatch) -> None:
    stop = asyncio.Event()
    sleeps: list[float] = []
    calls = 0

    monkeypatch.setattr(agent_worker, "TokenBucket", None)

    async def empty_process_one_task(*args: object, **kwargs: object) -> bool:
        nonlocal calls
        calls += 1
        return False

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) >= 3:
            stop.set()

    monkeypatch.setattr(agent_worker, "process_one_task", empty_process_one_task)
    monkeypatch.setattr(agent_worker.asyncio, "sleep", fake_sleep)

    await agent_worker.run_forever(
        FakePool(),
        TicketActivities(ScriptedAgent(billing_classification(), refund_draft())),
        worker_id="fallback-1",
        max_per_second=None,
        max_concurrent=2,
        poll_interval=0.5,
        stop=stop,
    )

    assert sleeps and all(delay == 0.5 for delay in sleeps)


async def test_main_creates_primary_mock_agent_worker(monkeypatch) -> None:
    pool = FakePool()
    captured: dict[str, Any] = {}

    async def fake_run_forever(
        pool_arg: object, activities: object, **kwargs: object
    ) -> None:
        captured["pool"] = pool_arg
        captured["activities"] = activities
        captured["kwargs"] = kwargs

    monkeypatch.setattr(agent_worker, "setup_logging", lambda: None)
    monkeypatch.setattr(
        agent_worker.db,
        "bootstrap",
        lambda: captured.setdefault("bootstrapped", True),
    )
    monkeypatch.setattr(agent_worker.db, "make_pool", lambda: pool)
    monkeypatch.setattr(agent_worker, "run_forever", fake_run_forever)

    await agent_worker.main()

    assert captured["bootstrapped"] is True
    assert captured["pool"] is pool
    assert isinstance(captured["activities"], TicketActivities)
    assert captured["kwargs"]["queue_name"] == config.AGENT_TASK_QUEUE
    assert pool.opened is True
    assert pool.closed is True


@pytest.mark.integration
async def test_postgres_primary_task_completion_wakes_run() -> None:
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    ticket = make_ticket(id=f"t-agent-worker-{uuid.uuid4().hex}")
    activities = TicketActivities(
        ScriptedAgent(billing_classification(model="primary"), refund_draft())
    )
    future = datetime.now(UTC) + timedelta(hours=1)

    try:
        with pool.connection() as conn:
            conn.execute("DELETE FROM task_queue")
            conn.execute("DELETE FROM workflow_run")
            taskqueue.enqueue(
                conn,
                queue_name=config.AGENT_TASK_QUEUE,
                task_type="classify",
                workflow_id=ticket.id,
                payload={"ticket": ticket.model_dump(mode="json")},
                idempotency_key=f"{ticket.id}:classify",
            )
            conn.execute(
                "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
                "VALUES (%s, %s, %s)",
                (ticket.id, TicketStatus.CLASSIFYING, future),
            )
            conn.commit()

        processed = await agent_worker.process_one_task(pool, activities)

        with pool.connection() as conn:
            row = conn.execute(
                "SELECT status, result FROM task_queue WHERE workflow_id = %s",
                (ticket.id,),
            ).fetchone()
            run_row = conn.execute(
                "SELECT wakeup_at <= now() FROM workflow_run WHERE ticket_id = %s",
                (ticket.id,),
            ).fetchone()
    finally:
        pool.close()

    assert processed is True
    assert row == (
        "done",
        {"category": "billing", "confidence": 0.9, "model": "primary"},
    )
    assert run_row == (True,)
