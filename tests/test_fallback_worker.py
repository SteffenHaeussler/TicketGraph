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
from tests.test_agent_worker import FakePool, queued_task
from ticketflow import agent_worker, config, db, fallback_worker, taskqueue
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.models import TicketStatus


async def test_fallback_main_creates_unthrottled_fallback_worker(monkeypatch) -> None:
    pool = FakePool()
    captured: dict[str, Any] = {}

    async def fake_run_forever(
        pool_arg: object, activities: object, **kwargs: object
    ) -> None:
        captured["pool"] = pool_arg
        captured["activities"] = activities
        captured["kwargs"] = kwargs

    monkeypatch.setattr(fallback_worker, "setup_logging", lambda: None)
    monkeypatch.setattr(
        fallback_worker.db,
        "bootstrap",
        lambda: captured.setdefault("bootstrapped", True),
    )
    monkeypatch.setattr(fallback_worker.db, "make_pool", lambda: pool)
    monkeypatch.setattr(fallback_worker.agent_worker, "run_forever", fake_run_forever)

    await fallback_worker.main()

    assert captured["bootstrapped"] is True
    assert captured["pool"] is pool
    activities = captured["activities"]
    assert isinstance(activities, TicketActivities)
    assert isinstance(activities._agent, MockAgent)
    assert activities._agent._model == "fallback"
    assert captured["kwargs"]["queue_name"] == config.FALLBACK_TASK_QUEUE
    assert captured["kwargs"]["max_per_second"] is None
    assert pool.opened is True
    assert pool.closed is True


async def test_process_one_task_drains_fallback_queue(monkeypatch) -> None:
    ticket = make_ticket(id="ticket-fb", subject="Refund", body="I need my money")
    agent = ScriptedAgent(billing_classification(model="fallback"), refund_draft())
    task = queued_task(
        task_type="classify",
        workflow_id="ticket-fb",
        payload={"ticket": ticket.model_dump(mode="json")},
    )
    pool = FakePool()
    seen_queues: list[str] = []
    completed: list[tuple[int, dict[str, Any]]] = []
    woken: list[str] = []

    def dequeue(queue_name: str, worker_id: str, *, pool: object) -> db.QueuedTask:
        _ = (worker_id, pool)
        seen_queues.append(queue_name)
        return task

    monkeypatch.setattr(agent_worker.db, "dequeue", dequeue)
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
        pool,
        TicketActivities(agent),
        worker_id="fallback-1",
        queue_name=config.FALLBACK_TASK_QUEUE,
    )

    assert processed is True
    assert seen_queues == [config.FALLBACK_TASK_QUEUE]
    assert agent.classify_calls == 1
    assert completed and completed[0][0] == 7
    assert woken == ["ticket-fb"]


@pytest.mark.integration
async def test_postgres_fallback_task_completion_wakes_run(
    postgres_pool: db.ConnectionPool,
) -> None:
    ticket = make_ticket(id=f"t-fallback-worker-{uuid.uuid4().hex}")
    activities = TicketActivities(
        ScriptedAgent(billing_classification(model="fallback"), refund_draft())
    )
    future = datetime.now(UTC) + timedelta(hours=1)

    with postgres_pool.connection() as conn:
        taskqueue.enqueue(
            conn,
            queue_name=config.FALLBACK_TASK_QUEUE,
            task_type="classify",
            workflow_id=ticket.id,
            payload={"ticket": ticket.model_dump(mode="json")},
            idempotency_key=f"{ticket.id}:classify:fallback",
        )
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
            "VALUES (%s, %s, %s)",
            (ticket.id, TicketStatus.CLASSIFYING, future),
        )
        conn.commit()

    processed = await agent_worker.process_one_task(
        postgres_pool, activities, queue_name=config.FALLBACK_TASK_QUEUE
    )

    with postgres_pool.connection() as conn:
        row = conn.execute(
            "SELECT status, result FROM task_queue WHERE workflow_id = %s",
            (ticket.id,),
        ).fetchone()
    claimed = db.claim_run("runner-assert", pool=postgres_pool)

    assert processed is True
    assert row == (
        "done",
        {"category": "billing", "confidence": 0.9, "model": "fallback"},
    )
    assert claimed is not None
    assert claimed.ticket_id == ticket.id
