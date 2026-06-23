import dataclasses
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from tests.helpers import make_ticket, refund_draft, reply_only_draft
from tests.test_agent_worker import FakePool, queued_task
from ticketflow import (
    agent_worker,
    config,
    db,
    readmodel,
    side_effect_worker,
    taskqueue,
)
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.models import (
    ProposedAction,
    Ticket,
    TicketResult,
    TicketStatus,
)


class RecordingActivities(TicketActivities):
    """Activities that record side effects without touching the database."""

    def __init__(self, *, refund_first: bool = True) -> None:
        super().__init__(MockAgent())
        self.refund_first = refund_first
        self.refund_calls: list[tuple[str, float, int]] = []
        self.sent_replies: list[tuple[str, str]] = []
        self.recorded: list[TicketResult] = []

    async def execute_refund(
        self, ticket_id: str, amount: float, attempt: int = 1
    ) -> bool:
        self.refund_calls.append((ticket_id, amount, attempt))
        return self.refund_first

    async def send_reply(self, ticket: Ticket, reply_text: str) -> None:
        self.sent_replies.append((ticket.id, reply_text))

    async def record_result(self, result: TicketResult) -> None:
        self.recorded.append(result)


def _finalize_task(
    *,
    ticket: Ticket,
    action: ProposedAction,
    result: TicketResult,
    attempts: int = 1,
) -> db.QueuedTask:
    task = queued_task(
        task_type="finalize_ticket",
        workflow_id=ticket.id,
        payload={
            "ticket": ticket.model_dump(mode="json"),
            "action": action.model_dump(mode="json"),
            "result": result.model_dump(mode="json"),
        },
    )
    return dataclasses.replace(task, attempts=attempts)


async def test_main_creates_unthrottled_side_effect_worker(monkeypatch) -> None:
    pool = FakePool()
    captured: dict[str, Any] = {}

    async def fake_run_forever(
        pool_arg: object, activities: object, **kwargs: object
    ) -> None:
        captured["pool"] = pool_arg
        captured["activities"] = activities
        captured["kwargs"] = kwargs

    monkeypatch.setattr(side_effect_worker, "setup_logging", lambda: None)
    monkeypatch.setattr(
        side_effect_worker.db,
        "bootstrap",
        lambda: captured.setdefault("bootstrapped", True),
    )
    monkeypatch.setattr(side_effect_worker.db, "make_pool", lambda: pool)
    monkeypatch.setattr(
        side_effect_worker.agent_worker, "run_forever", fake_run_forever
    )

    await side_effect_worker.main()

    assert captured["bootstrapped"] is True
    assert captured["pool"] is pool
    activities = captured["activities"]
    assert isinstance(activities, TicketActivities)
    kwargs = captured["kwargs"]
    assert kwargs["queue_name"] == config.TASK_QUEUE
    assert kwargs["max_per_second"] is None
    assert kwargs["max_concurrent"] == config.AGENT_MAX_CONCURRENT
    assert kwargs["run_activity"] is side_effect_worker.run_finalize
    assert pool.opened is True
    assert pool.closed is True


async def test_run_finalize_executes_refund_for_resolved_refund() -> None:
    ticket = make_ticket(id="ticket-refund")
    draft = refund_draft(amount=42.0)
    result = TicketResult(
        ticket_id=ticket.id,
        status=TicketStatus.RESOLVED,
        reply_text=draft.reply_text,
    )
    task = _finalize_task(ticket=ticket, action=draft.action, result=result, attempts=3)
    activities = RecordingActivities(refund_first=True)

    out = await side_effect_worker.run_finalize(task, activities)

    assert activities.refund_calls == [(ticket.id, 42.0, 3)]
    assert activities.sent_replies == [(ticket.id, draft.reply_text)]
    assert activities.recorded == [out]
    assert out.refund_executed is True


async def test_run_finalize_skips_refund_for_reply_only() -> None:
    ticket = make_ticket(id="ticket-reply")
    draft = reply_only_draft()
    result = TicketResult(
        ticket_id=ticket.id,
        status=TicketStatus.RESOLVED,
        reply_text=draft.reply_text,
    )
    task = _finalize_task(ticket=ticket, action=draft.action, result=result)
    activities = RecordingActivities()

    out = await side_effect_worker.run_finalize(task, activities)

    assert activities.refund_calls == []
    assert activities.sent_replies == [(ticket.id, draft.reply_text)]
    assert out.refund_executed is False


async def test_run_finalize_skips_refund_when_not_resolved() -> None:
    ticket = make_ticket(id="ticket-escalated")
    draft = refund_draft(amount=42.0)
    result = TicketResult(
        ticket_id=ticket.id,
        status=TicketStatus.ESCALATED,
        reply_text=draft.reply_text,
    )
    task = _finalize_task(ticket=ticket, action=draft.action, result=result)
    activities = RecordingActivities()

    out = await side_effect_worker.run_finalize(task, activities)

    assert activities.refund_calls == []
    assert out.refund_executed is False


async def test_run_finalize_rejects_unexpected_task_type() -> None:
    task = queued_task(task_type="classify", payload={})

    with pytest.raises(ValueError, match="unexpected side-effect task_type"):
        await side_effect_worker.run_finalize(task, RecordingActivities())


@pytest.mark.integration
async def test_postgres_finalize_runs_side_effects_and_wakes_run(
    postgres_pool: db.ConnectionPool,
) -> None:
    ticket = make_ticket(id=f"t-side-effect-{uuid.uuid4().hex}")
    draft = refund_draft(amount=42.0)
    result = TicketResult(
        ticket_id=ticket.id,
        status=TicketStatus.RESOLVED,
        reply_text=draft.reply_text,
    )
    activities = TicketActivities(MockAgent(), database_url=config.DATABASE_URL)
    future = datetime.now(UTC) + timedelta(hours=1)

    with postgres_pool.connection() as conn:
        taskqueue.enqueue(
            conn,
            queue_name=config.TASK_QUEUE,
            task_type="finalize_ticket",
            workflow_id=ticket.id,
            payload={
                "ticket": ticket.model_dump(mode="json"),
                "action": draft.action.model_dump(mode="json"),
                "result": result.model_dump(mode="json"),
            },
            idempotency_key=f"{ticket.id}:finalize",
        )
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
            "VALUES (%s, %s, %s)",
            (ticket.id, TicketStatus.RESOLVED, future),
        )
        conn.commit()

    processed = await agent_worker.process_one_task(
        postgres_pool,
        activities,
        queue_name=config.TASK_QUEUE,
        run_activity=side_effect_worker.run_finalize,
    )

    with postgres_pool.connection() as conn:
        task_row = conn.execute(
            "SELECT status, result FROM task_queue WHERE workflow_id = %s",
            (ticket.id,),
        ).fetchone()
        refund_row = conn.execute(
            "SELECT amount FROM refunds WHERE ticket_id = %s", (ticket.id,)
        ).fetchone()
    stored = readmodel.load_result(ticket.id, database_url=config.DATABASE_URL)
    claimed = db.claim_run("runner-assert", pool=postgres_pool)

    assert processed is True
    assert task_row is not None
    assert task_row[0] == "done"
    assert task_row[1]["refund_executed"] is True
    assert refund_row == (42.0,)
    assert claimed is not None
    assert claimed.ticket_id == ticket.id
    assert stored is not None
    assert stored.refund_executed is True
