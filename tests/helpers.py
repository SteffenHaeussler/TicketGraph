"""Test doubles and factories shared across test modules."""

import uuid
from datetime import datetime, timedelta
from typing import Any

from ticketflow import config, db, taskqueue
from ticketflow.activities import TicketActivities
from ticketflow.agent.base import AgentOverloadedError
from ticketflow.db import _Pool
from ticketflow.models import (
    ActionType,
    Classification,
    DraftReply,
    ProposedAction,
    Ticket,
    TicketCategory,
    TicketResult,
    TicketStatus,
)


class FrozenClock:
    """Advanceable clock for deterministic timer tests."""

    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now += delta


def make_ticket(**overrides: object) -> Ticket:
    defaults: dict[str, object] = {
        "id": uuid.uuid4().hex,
        "customer_email": "jo@example.com",
        "subject": "Help",
        "body": "Something broke",
    }
    defaults.update(overrides)
    return Ticket.model_validate(defaults)


def billing_classification(
    confidence: float = 0.9, model: str = "primary"
) -> Classification:
    return Classification(
        category=TicketCategory.BILLING, confidence=confidence, model=model
    )


def refund_draft(
    amount: float = 42.0, confidence: float = 0.9, model: str = "primary"
) -> DraftReply:
    return DraftReply(
        reply_text="We can refund you.",
        action=ProposedAction(type=ActionType.REFUND, refund_amount=amount),
        confidence=confidence,
        model=model,
    )


def reply_only_draft(confidence: float = 0.9, model: str = "primary") -> DraftReply:
    return DraftReply(
        reply_text="Try restarting the app.",
        action=ProposedAction(type=ActionType.REPLY_ONLY),
        confidence=confidence,
        model=model,
    )


async def process_one_agent_task(
    pool: _Pool,
    activities: TicketActivities,
    worker_id: str = "w",
    queue_name: str = config.AGENT_TASK_QUEUE,
) -> bool:
    """Lease one agent task from ``queue_name``, run it, and store the result.

    A stand-in for the real agent worker (Milestone 5) so M3.2 tests can drive a
    dispatch -> queue -> resume loop end to end. Pass
    ``queue_name=config.FALLBACK_TASK_QUEUE`` to drain the schedule-to-start
    fallback queue (M3.4). Returns ``True`` if a task was processed, ``False`` if
    the queue was empty.
    """
    return await process_one_task(pool, activities, worker_id, queue_name)


async def process_one_task(
    pool: _Pool,
    activities: TicketActivities,
    worker_id: str = "w",
    queue_name: str = config.AGENT_TASK_QUEUE,
) -> bool:
    """Process one queued task from ``queue_name`` in graph integration tests."""
    task = db.dequeue(queue_name, worker_id, pool=pool)
    if task is None:
        return False

    ticket = Ticket.model_validate(task.payload["ticket"])
    if task.task_type == "classify":
        result = await activities.classify_ticket(ticket)
    elif task.task_type == "draft":
        classification = Classification.model_validate(task.payload["classification"])
        result = await activities.draft_reply(ticket, classification)
    elif task.task_type == "finalize_ticket":
        action = ProposedAction.model_validate(task.payload["action"])
        result = TicketResult.model_validate(task.payload["result"])
        refund_executed = False
        if result.status == TicketStatus.RESOLVED and action.type == ActionType.REFUND:
            assert action.refund_amount is not None
            refund_executed = await activities.execute_refund(
                ticket.id, action.refund_amount, attempt=1
            )
        await activities.send_reply(ticket, result.reply_text)
        result = result.model_copy(update={"refund_executed": refund_executed})
        await activities.record_result(result)
    else:  # pragma: no cover - defensive
        raise ValueError(f"unexpected task_type {task.task_type!r}")

    with pool.connection() as conn:
        taskqueue.complete(conn, task.id, result=result.model_dump(mode="json"))
        conn.commit()
    return True


async def drive_until_quiescent(
    compiled: Any,
    pool: _Pool,
    activities: TicketActivities,
    ticket_id: str,
    *,
    worker_id: str = "runner-1",
    max_iterations: int = 50,
) -> None:
    """Interleave ``runner.step`` with the worker stub until no work remains.

    Stands in for the real runner + worker processes (plan M7.3): each iteration
    advances the graph by one ready resume, otherwise drains one queued task and
    wakes the run so the next ``runner.step`` can pick up the fresh result. Raises
    if quiescence is not reached within ``max_iterations``.
    """
    from ticketflow import runner

    for _ in range(max_iterations):
        if await runner.step(compiled, pool, worker_id):
            continue
        produced = False
        for queue_name in (
            config.AGENT_TASK_QUEUE,
            config.FALLBACK_TASK_QUEUE,
            config.TASK_QUEUE,
        ):
            if await process_one_task(pool, activities, queue_name=queue_name):
                produced = True
                break
        if not produced:
            return
        db.wake_run(ticket_id, pool=pool)
    raise AssertionError(f"run {ticket_id} did not reach quiescence")


class ScriptedAgent:
    """Agent stub returning fixed responses; counts calls."""

    def __init__(self, classification: Classification, draft: DraftReply):
        self.classification = classification
        self.draft = draft
        self.classify_calls = 0
        self.draft_calls = 0

    async def classify(self, ticket: Ticket) -> Classification:
        self.classify_calls += 1
        return self.classification

    async def draft_reply(
        self, ticket: Ticket, classification: Classification
    ) -> DraftReply:
        self.draft_calls += 1
        return self.draft


class FlakyAgent:
    """Fails the first `failures` classify calls, then delegates to `inner`."""

    def __init__(self, inner: ScriptedAgent, failures: int):
        self.inner = inner
        self.remaining = failures
        self.classify_calls = 0

    async def classify(self, ticket: Ticket) -> Classification:
        self.classify_calls += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise AgentOverloadedError("flaky")
        return await self.inner.classify(ticket)

    async def draft_reply(
        self, ticket: Ticket, classification: Classification
    ) -> DraftReply:
        return await self.inner.draft_reply(ticket, classification)
