"""Test doubles and factories shared across test modules."""

import uuid

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
)


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
    """Lease one task from ``queue_name``, run it, and store the result.

    A stand-in for the real agent worker (Milestone 5) so M3.2 tests can drive a
    dispatch -> queue -> resume loop end to end. Pass
    ``queue_name=config.FALLBACK_TASK_QUEUE`` to drain the schedule-to-start
    fallback queue (M3.4). Returns ``True`` if a task was processed, ``False`` if
    the queue was empty.
    """
    task = db.dequeue(queue_name, worker_id, pool=pool)
    if task is None:
        return False

    ticket = Ticket.model_validate(task.payload["ticket"])
    if task.task_type == "classify":
        result = await activities.classify_ticket(ticket)
    elif task.task_type == "draft":
        classification = Classification.model_validate(task.payload["classification"])
        result = await activities.draft_reply(ticket, classification)
    else:  # pragma: no cover - defensive
        raise ValueError(f"unexpected task_type {task.task_type!r}")

    with pool.connection() as conn:
        taskqueue.complete(conn, task.id, result=result.model_dump(mode="json"))
        conn.commit()
    return True


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
