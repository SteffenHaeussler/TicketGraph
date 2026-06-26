"""Test doubles and factories shared across test modules."""

import uuid
from typing import Any

from ticketflow import agent_worker, config, side_effect_worker
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
    """Drive one real agent worker step against ``queue_name``.

    Lets M3.2 tests run a dispatch -> queue -> resume loop end to end. Pass
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
    """Drive one real in-process worker step for a queued task (plan M7.3).

    Delegates to :func:`agent_worker.process_one_task`, selecting the router by
    queue exactly as the production workers wire it: the default ``TASK_QUEUE``
    finalizes side effects via :func:`side_effect_worker.run_finalize`, while the
    agent and fallback queues use the default classify/draft router.
    """
    if queue_name == config.TASK_QUEUE:
        return await agent_worker.process_one_task(
            pool,
            activities,
            worker_id=worker_id,
            queue_name=queue_name,
            run_activity=side_effect_worker.run_finalize,
        )
    return await agent_worker.process_one_task(
        pool,
        activities,
        worker_id=worker_id,
        queue_name=queue_name,
    )


async def drive_until_quiescent(
    compiled: Any,
    pool: _Pool,
    activities: TicketActivities,
    ticket_id: str,
    *,
    worker_id: str = "runner-1",
    max_iterations: int = 50,
) -> None:
    """Interleave the real runner and worker steps until no work remains (M7.3).

    Stands in for the real runner + worker processes: each iteration advances the
    graph by one ready ``runner.step`` resume, otherwise drains one queued task
    through the real worker step (which wakes the run so the next ``runner.step``
    picks up the fresh result). Raises if quiescence is not reached within
    ``max_iterations``.
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
