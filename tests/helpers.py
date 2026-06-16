"""Test doubles and factories shared across test modules."""

import asyncio
import uuid
from contextlib import AsyncExitStack
from typing import cast

from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Worker, WorkflowRunner

from ticketflow import workflows
from ticketflow.activities import TicketActivities
from ticketflow.agent.base import Agent, AgentOverloadedError
from ticketflow.models import (
    ActionType,
    Classification,
    DraftReply,
    ProposedAction,
    Ticket,
    TicketCategory,
    TicketStatus,
    TicketStatusInfo,
)
from ticketflow.workflows import TicketWorkflow


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


class CombinedWorker:
    """Async context manager that runs related Temporal workers together."""

    def __init__(self, *workers: Worker):
        self._workers = workers
        self._stack = AsyncExitStack()

    async def __aenter__(self) -> "CombinedWorker":
        for worker in self._workers:
            await self._stack.enter_async_context(worker)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool | None:
        return await self._stack.__aexit__(exc_type, exc, tb)


def make_worker(
    client: Client,
    agent: Agent,
    task_queue: str,
    workflow_runner: WorkflowRunner | None = None,
    db_path: str | None = None,
) -> CombinedWorker:
    acts = TicketActivities(agent, db_path=db_path)
    workflow_activities = [
        acts.send_reply,
        acts.execute_refund,
        acts.record_result,
    ]
    agent_activities = [
        acts.classify_ticket,
        acts.draft_reply,
    ]
    if workflow_runner is not None:
        workflow_worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[TicketWorkflow],
            activities=workflow_activities,
            workflow_runner=workflow_runner,
        )
    else:
        workflow_worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[TicketWorkflow],
            activities=workflow_activities,
        )
    llm_worker = Worker(
        client,
        task_queue=workflows.AGENT_TASK_QUEUE,
        activities=agent_activities,
    )
    return CombinedWorker(workflow_worker, llm_worker)


async def wait_for_status(
    handle: WorkflowHandle, expected: TicketStatus, attempts: int = 100
) -> TicketStatusInfo:
    for _ in range(attempts):
        info = cast(TicketStatusInfo, await handle.query(TicketWorkflow.status))
        if info.status == expected:
            return info
        await asyncio.sleep(0.1)
    raise AssertionError(f"workflow never reached status {expected}")
