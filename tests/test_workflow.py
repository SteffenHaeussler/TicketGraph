import asyncio
import uuid
from contextlib import AsyncExitStack

import pytest
from temporalio import activity
from temporalio.client import WorkflowUpdateFailedError, WorkflowUpdateStage
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from tests.helpers import (
    FlakyAgent,
    ScriptedAgent,
    billing_classification,
    make_ticket,
    make_worker,
    refund_draft,
    reply_only_draft,
    wait_for_status,
)
from ticketflow import readmodel
from ticketflow.activities import TicketActivities
from ticketflow.agent.base import AgentOverloadedError, AgentPermanentError
from ticketflow.models import ApprovalDecision, Ticket, TicketStatus
from ticketflow.workflows import (
    AGENT_TASK_QUEUE,
    ESCALATION_REPLY,
    REJECTION_REPLY,
    TICKET_STATUS_ATTR,
    TicketWorkflow,
)


def unique_queue() -> str:
    return f"tq-{uuid.uuid4().hex[:8]}"


class DraftFailingAgent:
    def __init__(self):
        self.classification = billing_classification()
        self.classify_calls = 0
        self.draft_calls = 0

    async def classify(self, ticket):
        self.classify_calls += 1
        return self.classification

    async def draft_reply(self, ticket, classification):
        self.draft_calls += 1
        raise AgentOverloadedError("draft unavailable")


class PermanentlyFailingAgent:
    def __init__(self):
        self.classify_calls = 0

    async def classify(self, ticket):
        self.classify_calls += 1
        raise AgentPermanentError("invalid ticket input")

    async def draft_reply(self, ticket, classification):
        raise AssertionError("draft_reply should not run after classification fails")


class BlockingTicketActivities(TicketActivities):
    def __init__(self, agent):
        super().__init__(agent)
        self.reply_started = asyncio.Event()
        self.release_reply = asyncio.Event()

    @activity.defn
    async def send_reply(self, ticket: Ticket, reply_text: str) -> None:
        self.reply_started.set()
        await self.release_reply.wait()


def make_blocking_reply_worker(client, agent, task_queue, activities):
    workflow_worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[TicketWorkflow],
        activities=[
            activities.send_reply,
            activities.execute_refund,
            activities.record_result,
        ],
    )
    llm_worker = Worker(
        client,
        task_queue=AGENT_TASK_QUEUE,
        activities=[
            activities.classify_ticket,
            activities.draft_reply,
        ],
    )
    return CombinedTestWorker(workflow_worker, llm_worker)


class CombinedTestWorker:
    def __init__(self, *workers):
        self._workers = workers
        self._stack = AsyncExitStack()

    async def __aenter__(self):
        for worker in self._workers:
            await self._stack.enter_async_context(worker)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._stack.__aexit__(exc_type, exc, tb)


def make_workflow_only_worker(client, task_queue, agent=None, db_path=None):
    if agent is None:
        agent = ScriptedAgent(billing_classification(), reply_only_draft())
    activities = TicketActivities(agent, db_path=db_path)
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[TicketWorkflow],
        activities=[
            activities.send_reply,
            activities.execute_refund,
            activities.record_result,
        ],
        workflow_runner=UnsandboxedWorkflowRunner(),
    )


def make_agent_only_worker(client, agent, task_queue):
    activities = TicketActivities(agent)
    return Worker(
        client,
        task_queue=task_queue,
        activities=[
            activities.classify_ticket,
            activities.draft_reply,
        ],
    )


def configure_agent_queues(monkeypatch, primary_queue, fallback_queue, timeout_s=1.0):
    monkeypatch.setattr("ticketflow.workflows.AGENT_TASK_QUEUE", primary_queue)
    monkeypatch.setattr("ticketflow.workflows.FALLBACK_TASK_QUEUE", fallback_queue)
    monkeypatch.setattr("ticketflow.workflows.AGENT_SCHEDULE_TO_START_S", timeout_s)


async def test_high_confidence_reply_resolves_without_approval(env):
    agent = ScriptedAgent(billing_classification(), reply_only_draft(confidence=0.9))
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        result = await env.client.execute_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
    assert result.status == TicketStatus.RESOLVED
    assert result.reply_text == agent.draft.reply_text
    assert result.refund_executed is False


async def test_split_llm_workers_resolve_through_primary_model(env, monkeypatch):
    agent = ScriptedAgent(billing_classification(), reply_only_draft(confidence=0.9))
    ticket = make_ticket()
    workflow_queue = unique_queue()
    primary_queue = unique_queue()
    fallback_queue = unique_queue()
    configure_agent_queues(monkeypatch, primary_queue, fallback_queue)

    async with make_workflow_only_worker(env.client, workflow_queue):
        async with make_agent_only_worker(env.client, agent, primary_queue):
            result = await env.client.execute_workflow(
                TicketWorkflow.run,
                ticket,
                id=f"ticket-{ticket.id}",
                task_queue=workflow_queue,
            )

    assert result.status == TicketStatus.RESOLVED
    assert result.model_path == "primary/primary"


async def test_primary_schedule_to_start_timeout_uses_fallback_queue(env, monkeypatch):
    primary_agent = ScriptedAgent(
        billing_classification(model="primary"),
        reply_only_draft(confidence=0.9, model="primary"),
    )
    fallback_agent = ScriptedAgent(
        billing_classification(confidence=0.5, model="fallback"),
        reply_only_draft(confidence=0.5, model="fallback"),
    )
    ticket = make_ticket()
    workflow_queue = unique_queue()
    primary_queue = unique_queue()
    fallback_queue = unique_queue()
    configure_agent_queues(monkeypatch, primary_queue, fallback_queue, timeout_s=0.1)

    async with make_workflow_only_worker(env.client, workflow_queue):
        async with make_agent_only_worker(env.client, fallback_agent, fallback_queue):
            handle = await env.client.start_workflow(
                TicketWorkflow.run,
                ticket,
                id=f"ticket-{ticket.id}",
                task_queue=workflow_queue,
            )
            await env.sleep(1)
            info = await wait_for_status(handle, TicketStatus.AWAITING_APPROVAL)

    assert primary_agent.classify_calls == 0
    assert fallback_agent.classify_calls == 1
    assert fallback_agent.draft_calls == 1
    assert info.classification is not None
    assert info.classification.model == "fallback"
    assert info.draft is not None
    assert info.draft.model == "fallback"
    assert info.draft.confidence == 0.5


async def test_transient_agent_failures_are_retried(env):
    inner = ScriptedAgent(billing_classification(), reply_only_draft(confidence=0.9))
    agent = FlakyAgent(inner, failures=2)
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        result = await env.client.execute_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
    assert result.status == TicketStatus.RESOLVED
    assert agent.classify_calls == 3


async def test_workflow_escalates_when_classification_retries_are_exhausted(env):
    inner = ScriptedAgent(billing_classification(), reply_only_draft(confidence=0.9))
    agent = FlakyAgent(inner, failures=999)
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        result = await env.client.execute_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
    assert result.status == TicketStatus.ESCALATED
    assert result.reply_text == ESCALATION_REPLY
    assert result.refund_executed is False
    assert agent.classify_calls == 5


async def test_workflow_escalates_without_retrying_permanent_agent_errors(env):
    agent = PermanentlyFailingAgent()
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        result = await env.client.execute_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
    assert result.status == TicketStatus.ESCALATED
    assert result.reply_text == ESCALATION_REPLY
    assert agent.classify_calls == 1


async def test_workflow_escalates_when_draft_retries_are_exhausted(env):
    agent = DraftFailingAgent()
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        result = await env.client.execute_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
    assert result.status == TicketStatus.ESCALATED
    assert result.reply_text == ESCALATION_REPLY
    assert result.refund_executed is False
    assert agent.classify_calls == 1
    assert agent.draft_calls == 5


async def test_approved_refund_executes_and_resolves(env):
    agent = ScriptedAgent(billing_classification(), refund_draft(amount=42.0))
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        handle = await env.client.start_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
        info = await wait_for_status(handle, TicketStatus.AWAITING_APPROVAL)
        assert info.draft is not None
        assert info.draft.action.refund_amount == 42.0

        status = await handle.execute_update(
            TicketWorkflow.submit_approval,
            ApprovalDecision(
                approved=True,
                approver="sam@example.com",
                note="ok, refund them",
            ),
            result_type=TicketStatus,
        )
        result = await handle.result()

    assert status == TicketStatus.RESOLVED
    assert result.status == TicketStatus.RESOLVED
    assert result.refund_executed is True


async def test_rejected_refund_sends_fallback_reply(env):
    agent = ScriptedAgent(billing_classification(), refund_draft(amount=42.0))
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        handle = await env.client.start_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
        await wait_for_status(handle, TicketStatus.AWAITING_APPROVAL)
        status = await handle.execute_update(
            TicketWorkflow.submit_approval,
            ApprovalDecision(
                approved=False,
                approver="sam@example.com",
                note="amount looks wrong",
            ),
            result_type=TicketStatus,
        )
        result = await handle.result()

    assert status == TicketStatus.REJECTED
    assert result.status == TicketStatus.REJECTED
    assert result.reply_text == REJECTION_REPLY
    assert result.refund_executed is False


async def test_low_confidence_reply_requires_approval(env):
    agent = ScriptedAgent(billing_classification(), reply_only_draft(confidence=0.5))
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        handle = await env.client.start_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
        await wait_for_status(handle, TicketStatus.AWAITING_APPROVAL)
        status = await handle.execute_update(
            TicketWorkflow.submit_approval,
            ApprovalDecision(approved=True, approver="sam@example.com"),
            result_type=TicketStatus,
        )
        result = await handle.result()

    assert status == TicketStatus.RESOLVED
    assert result.status == TicketStatus.RESOLVED
    assert result.refund_executed is False


async def test_ticket_status_search_attribute_tracks_approval_inbox():
    agent = ScriptedAgent(billing_classification(), refund_draft(amount=42.0))
    ticket = make_ticket()
    queue = unique_queue()

    local_env = await WorkflowEnvironment.start_local(
        data_converter=pydantic_data_converter,
        search_attributes=[TICKET_STATUS_ATTR],
    )
    try:
        async with make_worker(local_env.client, agent, queue):
            handle = await local_env.client.start_workflow(
                TicketWorkflow.run,
                ticket,
                id=f"ticket-{ticket.id}",
                task_queue=queue,
            )
            await wait_for_status(handle, TicketStatus.AWAITING_APPROVAL)

            awaiting_query = (
                'WorkflowType = "TicketWorkflow" and TicketStatus = "awaiting_approval"'
            )
            for _ in range(100):
                awaiting_ids = [
                    workflow.id
                    async for workflow in local_env.client.list_workflows(
                        awaiting_query
                    )
                ]
                if f"ticket-{ticket.id}" in awaiting_ids:
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("ticket never appeared in approval inbox")

            status = await handle.execute_update(
                "submit_approval",
                ApprovalDecision(approved=True, approver="sam@example.com"),
                result_type=TicketStatus,
            )
            assert status == TicketStatus.RESOLVED

            for _ in range(100):
                awaiting_ids = [
                    workflow.id
                    async for workflow in local_env.client.list_workflows(
                        awaiting_query
                    )
                ]
                if f"ticket-{ticket.id}" not in awaiting_ids:
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("ticket never left approval inbox")
    finally:
        await local_env.shutdown()


async def test_duplicate_approval_update_is_rejected_while_first_is_finishing(env):
    agent = ScriptedAgent(billing_classification(), refund_draft(amount=42.0))
    ticket = make_ticket()
    queue = unique_queue()
    activities = BlockingTicketActivities(agent)
    async with make_blocking_reply_worker(env.client, agent, queue, activities):
        handle = await env.client.start_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
        await wait_for_status(handle, TicketStatus.AWAITING_APPROVAL)

        first = await handle.start_update(
            TicketWorkflow.submit_approval,
            ApprovalDecision(
                approved=True,
                approver="sam@example.com",
                note="first approval",
            ),
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            result_type=TicketStatus,
        )
        # The terminal status is set as soon as _finish starts, even though
        # send_reply is still blocked.
        await wait_for_status(handle, TicketStatus.RESOLVED)

        with pytest.raises(WorkflowUpdateFailedError):
            await handle.execute_update(
                TicketWorkflow.submit_approval,
                ApprovalDecision(
                    approved=False,
                    approver="lee@example.com",
                    note="duplicate approval",
                ),
                result_type=TicketStatus,
            )

        activities.release_reply.set()
        assert await first.result() == TicketStatus.RESOLVED
        result = await handle.result()

    assert result.status == TicketStatus.RESOLVED
    assert result.refund_executed is True


async def test_late_approval_after_timeout_is_rejected_while_escalation_finishes(env):
    agent = ScriptedAgent(billing_classification(), refund_draft(amount=42.0))
    ticket = make_ticket()
    queue = unique_queue()
    activities = BlockingTicketActivities(agent)
    async with make_blocking_reply_worker(env.client, agent, queue, activities):
        handle = await env.client.start_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )
        # Awaiting the result unlocks time skipping, so the 24h approval
        # timer fires and _finish blocks inside the gated send_reply.
        result_task = asyncio.create_task(handle.result())
        await asyncio.wait_for(activities.reply_started.wait(), timeout=30)

        # The wait_for bound keeps a regression from hanging: an accepted
        # update would block until the reply is released.
        with pytest.raises(WorkflowUpdateFailedError):
            await asyncio.wait_for(
                handle.execute_update(
                    TicketWorkflow.submit_approval,
                    ApprovalDecision(
                        approved=True,
                        approver="sam@example.com",
                        note="too late",
                    ),
                    result_type=TicketStatus,
                ),
                timeout=10,
            )

        activities.release_reply.set()
        result = await result_task

    assert result.status == TicketStatus.ESCALATED
    assert result.reply_text == ESCALATION_REPLY
    assert result.refund_executed is False


async def test_unanswered_approval_escalates_after_timeout(env):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue):
        result = await env.client.execute_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )

    assert result.status == TicketStatus.ESCALATED
    assert result.reply_text == ESCALATION_REPLY
    assert result.refund_executed is False


async def test_finish_without_ticket_fails_workflow_non_retryably():
    workflow = TicketWorkflow()

    with pytest.raises(ApplicationError) as exc_info:
        await workflow._finish(
            reply_text="cannot finish",
            refund=False,
            status=TicketStatus.ESCALATED,
        )

    assert str(exc_info.value) == "workflow has no ticket"
    assert exc_info.value.non_retryable is True


async def test_finished_ticket_result_is_persisted_to_read_model(env, tmp_path):
    db = str(tmp_path / "read.db")
    agent = ScriptedAgent(billing_classification(), reply_only_draft(confidence=0.9))
    ticket = make_ticket()
    queue = unique_queue()
    async with make_worker(env.client, agent, queue, db_path=db):
        result = await env.client.execute_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=queue,
        )

    assert result.status == TicketStatus.RESOLVED
    assert readmodel.load_result(ticket.id, db) == result
