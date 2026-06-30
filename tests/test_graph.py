"""Tests for the dispatch-and-interrupt LangGraph ticket workflow (M3.2/M3.3).

``classify`` and ``draft`` dispatch a task and ``interrupt()``; the tests resume
them with the agent result. Refund / low-confidence drafts then pause at the
approval gate, which resumes from a human decision or a timer envelope.
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from tests.helpers import (
    drive_until_quiescent,
    process_one_task,
)
from tests.test_db import FakePool
from ticketflow import config, db, graph, readmodel, runner
from ticketflow.activities import TicketActivities
from ticketflow.agent.base import AgentOverloadedError, AgentPermanentError
from ticketflow.agent.mock import MockAgent
from ticketflow.models import (
    ActionType,
    ApprovalDecision,
    Classification,
    DraftReply,
    ProposedAction,
    Ticket,
    TicketCategory,
    TicketResult,
    TicketStatus,
)
from ticketflow.signals import APPROVAL_DECISION_SIGNAL
from ticketflow.workflows import APPROVAL_TIMEOUT, ESCALATION_REPLY, REJECTION_REPLY


class RecordingActivities(TicketActivities):
    """Ticket activities that keep side effects visible to graph tests."""

    def __init__(self, agent: MockAgent):
        super().__init__(agent)
        self.sent_replies: list[tuple[str, str]] = []
        self.refund_calls: list[tuple[str, float, int]] = []
        self.refund_results: list[bool] = [True]
        self.recorded_results: list[TicketResult] = []

    async def send_reply(
        self, ticket: Ticket, reply_text: str, attempt: int = 1
    ) -> bool:
        _ = attempt
        self.sent_replies.append((ticket.id, reply_text))
        return True

    async def execute_refund(
        self, ticket_id: str, amount: float, attempt: int = 1
    ) -> bool:
        self.refund_calls.append((ticket_id, amount, attempt))
        return self.refund_results.pop(0)

    async def record_result(self, result: TicketResult) -> None:
        self.recorded_results.append(result)


class ScriptedAgent:
    """Agent test double with fixed classify/draft results and call counts."""

    def __init__(self, classification: Classification, draft: DraftReply) -> None:
        self.classification = classification
        self.draft = draft
        self.classify_calls = 0
        self.draft_calls = 0

    async def classify(self, ticket: Ticket) -> Classification:
        _ = ticket
        self.classify_calls += 1
        return self.classification

    async def draft_reply(
        self, ticket: Ticket, classification: Classification
    ) -> DraftReply:
        _ = ticket, classification
        self.draft_calls += 1
        return self.draft


class TransientFailureAgent(ScriptedAgent):
    """Agent that fails a fixed number of classify calls, then succeeds."""

    def __init__(
        self, classification: Classification, draft: DraftReply, *, failures: int
    ) -> None:
        super().__init__(classification, draft)
        self.remaining_failures = failures

    async def classify(self, ticket: Ticket) -> Classification:
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            self.classify_calls += 1
            raise AgentOverloadedError("backend overloaded")
        return await super().classify(ticket)


class AlwaysOverloadedAgent(ScriptedAgent):
    """Agent that always raises a transient overload while classifying."""

    async def classify(self, ticket: Ticket) -> Classification:
        _ = ticket
        self.classify_calls += 1
        raise AgentOverloadedError("backend overloaded")


class PermanentFailureAgent(ScriptedAgent):
    """Agent that raises a permanent classification error without retrying."""

    async def classify(self, ticket: Ticket) -> Classification:
        _ = ticket
        self.classify_calls += 1
        raise AgentPermanentError("invalid ticket input")


class FailReplyOnceActivities(TicketActivities):
    """Activities that fail the first reply after refunding, then succeed."""

    def __init__(self, agent: ScriptedAgent, *, database_url: str) -> None:
        super().__init__(agent, database_url=database_url)
        self.reply_calls = 0

    async def send_reply(
        self, ticket: Ticket, reply_text: str, attempt: int = 1
    ) -> bool:
        self.reply_calls += 1
        if self.reply_calls == 1:
            raise RuntimeError("mail server down")
        return await super().send_reply(ticket, reply_text, attempt=attempt)


def make_ticket(ticket_id: str = "t-1") -> Ticket:
    return Ticket(
        id=ticket_id,
        customer_email="customer@example.com",
        subject="Need help",
        body="My login keeps failing and I want it fixed.",
    )


def config_for(ticket_id: str) -> RunnableConfig:
    return {"configurable": {"thread_id": ticket_id}}


def make_classification(confidence: float = 0.9) -> Classification:
    return Classification(
        category=TicketCategory.GENERAL, confidence=confidence, model="primary"
    )


def reply_draft(confidence: float = 0.9) -> DraftReply:
    return DraftReply(
        reply_text="Try restarting the app.",
        action=ProposedAction(type=ActionType.REPLY_ONLY),
        confidence=confidence,
        model="primary",
    )


def refund_draft(amount: float = 42.0, confidence: float = 0.9) -> DraftReply:
    return DraftReply(
        reply_text="We can refund you.",
        action=ProposedAction(type=ActionType.REFUND, refund_amount=amount),
        confidence=confidence,
        model="primary",
    )


def recording_activities() -> RecordingActivities:
    # The agent is never called by the graph (the worker owns that in M5); the
    # terminal task worker owns side effects; tests assert the graph does not.
    return RecordingActivities(MockAgent(seed=1, failure_rate=0.0))


def idempotency_keys(pool: FakePool) -> list[object]:
    # taskqueue.enqueue params: (queue, task_type, workflow_id, idempotency_key, ...).
    # Skip shorter tuples from reads like taskqueue.is_pending (single param).
    return [params[3] for params in pool.connection_obj.params if len(params) > 3]


def enqueue_calls(pool: FakePool) -> list[tuple[object, object]]:
    """(queue_name, idempotency_key) for each enqueue recorded on the pool."""
    return [
        (params[0], params[3])
        for params in pool.connection_obj.params
        if len(params) > 3
    ]


async def resume_through_agents(
    compiled, ticket: Ticket, *, classification: Classification, draft: DraftReply
) -> dict:
    """Drive past the classify and draft dispatch interrupts.

    Returns the invoke output after the draft result is applied -- either the
    final resolved state or the approval-gate interrupt envelope.
    """
    cfg = config_for(ticket.id)
    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out  # suspended at the classify dispatch
    out = await compiled.ainvoke(
        Command(resume=classification.model_dump(mode="json")), cfg
    )
    assert "__interrupt__" in out  # suspended at the draft dispatch
    return await compiled.ainvoke(Command(resume=draft.model_dump(mode="json")), cfg)


def make_terminal_result(
    ticket: Ticket,
    classification: Classification,
    draft: DraftReply,
    *,
    status: TicketStatus = TicketStatus.RESOLVED,
    refund_executed: bool = False,
) -> TicketResult:
    reply_text = {
        TicketStatus.REJECTED: REJECTION_REPLY,
        TicketStatus.ESCALATED: ESCALATION_REPLY,
    }.get(status, draft.reply_text)
    return TicketResult(
        ticket_id=ticket.id,
        status=status,
        reply_text=reply_text,
        refund_executed=refund_executed,
        model_path=f"{classification.model}/{draft.model}",
    )


def terminal_interrupt_payload(out: dict, ticket: Ticket) -> dict:
    assert "__interrupt__" in out
    interrupts = out["__interrupt__"]
    assert len(interrupts) == 1
    payload = interrupts[0].value
    assert payload["kind"] == "terminal_task"
    assert payload["task_type"] == "finalize_ticket"
    assert payload["workflow_id"] == ticket.id
    assert payload["queue"] == config.TASK_QUEUE
    assert payload["idempotency_key"] == f"{ticket.id}:finalize"
    return payload


async def resume_terminal(
    compiled, ticket: Ticket, result: TicketResult
) -> dict[str, Any]:
    final = await compiled.ainvoke(
        Command(resume=result.model_dump(mode="json")),
        config_for(ticket.id),
    )
    assert "__interrupt__" not in final
    return final


async def drive_to_approval_interrupt(
    compiled, ticket: Ticket, *, draft: DraftReply
) -> tuple[dict, graph.TicketState]:
    """Drive through dispatch to the approval gate and assert the envelope."""
    started_at = datetime.now(timezone.utc)
    out = await resume_through_agents(
        compiled, ticket, classification=make_classification(), draft=draft
    )
    finished_at = datetime.now(timezone.utc)
    snapshot = await compiled.aget_state(config_for(ticket.id))
    state = snapshot.values

    assert "__interrupt__" in out
    interrupts = out["__interrupt__"]
    assert len(interrupts) == 1
    payload = interrupts[0].value
    assert payload["kind"] == "approval_required"
    assert payload["ticket_id"] == ticket.id
    assert isinstance(payload["wakeup_at"], str)
    assert payload["draft"] == state["draft"].model_dump(mode="json")

    assert snapshot.next == ("await_approval",)
    assert state["status"] == TicketStatus.AWAITING_APPROVAL
    assert state["needs_approval"] is True
    assert state["wakeup_at"] is not None
    assert started_at + APPROVAL_TIMEOUT <= state["wakeup_at"]
    assert state["wakeup_at"] <= finished_at + APPROVAL_TIMEOUT
    return out, state


async def test_happy_path_dispatches_then_resolves() -> None:
    pool = FakePool(opened=True, row=(1,))
    activities = recording_activities()
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket()

    classification = make_classification()
    draft = reply_draft()

    out = await resume_through_agents(
        compiled, ticket, classification=classification, draft=draft
    )
    payload = terminal_interrupt_payload(out, ticket)
    assert payload["result"]["status"] == TicketStatus.RESOLVED
    assert f"{ticket.id}:finalize" in idempotency_keys(pool)
    assert activities.sent_replies == []
    assert activities.refund_calls == []
    assert activities.recorded_results == []

    final = await resume_terminal(
        compiled, ticket, make_terminal_result(ticket, classification, draft)
    )

    assert final["status"] == TicketStatus.RESOLVED
    assert final["classification"].category == TicketCategory.GENERAL
    assert final["draft"].reply_text == "Try restarting the app."
    assert final["needs_approval"] is False
    assert final["result"].status == TicketStatus.RESOLVED
    assert final["result"].reply_text == final["draft"].reply_text
    assert final["result"].refund_executed is False

    keys = idempotency_keys(pool)
    assert f"{ticket.id}:classify" in keys
    assert f"{ticket.id}:draft" in keys
    assert activities.sent_replies == []
    assert activities.refund_calls == []
    assert activities.recorded_results == []


async def test_agent_dispatch_updates_visible_status_before_interrupt() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-visible")
    cfg = config_for(ticket.id)

    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out
    snapshot = await compiled.aget_state(cfg)
    assert snapshot.values["status"] == TicketStatus.CLASSIFYING

    out = await compiled.ainvoke(
        Command(resume=make_classification().model_dump(mode="json")), cfg
    )
    assert "__interrupt__" in out
    snapshot = await compiled.aget_state(cfg)
    assert snapshot.values["status"] == TicketStatus.DRAFTING


async def test_agent_dispatch_uses_database_clock_for_schedule_to_start() -> None:
    classify_wakeup = datetime(2026, 6, 23, 12, 0, 30, tzinfo=timezone.utc)
    draft_wakeup = datetime(2026, 6, 23, 12, 5, 30, tzinfo=timezone.utc)
    pool = FakePool(opened=True, row=(1,))
    pool.connection_obj.rows = [(1,), (classify_wakeup,), (1,), (draft_wakeup,)]
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-clock-dispatch")
    cfg = config_for(ticket.id)

    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )

    assert out["wakeup_at"] == classify_wakeup
    assert out["__interrupt__"][0].value["wakeup_at"] == classify_wakeup.isoformat()
    assert (timedelta(seconds=config.AGENT_SCHEDULE_TO_START_S),) in (
        pool.connection_obj.params
    )

    out = await compiled.ainvoke(
        Command(resume=make_classification().model_dump(mode="json")), cfg
    )

    assert out["wakeup_at"] == draft_wakeup
    assert out["__interrupt__"][0].value["wakeup_at"] == draft_wakeup.isoformat()


async def test_classify_task_failure_escalates_without_draft_dispatch() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-classify-failed")
    cfg = config_for(ticket.id)

    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out

    out = await compiled.ainvoke(
        Command(
            resume={
                "kind": "task_failed",
                "error": "invalid ticket input",
                "permanent": True,
            }
        ),
        cfg,
    )

    payload = terminal_interrupt_payload(out, ticket)
    assert payload["result"]["status"] == TicketStatus.ESCALATED
    assert payload["result"]["reply_text"] == ESCALATION_REPLY
    keys = idempotency_keys(pool)
    assert f"{ticket.id}:draft" not in keys
    assert f"{ticket.id}:finalize" in keys
    snapshot = await compiled.aget_state(cfg)
    assert snapshot.values["status"] == TicketStatus.ESCALATED
    assert snapshot.values["needs_approval"] is False


async def test_draft_task_failure_escalates_without_approval() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-draft-failed")
    cfg = config_for(ticket.id)

    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out
    out = await compiled.ainvoke(
        Command(resume=make_classification().model_dump(mode="json")), cfg
    )
    assert "__interrupt__" in out

    out = await compiled.ainvoke(
        Command(
            resume={
                "kind": "task_failed",
                "error": "drafting failed permanently",
                "permanent": True,
            }
        ),
        cfg,
    )

    payload = terminal_interrupt_payload(out, ticket)
    assert payload["result"]["status"] == TicketStatus.ESCALATED
    assert payload["result"]["reply_text"] == ESCALATION_REPLY
    snapshot = await compiled.aget_state(cfg)
    assert snapshot.values["status"] == TicketStatus.ESCALATED
    assert snapshot.values["needs_approval"] is False
    assert snapshot.next == ("execute",)


async def test_schedule_to_start_timeout_redispatches_to_fallback() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-fallback")
    cfg = config_for(ticket.id)

    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out  # suspended at the classify dispatch
    payload = out["__interrupt__"][0].value
    assert payload["queue"] == config.AGENT_TASK_QUEUE
    assert isinstance(payload["wakeup_at"], str)

    # The schedule-to-start timer fires while the task is still pending.
    out = await compiled.ainvoke(Command(resume={"kind": "timeout"}), cfg)
    assert "__interrupt__" in out  # re-suspended after the fallback dispatch
    assert out["__interrupt__"][0].value["queue"] == config.FALLBACK_TASK_QUEUE

    # The same work was re-dispatched to the fallback queue under its own key.
    assert (
        config.FALLBACK_TASK_QUEUE,
        f"{ticket.id}:classify:fallback",
    ) in enqueue_calls(pool)
    assert ("redispatched to fallback", f"{ticket.id}:classify") in (
        pool.connection_obj.params
    )

    # Resuming with the agent result advances to the draft dispatch as usual.
    out = await compiled.ainvoke(
        Command(resume=make_classification().model_dump(mode="json")), cfg
    )
    assert "__interrupt__" in out
    assert out["__interrupt__"][0].value["task_type"] == "draft"


async def test_schedule_to_start_timeout_skips_fallback_when_not_pending() -> None:
    # row=None makes taskqueue.is_pending() report the task is no longer pending
    # (a worker already leased it), so no fallback dispatch happens.
    pool = FakePool(opened=True, row=None)
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-leased")
    cfg = config_for(ticket.id)

    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out

    out = await compiled.ainvoke(Command(resume={"kind": "timeout"}), cfg)
    assert "__interrupt__" in out  # re-suspended, still waiting on the primary task
    assert out["__interrupt__"][0].value["queue"] == config.AGENT_TASK_QUEUE

    # No work was routed to the fallback queue.
    assert all(queue != config.FALLBACK_TASK_QUEUE for queue, _ in enqueue_calls(pool))

    # The result still resumes the graph normally.
    out = await compiled.ainvoke(
        Command(resume=make_classification().model_dump(mode="json")), cfg
    )
    assert "__interrupt__" in out
    assert out["__interrupt__"][0].value["task_type"] == "draft"


async def test_timeout_skips_fallback_when_cancel_loses_race() -> None:
    # The first row lets the initial dispatch enqueue appear successful; the
    # second row makes the atomic cancel report that no pending row was claimed.
    pool = FakePool(opened=True, row=(1,))
    pool.connection_obj.rows = [
        (1,),
        (datetime.now(timezone.utc) + timedelta(seconds=30),),
        None,
    ]
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-cancel-race")
    cfg = config_for(ticket.id)

    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out

    out = await compiled.ainvoke(Command(resume={"kind": "timeout"}), cfg)
    assert "__interrupt__" in out
    assert out["__interrupt__"][0].value["queue"] == config.AGENT_TASK_QUEUE
    assert all(queue != config.FALLBACK_TASK_QUEUE for queue, _ in enqueue_calls(pool))


async def test_decide_approval_flags_refund_drafts() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-refund")

    _, state = await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())

    draft = state.get("draft")
    assert draft is not None
    assert draft.action.type == ActionType.REFUND


async def test_decide_approval_flags_low_confidence_drafts() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-lowconf")

    _, state = await drive_to_approval_interrupt(
        compiled, ticket, draft=reply_draft(confidence=0.4)
    )

    draft = state.get("draft")
    assert draft is not None
    assert draft.confidence < 0.75


async def test_prepare_approval_uses_database_clock_for_timeout() -> None:
    approval_wakeup = datetime(2026, 6, 24, 12, 0, tzinfo=timezone.utc)
    pool = FakePool(opened=True, row=(1,))
    pool.connection_obj.rows = [
        (1,),
        (datetime(2026, 6, 23, 12, 0, 30, tzinfo=timezone.utc),),
        (1,),
        (datetime(2026, 6, 23, 12, 1, 0, tzinfo=timezone.utc),),
        (approval_wakeup,),
    ]
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-clock-approval")

    out = await resume_through_agents(
        compiled,
        ticket,
        classification=make_classification(),
        draft=refund_draft(),
    )
    snapshot = await compiled.aget_state(config_for(ticket.id))

    assert snapshot.values["wakeup_at"] == approval_wakeup
    assert out["__interrupt__"][0].value["wakeup_at"] == approval_wakeup.isoformat()
    assert (APPROVAL_TIMEOUT,) in pool.connection_obj.params


async def test_approved_resume_continues_to_resolution() -> None:
    pool = FakePool(opened=True, row=(1,))
    activities = recording_activities()
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-approved")
    _, state = await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())
    decision = ApprovalDecision(approved=True, approver="sam@example.com")

    out = await compiled.ainvoke(
        Command(
            resume={"kind": "decision", "decision": decision.model_dump(mode="json")}
        ),
        config_for(ticket.id),
    )

    terminal_interrupt_payload(out, ticket)
    draft = state.get("draft")
    classification = state.get("classification")
    assert draft is not None and classification is not None
    final = await resume_terminal(
        compiled,
        ticket,
        make_terminal_result(ticket, classification, draft, refund_executed=True),
    )

    assert final["status"] == TicketStatus.RESOLVED
    assert final["decision"] == decision
    assert final["result"].status == TicketStatus.RESOLVED
    assert final["result"].refund_executed is True
    assert activities.refund_calls == []
    assert activities.sent_replies == []
    assert activities.recorded_results == []


async def test_approved_duplicate_refund_records_no_new_refund() -> None:
    pool = FakePool(opened=True, row=(1,))
    activities = recording_activities()
    activities.refund_results = [False]
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-approved-duplicate")
    await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())
    decision = ApprovalDecision(approved=True, approver="sam@example.com")

    out = await compiled.ainvoke(
        Command(
            resume={"kind": "decision", "decision": decision.model_dump(mode="json")}
        ),
        config_for(ticket.id),
    )

    terminal_interrupt_payload(out, ticket)
    snapshot = await compiled.aget_state(config_for(ticket.id))
    draft = snapshot.values.get("draft")
    classification = snapshot.values.get("classification")
    assert isinstance(draft, DraftReply)
    assert isinstance(classification, Classification)
    final = await resume_terminal(
        compiled,
        ticket,
        make_terminal_result(ticket, classification, draft, refund_executed=False),
    )

    assert final["status"] == TicketStatus.RESOLVED
    assert final["result"].refund_executed is False
    assert activities.refund_calls == []
    assert activities.recorded_results == []


async def test_rejected_resume_sends_and_records_rejection_reply() -> None:
    pool = FakePool(opened=True, row=(1,))
    activities = recording_activities()
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-rejected")
    await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())
    decision = ApprovalDecision(
        approved=False, approver="sam@example.com", note="Manual review rejected it."
    )

    out = await compiled.ainvoke(
        Command(
            resume={"kind": "decision", "decision": decision.model_dump(mode="json")}
        ),
        config_for(ticket.id),
    )

    terminal_interrupt_payload(out, ticket)
    snapshot = await compiled.aget_state(config_for(ticket.id))
    draft = snapshot.values.get("draft")
    classification = snapshot.values.get("classification")
    assert isinstance(draft, DraftReply)
    assert isinstance(classification, Classification)
    final = await resume_terminal(
        compiled,
        ticket,
        make_terminal_result(
            ticket, classification, draft, status=TicketStatus.REJECTED
        ),
    )

    assert final["status"] == TicketStatus.REJECTED
    assert final["decision"] == decision
    assert final["result"].status == TicketStatus.REJECTED
    assert final["result"].reply_text == REJECTION_REPLY
    assert final["result"].refund_executed is False
    assert final["wakeup_at"] is None
    assert activities.sent_replies == []
    assert activities.refund_calls == []
    assert activities.recorded_results == []


async def test_timeout_resume_sends_and_records_escalation_reply() -> None:
    pool = FakePool(opened=True, row=(1,))
    activities = recording_activities()
    compiled = graph.compile_ticket_graph(InMemorySaver(), pool)
    ticket = make_ticket("t-timeout")
    await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())

    out = await compiled.ainvoke(
        Command(resume={"kind": "timeout"}),
        config_for(ticket.id),
    )

    terminal_interrupt_payload(out, ticket)
    snapshot = await compiled.aget_state(config_for(ticket.id))
    draft = snapshot.values.get("draft")
    classification = snapshot.values.get("classification")
    assert isinstance(draft, DraftReply)
    assert isinstance(classification, Classification)
    final = await resume_terminal(
        compiled,
        ticket,
        make_terminal_result(
            ticket, classification, draft, status=TicketStatus.ESCALATED
        ),
    )

    assert final["status"] == TicketStatus.ESCALATED
    assert final.get("decision") is None
    assert final["result"].status == TicketStatus.ESCALATED
    assert final["result"].reply_text == ESCALATION_REPLY
    assert final["result"].refund_executed is False
    assert final["wakeup_at"] is None
    assert activities.sent_replies == []
    assert activities.refund_calls == []
    assert activities.recorded_results == []


@pytest.mark.integration
async def test_dispatch_loop_resolves_through_real_postgres(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    activities = TicketActivities(
        MockAgent(
            seed=1, failure_rate=0.0, refund_rate=0.0, confidence_range=(0.8, 1.0)
        )
    )
    ticket = make_ticket("t-dispatch")
    cfg = config_for(ticket.id)

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(saver, postgres_pool)

        out = await compiled.ainvoke(
            {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
        )
        # The graph suspended after enqueueing the classify task.
        assert "__interrupt__" in out
        assert _pending_keys(postgres_pool, ticket.id) == [f"{ticket.id}:classify"]

        # Drive the dispatch -> worker -> resume loop to completion.
        while "__interrupt__" in out:
            interrupt = out["__interrupt__"][0].value
            key = interrupt["idempotency_key"]
            assert await process_one_task(
                postgres_pool, activities, queue_name=interrupt["queue"]
            )
            result = _task_result(postgres_pool, key)
            out = await compiled.ainvoke(Command(resume=result), cfg)

        assert out["status"] == TicketStatus.RESOLVED

    # A fresh saver (new connection) proves the run is durable in Postgres.
    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as fresh:
        reopened = graph.compile_ticket_graph(fresh, postgres_pool)
        snapshot = await reopened.aget_state(cfg)

    state = snapshot.values
    assert state["status"] == TicketStatus.RESOLVED
    assert isinstance(state["ticket"], Ticket)
    assert isinstance(state["draft"], DraftReply)
    assert state["result"].reply_text == state["draft"].reply_text


@pytest.mark.integration
async def test_schedule_to_start_timeout_routes_to_fallback_through_postgres(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    activities = TicketActivities(
        MockAgent(
            seed=1, failure_rate=0.0, refund_rate=0.0, confidence_range=(0.8, 1.0)
        )
    )
    ticket = make_ticket("t-fb")
    cfg = config_for(ticket.id)

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(saver, postgres_pool)

        out = await compiled.ainvoke(
            {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
        )
        # Suspended at the classify dispatch with only the primary task pending.
        assert "__interrupt__" in out
        assert _pending_keys(postgres_pool, ticket.id) == [f"{ticket.id}:classify"]

        # No primary worker picked it up; the schedule-to-start timer fires.
        out = await compiled.ainvoke(Command(resume={"kind": "timeout"}), cfg)
        assert "__interrupt__" in out
        assert out["__interrupt__"][0].value["queue"] == config.FALLBACK_TASK_QUEUE
        assert _pending_keys(postgres_pool, ticket.id) == [
            f"{ticket.id}:classify:fallback"
        ]

        # The unthrottled fallback worker drains it; resume with its result.
        assert await process_one_task(
            postgres_pool, activities, queue_name=config.FALLBACK_TASK_QUEUE
        )
        out = await compiled.ainvoke(
            Command(
                resume=_task_result(postgres_pool, f"{ticket.id}:classify:fallback")
            ),
            cfg,
        )

        # Drive the remaining dispatch(es) to completion through the primary
        # queue. An orphaned primary copy of the classify task may also be
        # pending, so drain until the awaited task has produced its result.
        while "__interrupt__" in out:
            interrupt = out["__interrupt__"][0].value
            key = interrupt["idempotency_key"]
            while _result_for(postgres_pool, key) is None:
                assert await process_one_task(
                    postgres_pool, activities, queue_name=interrupt["queue"]
                )
            out = await compiled.ainvoke(
                Command(resume=_result_for(postgres_pool, key)), cfg
            )

        assert out["status"] == TicketStatus.RESOLVED


@pytest.mark.integration
async def test_retargeted_workflow_happy_path_resolves_through_runner(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    ticket = make_ticket(f"t-74-happy-{uuid.uuid4().hex}")
    agent = ScriptedAgent(make_classification(), reply_draft())
    activities = TicketActivities(agent, database_url=postgres_database_url)
    cfg = config_for(ticket.id)

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(saver, postgres_pool)
        await _start_durable_run(compiled, postgres_pool, ticket, cfg)

        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        snapshot = await compiled.aget_state(cfg)

    assert snapshot.values["status"] == TicketStatus.RESOLVED
    assert snapshot.values["result"].status == TicketStatus.RESOLVED
    assert agent.classify_calls == 1
    assert agent.draft_calls == 1
    assert _require_task_row(postgres_pool, f"{ticket.id}:classify")["status"] == "done"
    assert _require_task_row(postgres_pool, f"{ticket.id}:draft")["status"] == "done"
    assert _require_task_row(postgres_pool, f"{ticket.id}:finalize")["status"] == "done"
    assert (
        readmodel.load_result(ticket.id, pool=postgres_pool)
        == snapshot.values["result"]
    )
    assert _refund_counts(postgres_pool, ticket.id) == (0, 0)
    _assert_terminal_run_clean(postgres_pool, ticket.id, TicketStatus.RESOLVED)


@pytest.mark.integration
async def test_retargeted_workflow_fallback_on_timeout_completes(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    ticket = make_ticket(f"t-74-fallback-{uuid.uuid4().hex}")
    agent = ScriptedAgent(make_classification(), reply_draft())
    activities = TicketActivities(agent, database_url=postgres_database_url)
    cfg = config_for(ticket.id)

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(saver, postgres_pool)
        await _start_durable_run(compiled, postgres_pool, ticket, cfg)

        with postgres_pool.connection() as conn:
            conn.execute(
                "UPDATE workflow_run SET wakeup_at = now(), updated_at = now() "
                "WHERE ticket_id = %s",
                (ticket.id,),
            )
            conn.commit()
        advanced = await runner.step(compiled, postgres_pool, "runner-1")
        fallback_row = _require_task_row(
            postgres_pool, f"{ticket.id}:classify:fallback"
        )

        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        snapshot = await compiled.aget_state(cfg)

    primary_row = _require_task_row(postgres_pool, f"{ticket.id}:classify")
    assert advanced is True
    assert primary_row["status"] == "failed"
    assert primary_row["permanent"] is True
    assert primary_row["error"] == "redispatched to fallback"
    assert fallback_row["queue_name"] == config.FALLBACK_TASK_QUEUE
    assert fallback_row["status"] == "pending"
    assert (
        _require_task_row(postgres_pool, f"{ticket.id}:classify:fallback")["status"]
        == "done"
    )
    assert snapshot.values["status"] == TicketStatus.RESOLVED
    _assert_terminal_run_clean(postgres_pool, ticket.id, TicketStatus.RESOLVED)


@pytest.mark.integration
async def test_retargeted_workflow_transient_retry_succeeds(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    ticket = make_ticket(f"t-74-transient-{uuid.uuid4().hex}")
    agent = TransientFailureAgent(make_classification(), reply_draft(), failures=1)
    activities = TicketActivities(agent, database_url=postgres_database_url)
    cfg = config_for(ticket.id)
    key = f"{ticket.id}:classify"

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(saver, postgres_pool)
        await _start_durable_run(compiled, postgres_pool, ticket, cfg)

        assert await process_one_task(postgres_pool, activities)
        first_row = _require_task_row(postgres_pool, key)
        assert first_row["status"] == "pending"
        assert first_row["attempts"] == 1

        _force_task_due(postgres_pool, key)
        assert await process_one_task(postgres_pool, activities)
        second_row = _require_task_row(postgres_pool, key)
        assert second_row["status"] == "done"
        assert second_row["attempts"] == 2

        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        snapshot = await compiled.aget_state(cfg)

    assert agent.classify_calls == 2
    assert snapshot.values["status"] == TicketStatus.RESOLVED
    _assert_terminal_run_clean(postgres_pool, ticket.id, TicketStatus.RESOLVED)


@pytest.mark.integration
async def test_retargeted_workflow_exhausted_retries_escalate(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    ticket = make_ticket(f"t-74-exhausted-{uuid.uuid4().hex}")
    agent = AlwaysOverloadedAgent(make_classification(), reply_draft())
    activities = TicketActivities(agent, database_url=postgres_database_url)
    cfg = config_for(ticket.id)
    key = f"{ticket.id}:classify"

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(saver, postgres_pool)
        await _start_durable_run(compiled, postgres_pool, ticket, cfg)

        for expected_attempt in (1, 2, 3):
            assert await process_one_task(postgres_pool, activities)
            row = _require_task_row(postgres_pool, key)
            assert row["attempts"] == expected_attempt
            if expected_attempt < 3:
                assert row["status"] == "pending"
                _force_task_due(postgres_pool, key)
            else:
                assert row["status"] == "failed"

        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        snapshot = await compiled.aget_state(cfg)

    assert agent.classify_calls == 3
    assert _require_task_row(postgres_pool, key)["error"] == "backend overloaded"
    assert _task_row(postgres_pool, f"{ticket.id}:draft") is None
    assert snapshot.values["status"] == TicketStatus.ESCALATED
    assert snapshot.values["result"].status == TicketStatus.ESCALATED
    assert _refund_counts(postgres_pool, ticket.id) == (0, 0)
    _assert_terminal_run_clean(postgres_pool, ticket.id, TicketStatus.ESCALATED)


@pytest.mark.integration
async def test_retargeted_workflow_permanent_error_does_not_retry(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    ticket = make_ticket(f"t-74-permanent-{uuid.uuid4().hex}")
    agent = PermanentFailureAgent(make_classification(), reply_draft())
    activities = TicketActivities(agent, database_url=postgres_database_url)
    cfg = config_for(ticket.id)
    key = f"{ticket.id}:classify"

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(saver, postgres_pool)
        await _start_durable_run(compiled, postgres_pool, ticket, cfg)

        assert await process_one_task(postgres_pool, activities)
        row = _require_task_row(postgres_pool, key)

        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        snapshot = await compiled.aget_state(cfg)

    assert agent.classify_calls == 1
    assert row["status"] == "failed"
    assert row["attempts"] == 1
    assert row["permanent"] is True
    assert row["error"] == "invalid ticket input"
    assert _task_row(postgres_pool, f"{ticket.id}:draft") is None
    assert snapshot.values["status"] == TicketStatus.ESCALATED
    _assert_terminal_run_clean(postgres_pool, ticket.id, TicketStatus.ESCALATED)


@pytest.mark.integration
async def test_retargeted_workflow_refund_idempotency_on_finalizer_retry(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
) -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    ticket = make_ticket(f"t-74-refund-idem-{uuid.uuid4().hex}")
    agent = ScriptedAgent(make_classification(), refund_draft(amount=42.0))
    activities = FailReplyOnceActivities(agent, database_url=postgres_database_url)
    cfg = config_for(ticket.id)
    finalize_key = f"{ticket.id}:finalize"
    decision = ApprovalDecision(approved=True, approver="sam@example.com")

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(saver, postgres_pool)
        await _start_durable_run(compiled, postgres_pool, ticket, cfg)

        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        awaiting = await compiled.aget_state(cfg)
        assert awaiting.values["status"] == TicketStatus.AWAITING_APPROVAL

        signal_id = db.add_pending_signal_if_waiting(
            ticket.id,
            APPROVAL_DECISION_SIGNAL,
            decision.model_dump(mode="json"),
            waiting_status=TicketStatus.AWAITING_APPROVAL,
            pool=postgres_pool,
        )
        assert signal_id is not None
        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        first_finalize = _require_task_row(postgres_pool, finalize_key)
        assert first_finalize["status"] == "pending"
        assert first_finalize["attempts"] == 1
        assert _refund_counts(postgres_pool, ticket.id) == (1, 1)

        _force_task_due(postgres_pool, finalize_key)
        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        snapshot = await compiled.aget_state(cfg)

    finalizer = _require_task_row(postgres_pool, finalize_key)
    assert activities.reply_calls == 2
    assert finalizer["status"] == "done"
    assert finalizer["attempts"] == 2
    # The refund moved money on attempt 1; the retry is a ledger no-op but the flag
    # is sourced from durable refund state, so it still honestly reports True (9.3).
    assert finalizer["result"]["refund_executed"] is True
    assert _refund_counts(postgres_pool, ticket.id) == (1, 2)
    assert snapshot.values["status"] == TicketStatus.RESOLVED
    assert snapshot.values["result"].refund_executed is True
    _assert_terminal_run_clean(postgres_pool, ticket.id, TicketStatus.RESOLVED)


async def _start_durable_run(
    compiled: Any,
    pool: db.ConnectionPool,
    ticket: Ticket,
    cfg: RunnableConfig,
) -> None:
    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out
    snapshot = await compiled.aget_state(cfg)
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
            "VALUES (%s, %s, %s)",
            (
                ticket.id,
                snapshot.values["status"],
                snapshot.values.get("wakeup_at"),
            ),
        )
        conn.commit()


def _task_row(pool: object, idempotency_key: str) -> dict[str, Any] | None:
    with pool.connection() as conn:  # type: ignore[attr-defined]
        row = conn.execute(
            """
            SELECT queue_name, task_type, status, attempts, max_attempts,
                   error, permanent, result
            FROM task_queue
            WHERE idempotency_key = %s
            """,
            (idempotency_key,),
        ).fetchone()
    if row is None:
        return None
    return {
        "queue_name": row[0],
        "task_type": row[1],
        "status": row[2],
        "attempts": row[3],
        "max_attempts": row[4],
        "error": row[5],
        "permanent": row[6],
        "result": row[7],
    }


def _require_task_row(pool: object, idempotency_key: str) -> dict[str, Any]:
    row = _task_row(pool, idempotency_key)
    assert row is not None, f"missing task row {idempotency_key}"
    return row


def _force_task_due(pool: object, idempotency_key: str) -> None:
    with pool.connection() as conn:  # type: ignore[attr-defined]
        conn.execute(
            "UPDATE task_queue SET available_at = now() WHERE idempotency_key = %s",
            (idempotency_key,),
        )
        conn.commit()


def _refund_counts(pool: object, ticket_id: str) -> tuple[int, int]:
    with pool.connection() as conn:  # type: ignore[attr-defined]
        refunds = conn.execute(
            "SELECT count(*) FROM refunds WHERE ticket_id = %s", (ticket_id,)
        ).fetchone()
        attempts = conn.execute(
            "SELECT count(*) FROM refund_attempts WHERE ticket_id = %s",
            (ticket_id,),
        ).fetchone()
    assert refunds is not None and attempts is not None
    return int(refunds[0]), int(attempts[0])


def _assert_terminal_run_clean(
    pool: object, ticket_id: str, status: TicketStatus
) -> None:
    with pool.connection() as conn:  # type: ignore[attr-defined]
        row = conn.execute(
            "SELECT status, wakeup_at, lease_owner, lease_expires_at "
            "FROM workflow_run WHERE ticket_id = %s",
            (ticket_id,),
        ).fetchone()
    assert row == (status.value, None, None, None)


def _pending_keys(pool: object, workflow_id: str) -> list[str]:
    with pool.connection() as conn:  # type: ignore[attr-defined]
        rows = conn.execute(
            "SELECT idempotency_key FROM task_queue "
            "WHERE workflow_id = %s AND status = 'pending' ORDER BY idempotency_key",
            (workflow_id,),
        ).fetchall()
    return [row[0] for row in rows]


def _task_result(pool: object, idempotency_key: str) -> dict:
    with pool.connection() as conn:  # type: ignore[attr-defined]
        row = conn.execute(
            "SELECT result FROM task_queue WHERE idempotency_key = %s",
            (idempotency_key,),
        ).fetchone()
    assert row is not None and row[0] is not None
    return row[0]


def _result_for(pool: object, idempotency_key: str) -> dict | None:
    """Return a task's stored result, or ``None`` if it has not produced one."""
    with pool.connection() as conn:  # type: ignore[attr-defined]
        row = conn.execute(
            "SELECT result FROM task_queue "
            "WHERE idempotency_key = %s AND result IS NOT NULL",
            (idempotency_key,),
        ).fetchone()
    return row[0] if row is not None else None
