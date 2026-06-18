"""Tests for the dispatch-and-interrupt LangGraph ticket workflow (M3.2/M3.3).

``classify`` and ``draft`` dispatch a task and ``interrupt()``; the tests resume
them with the agent result. Refund / low-confidence drafts then pause at the
approval gate, which resumes from a human decision or a timer envelope.
"""

from datetime import datetime, timezone

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from tests.helpers import process_one_agent_task
from tests.test_db import FakePool
from ticketflow import config, graph
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.models import (
    ActionType,
    ApprovalDecision,
    Classification,
    DraftReply,
    ProposedAction,
    Ticket,
    TicketCategory,
    TicketStatus,
)
from ticketflow.workflows import APPROVAL_TIMEOUT


class RecordingActivities(TicketActivities):
    """Ticket activities that keep side effects visible to graph tests."""

    def __init__(self, agent: MockAgent):
        super().__init__(agent)
        self.sent_replies: list[tuple[str, str]] = []

    async def send_reply(self, ticket: Ticket, reply_text: str) -> None:
        self.sent_replies.append((ticket.id, reply_text))


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
    # inline ``execute`` node still needs ``send_reply``, captured here.
    return RecordingActivities(MockAgent(seed=1, failure_rate=0.0))


def idempotency_keys(pool: FakePool) -> list[object]:
    # taskqueue.enqueue params: (queue, task_type, workflow_id, idempotency_key, ...)
    return [params[3] for params in pool.connection_obj.params]


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
    compiled = graph.compile_ticket_graph(activities, InMemorySaver(), pool)
    ticket = make_ticket()

    final = await resume_through_agents(
        compiled, ticket, classification=make_classification(), draft=reply_draft()
    )

    assert "__interrupt__" not in final
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
    assert activities.sent_replies == [(ticket.id, final["draft"].reply_text)]


async def test_decide_approval_flags_refund_drafts() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(recording_activities(), InMemorySaver(), pool)
    ticket = make_ticket("t-refund")

    _, state = await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())

    draft = state.get("draft")
    assert draft is not None
    assert draft.action.type == ActionType.REFUND


async def test_decide_approval_flags_low_confidence_drafts() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(recording_activities(), InMemorySaver(), pool)
    ticket = make_ticket("t-lowconf")

    _, state = await drive_to_approval_interrupt(
        compiled, ticket, draft=reply_draft(confidence=0.4)
    )

    draft = state.get("draft")
    assert draft is not None
    assert draft.confidence < 0.75


async def test_approved_resume_continues_to_resolution() -> None:
    pool = FakePool(opened=True, row=(1,))
    activities = recording_activities()
    compiled = graph.compile_ticket_graph(activities, InMemorySaver(), pool)
    ticket = make_ticket("t-approved")
    _, state = await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())
    decision = ApprovalDecision(approved=True, approver="sam@example.com")

    final = await compiled.ainvoke(
        Command(
            resume={"kind": "decision", "decision": decision.model_dump(mode="json")}
        ),
        config_for(ticket.id),
    )

    assert final["status"] == TicketStatus.RESOLVED
    assert final["decision"] == decision
    assert final["result"].status == TicketStatus.RESOLVED
    draft = state.get("draft")
    assert draft is not None
    assert activities.sent_replies == [(ticket.id, draft.reply_text)]


async def test_rejected_resume_stops_without_sending_draft() -> None:
    pool = FakePool(opened=True, row=(1,))
    activities = recording_activities()
    compiled = graph.compile_ticket_graph(activities, InMemorySaver(), pool)
    ticket = make_ticket("t-rejected")
    await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())
    decision = ApprovalDecision(
        approved=False, approver="sam@example.com", note="Manual review rejected it."
    )

    final = await compiled.ainvoke(
        Command(
            resume={"kind": "decision", "decision": decision.model_dump(mode="json")}
        ),
        config_for(ticket.id),
    )

    assert final["status"] == TicketStatus.REJECTED
    assert final["decision"] == decision
    assert final.get("result") is None
    assert activities.sent_replies == []


async def test_timeout_resume_escalates_without_sending_draft() -> None:
    pool = FakePool(opened=True, row=(1,))
    activities = recording_activities()
    compiled = graph.compile_ticket_graph(activities, InMemorySaver(), pool)
    ticket = make_ticket("t-timeout")
    await drive_to_approval_interrupt(compiled, ticket, draft=refund_draft())

    final = await compiled.ainvoke(
        Command(resume={"kind": "timeout"}),
        config_for(ticket.id),
    )

    assert final["status"] == TicketStatus.ESCALATED
    assert final.get("decision") is None
    assert final.get("result") is None
    assert activities.sent_replies == []


@pytest.mark.integration
async def test_dispatch_loop_resolves_through_real_postgres() -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from ticketflow import db

    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    activities = TicketActivities(
        MockAgent(
            seed=1, failure_rate=0.0, refund_rate=0.0, confidence_range=(0.8, 1.0)
        )
    )
    ticket = make_ticket("t-dispatch")
    cfg = config_for(ticket.id)

    try:
        with pool.connection() as conn:
            conn.execute("DELETE FROM task_queue WHERE workflow_id = %s", (ticket.id,))
            conn.commit()

        async with AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) as saver:
            await saver.setup()
            compiled = graph.compile_ticket_graph(activities, saver, pool)

            out = await compiled.ainvoke(
                {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
            )
            # The graph suspended after enqueueing the classify task.
            assert "__interrupt__" in out
            assert _pending_keys(pool, ticket.id) == [f"{ticket.id}:classify"]

            # Drive the dispatch -> worker -> resume loop to completion.
            while "__interrupt__" in out:
                interrupt = out["__interrupt__"][0].value
                key = f"{interrupt['workflow_id']}:{interrupt['task_type']}"
                assert await process_one_agent_task(pool, activities)
                result = _task_result(pool, key)
                out = await compiled.ainvoke(Command(resume=result), cfg)

            assert out["status"] == TicketStatus.RESOLVED

        # A fresh saver (new connection) proves the run is durable in Postgres.
        async with AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) as fresh:
            reopened = graph.compile_ticket_graph(activities, fresh, pool)
            snapshot = await reopened.aget_state(cfg)
    finally:
        pool.close()

    state = snapshot.values
    assert state["status"] == TicketStatus.RESOLVED
    assert isinstance(state["ticket"], Ticket)
    assert isinstance(state["draft"], DraftReply)
    assert state["result"].reply_text == state["draft"].reply_text


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
