"""Tests for the inline LangGraph ticket workflow."""

from datetime import datetime, timezone

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from ticketflow import config, graph
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.models import (
    ActionType,
    ApprovalDecision,
    DraftReply,
    Ticket,
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


async def start_until_approval_interrupt(
    compiled, ticket: Ticket
) -> tuple[dict, graph.TicketState]:
    started_at = datetime.now(timezone.utc)
    interrupted = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED},
        config_for(ticket.id),
    )
    finished_at = datetime.now(timezone.utc)
    snapshot = await compiled.aget_state(config_for(ticket.id))
    state = snapshot.values

    assert "__interrupt__" in interrupted
    interrupts = interrupted["__interrupt__"]
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
    return interrupted, state


async def test_happy_path_reaches_resolved() -> None:
    activities = TicketActivities(
        MockAgent(
            seed=1,
            failure_rate=0.0,
            refund_rate=0.0,
            confidence_range=(0.8, 1.0),
        )
    )
    compiled = graph.compile_ticket_graph(activities, InMemorySaver())
    ticket = make_ticket()

    final = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED},
        config_for(ticket.id),
    )

    assert final["status"] == TicketStatus.RESOLVED
    assert final["classification"] is not None
    assert final["draft"] is not None
    assert final["needs_approval"] is False
    assert final["result"].status == TicketStatus.RESOLVED
    assert final["result"].reply_text == final["draft"].reply_text
    assert final["result"].refund_executed is False


async def test_decide_approval_flags_refund_drafts() -> None:
    activities = TicketActivities(
        MockAgent(
            seed=1,
            failure_rate=0.0,
            refund_rate=1.0,
            confidence_range=(0.8, 1.0),
        )
    )
    compiled = graph.compile_ticket_graph(activities, InMemorySaver())
    ticket = make_ticket("t-refund")

    _, state = await start_until_approval_interrupt(compiled, ticket)

    draft = state.get("draft")
    assert draft is not None
    assert draft.action.type == ActionType.REFUND


async def test_decide_approval_flags_low_confidence_drafts() -> None:
    activities = TicketActivities(
        MockAgent(
            seed=1,
            failure_rate=0.0,
            refund_rate=0.0,
            confidence_range=(0.0, 0.5),
        )
    )
    compiled = graph.compile_ticket_graph(activities, InMemorySaver())
    ticket = make_ticket("t-lowconf")

    _, state = await start_until_approval_interrupt(compiled, ticket)

    draft = state.get("draft")
    assert draft is not None
    assert draft.confidence < 0.75


async def test_approved_resume_continues_to_resolution() -> None:
    activities = RecordingActivities(
        MockAgent(
            seed=1,
            failure_rate=0.0,
            refund_rate=1.0,
            confidence_range=(0.8, 1.0),
        )
    )
    compiled = graph.compile_ticket_graph(activities, InMemorySaver())
    ticket = make_ticket("t-approved")
    _, state = await start_until_approval_interrupt(compiled, ticket)
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
    activities = RecordingActivities(
        MockAgent(
            seed=1,
            failure_rate=0.0,
            refund_rate=1.0,
            confidence_range=(0.8, 1.0),
        )
    )
    compiled = graph.compile_ticket_graph(activities, InMemorySaver())
    ticket = make_ticket("t-rejected")
    await start_until_approval_interrupt(compiled, ticket)
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
    activities = RecordingActivities(
        MockAgent(
            seed=1,
            failure_rate=0.0,
            refund_rate=1.0,
            confidence_range=(0.8, 1.0),
        )
    )
    compiled = graph.compile_ticket_graph(activities, InMemorySaver())
    ticket = make_ticket("t-timeout")
    await start_until_approval_interrupt(compiled, ticket)

    final = await compiled.ainvoke(
        Command(resume={"kind": "timeout"}),
        config_for(ticket.id),
    )

    assert final["status"] == TicketStatus.ESCALATED
    assert final.get("decision") is None
    assert final.get("result") is None
    assert activities.sent_replies == []


@pytest.mark.integration
async def test_state_survives_a_fresh_checkpointer_process() -> None:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    activities = TicketActivities(
        MockAgent(
            seed=1,
            failure_rate=0.0,
            refund_rate=0.0,
            confidence_range=(0.8, 1.0),
        )
    )
    ticket = make_ticket("t-durable")
    cfg = config_for(ticket.id)

    async with AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(activities, saver)
        await compiled.ainvoke({"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg)

    # A fresh saver (new connection) simulates a brand-new process reading the
    # durable checkpoint back out of Postgres.
    async with AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) as fresh:
        reopened = graph.compile_ticket_graph(activities, fresh)
        snapshot = await reopened.aget_state(cfg)

    state = snapshot.values
    assert state["status"] == TicketStatus.RESOLVED
    assert isinstance(state["ticket"], Ticket)
    assert state["ticket"].id == ticket.id
    assert isinstance(state["draft"], DraftReply)
    assert state["result"].status == TicketStatus.RESOLVED
    assert state["result"].reply_text == state["draft"].reply_text
