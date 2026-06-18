"""Tests for the inline LangGraph ticket workflow (Milestone 3.1)."""

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver

from ticketflow import config, graph
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.models import (
    ActionType,
    DraftReply,
    Ticket,
    TicketStatus,
)


def make_ticket(ticket_id: str = "t-1") -> Ticket:
    return Ticket(
        id=ticket_id,
        customer_email="customer@example.com",
        subject="Need help",
        body="My login keeps failing and I want it fixed.",
    )


def config_for(ticket_id: str) -> RunnableConfig:
    return {"configurable": {"thread_id": ticket_id}}


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

    final = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED},
        config_for(ticket.id),
    )

    assert final["draft"].action.type == ActionType.REFUND
    assert final["needs_approval"] is True


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

    final = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED},
        config_for(ticket.id),
    )

    assert final["draft"].confidence < 0.75
    assert final["needs_approval"] is True


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
