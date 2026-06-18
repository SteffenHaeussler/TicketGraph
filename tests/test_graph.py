"""Tests for the dispatch-and-interrupt LangGraph ticket workflow (M3.2)."""

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
    Classification,
    DraftReply,
    ProposedAction,
    Ticket,
    TicketCategory,
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


def unit_activities() -> TicketActivities:
    # The agent is never called by the graph in M3.2 (the worker owns that);
    # the inline ``execute`` node still needs ``send_reply``.
    return TicketActivities(MockAgent(seed=1, failure_rate=0.0))


def idempotency_keys(pool: FakePool) -> list[object]:
    # taskqueue.enqueue params: (queue, task_type, workflow_id, idempotency_key, ...)
    return [params[3] for params in pool.connection_obj.params]


async def test_happy_path_dispatches_then_resolves() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(unit_activities(), InMemorySaver(), pool)
    ticket = make_ticket()
    cfg = config_for(ticket.id)

    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    assert "__interrupt__" in out  # suspended at the classify dispatch

    out = await compiled.ainvoke(
        Command(resume=make_classification().model_dump(mode="json")), cfg
    )
    assert "__interrupt__" in out  # suspended at the draft dispatch

    final = await compiled.ainvoke(
        Command(resume=reply_draft().model_dump(mode="json")), cfg
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


async def test_decide_approval_flags_refund_drafts() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(unit_activities(), InMemorySaver(), pool)
    ticket = make_ticket("t-refund")
    cfg = config_for(ticket.id)

    await compiled.ainvoke({"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg)
    await compiled.ainvoke(
        Command(resume=make_classification().model_dump(mode="json")), cfg
    )
    final = await compiled.ainvoke(
        Command(resume=refund_draft().model_dump(mode="json")), cfg
    )

    assert final["draft"].action.type == ActionType.REFUND
    assert final["needs_approval"] is True


async def test_decide_approval_flags_low_confidence_drafts() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(unit_activities(), InMemorySaver(), pool)
    ticket = make_ticket("t-lowconf")
    cfg = config_for(ticket.id)

    await compiled.ainvoke({"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg)
    await compiled.ainvoke(
        Command(resume=make_classification().model_dump(mode="json")), cfg
    )
    final = await compiled.ainvoke(
        Command(resume=reply_draft(confidence=0.4).model_dump(mode="json")), cfg
    )

    assert final["draft"].confidence < 0.75
    assert final["needs_approval"] is True


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
