"""LangGraph ticket workflow.

The ``StateGraph`` plus the Postgres checkpointer drive a ticket from
``received`` to ``resolved`` and the run survives a fresh process via the
checkpointer.

The ``classify`` and ``draft`` nodes do not call the agent inline. Each
**enqueues a durable task** onto the Postgres task queue and then suspends with
``interrupt()`` (Milestone 3.2). A separate worker (M5) produces the result and
a runner (M4) resumes the graph with ``Command(resume=<result>)``; on resume the
node re-runs from the top, so the enqueue is idempotent via its idempotency key.

Later milestones refine the remaining nodes: 3.3 turns ``decide_approval`` into
the real approval gate, and 3.5 makes ``execute``/``record`` terminal (refund
ledger, read-model persistence, rejection/escalation replies).
"""

from __future__ import annotations

from typing import TypedDict

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import interrupt

from ticketflow import config, taskqueue
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.db import _Pool
from ticketflow.models import (
    ActionType,
    ApprovalDecision,
    Classification,
    DraftReply,
    Ticket,
    TicketResult,
    TicketStatus,
)
from ticketflow.workflows import CONFIDENCE_THRESHOLD


class TicketState(TypedDict, total=False):
    """State threaded through the ticket workflow graph.

    ``ticket`` is supplied in the initial state and every other key is produced
    by an upstream node. Nodes return partial updates, so all keys are optional
    and reads go through ``state.get(...)``.
    """

    ticket: Ticket
    classification: Classification | None
    draft: DraftReply | None
    needs_approval: bool
    decision: ApprovalDecision | None
    status: TicketStatus
    result: TicketResult | None


def build_ticket_graph(activities: TicketActivities, pool: _Pool) -> StateGraph:
    """Build the uncompiled ticket workflow graph.

    ``pool`` is the Postgres connection pool the dispatching nodes use to
    enqueue agent tasks. ``activities`` carries the side-effect operations used
    by the inline terminal nodes (and, later, the agent worker).

    The graph is linear::

        classify -> draft -> decide_approval -> execute -> record

    ``classify`` and ``draft`` dispatch a task and ``interrupt()``; the rest run
    inline.
    """

    async def classify(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        assert ticket is not None
        with pool.connection() as conn:
            taskqueue.enqueue(
                conn,
                queue_name=config.AGENT_TASK_QUEUE,
                task_type="classify",
                workflow_id=ticket.id,
                payload={"ticket": ticket.model_dump(mode="json")},
                idempotency_key=f"{ticket.id}:classify",
            )
            conn.commit()
        result = interrupt({"task_type": "classify", "workflow_id": ticket.id})
        classification = Classification.model_validate(result)
        return {
            "classification": classification,
            "status": TicketStatus.CLASSIFYING,
        }

    async def draft(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        classification = state.get("classification")
        assert ticket is not None and classification is not None
        with pool.connection() as conn:
            taskqueue.enqueue(
                conn,
                queue_name=config.AGENT_TASK_QUEUE,
                task_type="draft",
                workflow_id=ticket.id,
                payload={
                    "ticket": ticket.model_dump(mode="json"),
                    "classification": classification.model_dump(mode="json"),
                },
                idempotency_key=f"{ticket.id}:draft",
            )
            conn.commit()
        result = interrupt({"task_type": "draft", "workflow_id": ticket.id})
        reply = DraftReply.model_validate(result)
        return {"draft": reply, "status": TicketStatus.DRAFTING}

    async def decide_approval(state: TicketState) -> TicketState:
        reply = state.get("draft")
        assert reply is not None
        needs_approval = (
            reply.action.type == ActionType.REFUND
            or reply.confidence < CONFIDENCE_THRESHOLD
        )
        return {"needs_approval": needs_approval}

    async def execute(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        reply = state.get("draft")
        assert ticket is not None and reply is not None
        await activities.send_reply(ticket, reply.reply_text)
        return {}

    async def record(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        reply = state.get("draft")
        classification = state.get("classification")
        assert ticket is not None and reply is not None and classification is not None
        result = TicketResult(
            ticket_id=ticket.id,
            status=TicketStatus.RESOLVED,
            reply_text=reply.reply_text,
            refund_executed=False,
            model_path=f"{classification.model}/{reply.model}",
        )
        return {"result": result, "status": TicketStatus.RESOLVED}

    builder: StateGraph = StateGraph(TicketState)
    builder.add_node("classify", classify)
    builder.add_node("draft", draft)
    builder.add_node("decide_approval", decide_approval)
    builder.add_node("execute", execute)
    builder.add_node("record", record)

    builder.add_edge(START, "classify")
    builder.add_edge("classify", "draft")
    builder.add_edge("draft", "decide_approval")
    builder.add_edge("decide_approval", "execute")
    builder.add_edge("execute", "record")
    builder.add_edge("record", END)

    return builder


def compile_ticket_graph(
    activities: TicketActivities,
    checkpointer: BaseCheckpointSaver,
    pool: _Pool,
) -> CompiledStateGraph:
    """Compile the ticket workflow graph with a durable ``checkpointer``."""
    return build_ticket_graph(activities, pool).compile(checkpointer=checkpointer)


def default_activities() -> TicketActivities:
    """Build the inline activities used by the demo workflow."""
    return TicketActivities(MockAgent(), database_url=config.DATABASE_URL)
