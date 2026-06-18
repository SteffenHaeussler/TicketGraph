"""Inline LangGraph ticket workflow (Milestone 3.1).

A first proof that the LangGraph ``StateGraph`` plus the Postgres checkpointer
work end to end. The nodes call the agent **inline** -- no task queue, no
``interrupt()`` -- so a ticket runs straight through to ``resolved`` and its
state survives a fresh process via the checkpointer.

Later milestones refine individual nodes: 3.2 turns ``classify``/``draft`` into
enqueue-and-interrupt, 3.3 turns ``decide_approval`` into the real approval
gate, and 3.5 makes ``execute``/``record`` terminal (refund ledger, read-model
persistence, rejection/escalation replies).
"""

from __future__ import annotations

from typing import TypedDict

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from ticketflow import config
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
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


def build_ticket_graph(activities: TicketActivities) -> StateGraph:
    """Build the uncompiled ticket workflow graph backed by ``activities``.

    The graph is linear for Milestone 3.1::

        classify -> draft -> decide_approval -> execute -> record
    """

    async def classify(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        assert ticket is not None
        classification = await activities.classify_ticket(ticket)
        return {
            "classification": classification,
            "status": TicketStatus.CLASSIFYING,
        }

    async def draft(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        classification = state.get("classification")
        assert ticket is not None and classification is not None
        reply = await activities.draft_reply(ticket, classification)
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
) -> CompiledStateGraph:
    """Compile the ticket workflow graph with a durable ``checkpointer``."""
    return build_ticket_graph(activities).compile(checkpointer=checkpointer)


def default_activities() -> TicketActivities:
    """Build the inline activities used by the demo workflow."""
    return TicketActivities(MockAgent(), database_url=config.DATABASE_URL)
