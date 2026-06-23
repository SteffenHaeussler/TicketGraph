"""LangGraph ticket workflow.

The ``StateGraph`` plus the Postgres checkpointer drive a ticket from
``received`` to ``resolved`` and the run survives a fresh process via the
checkpointer.

The ``classify`` and ``draft`` stages do not call the agent inline. Each
**enqueues a durable task** onto the Postgres task queue and then suspends with
``interrupt()`` (Milestone 3.2). A separate worker (M5) produces the result and
a runner (M4) resumes the graph with ``Command(resume=<result>)``. Dispatch and
await are split so queued statuses checkpoint before the graph parks.

Each dispatch also arms a schedule-to-start timer (``wakeup_at = now()+30s``,
Milestone 3.4): if the runner resumes with a ``{"kind": "timeout"}`` envelope
while the task is still ``pending``, the work is re-dispatched to the unthrottled
``ticketflow-agent-fallback`` queue and the original pending task is made
non-runnable.

Approval-needed drafts pause at the approval gate (Milestone 3.3): they
``interrupt()`` and resume from a human decision or a timer envelope.

Terminal side effects also go through the task queue. The graph records the
desired terminal result, dispatches a ``finalize_ticket`` task, and resumes with
the result that the worker persisted after sending replies/refunds.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal, TypedDict

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import interrupt

from ticketflow import config, taskqueue
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.clock import Clock, resolve_clock
from ticketflow.db import _Pool
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
from ticketflow.workflows import (
    APPROVAL_TIMEOUT,
    CONFIDENCE_THRESHOLD,
    ESCALATION_REPLY,
    REJECTION_REPLY,
)


def _is_timeout(resume: Any) -> bool:
    """True if a resume value is a ``{"kind": "timeout"}`` control envelope.

    The runner resumes a dispatch interrupt with either the raw agent result
    (a ``Classification``/``DraftReply`` dict, which has no ``kind`` field) or a
    timeout envelope when the schedule-to-start timer fires.
    """
    return isinstance(resume, dict) and resume.get("kind") == "timeout"


def _is_task_failed(resume: Any) -> bool:
    """True if a queued task failed finally and the workflow should escalate."""
    return isinstance(resume, dict) and resume.get("kind") == "task_failed"


def _failure_classification() -> Classification:
    """Synthetic classification used only to complete escalation bookkeeping."""
    return Classification(
        category=TicketCategory.GENERAL, confidence=0.0, model="failed-agent"
    )


def _failure_draft() -> DraftReply:
    """Synthetic draft used only to route failed agent work to finalization."""
    return DraftReply(
        reply_text=ESCALATION_REPLY,
        action=ProposedAction(type=ActionType.REPLY_ONLY),
        confidence=0.0,
        model="failed-agent",
    )


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
    wakeup_at: datetime | None
    status: TicketStatus
    refund_executed: bool
    result: TicketResult | None


def build_ticket_graph(
    activities: TicketActivities, pool: _Pool, *, clock: Clock | None = None
) -> StateGraph:
    """Build the uncompiled ticket workflow graph.

    ``pool`` is the Postgres connection pool the dispatching nodes use to
    enqueue agent and terminal tasks. ``activities`` is accepted for API
    compatibility with callers; side effects are performed by queued workers.

    ``dispatch_*`` nodes enqueue and checkpoint status; ``await_*`` nodes
    ``interrupt()``. After drafting, the graph either dispatches terminal work
    directly or pauses at the approval gate::

        dispatch_classify -> await_classify -> dispatch_draft -> await_draft
          -> decide_approval -> execute -> record
                              |-> prepare_approval -> await_approval
    """
    del activities
    active_clock = resolve_clock(clock)

    def enqueue_agent_task(
        *, task_type: str, workflow_id: str, payload: dict[str, Any]
    ) -> datetime:
        key = f"{workflow_id}:{task_type}"
        with pool.connection() as conn:
            taskqueue.enqueue(
                conn,
                queue_name=config.AGENT_TASK_QUEUE,
                task_type=task_type,
                workflow_id=workflow_id,
                payload=payload,
                idempotency_key=key,
            )
            conn.commit()
        return active_clock.now() + timedelta(seconds=config.AGENT_SCHEDULE_TO_START_S)

    async def await_agent_task(
        *,
        task_type: str,
        workflow_id: str,
        payload: dict[str, Any],
        wakeup_at: datetime | None,
    ) -> Any:
        """Suspend for an agent result, redispatching to fallback on timeout."""
        key = f"{workflow_id}:{task_type}"
        active_queue = config.AGENT_TASK_QUEUE
        resume = interrupt(
            {
                "idempotency_key": key,
                "task_type": task_type,
                "workflow_id": workflow_id,
                "queue": active_queue,
                "wakeup_at": wakeup_at.isoformat() if wakeup_at else None,
            }
        )
        if _is_timeout(resume):
            with pool.connection() as conn:
                redispatched = taskqueue.cancel_pending(
                    conn, key, reason="redispatched to fallback"
                )
                if redispatched:
                    taskqueue.enqueue(
                        conn,
                        queue_name=config.FALLBACK_TASK_QUEUE,
                        task_type=task_type,
                        workflow_id=workflow_id,
                        payload=payload,
                        idempotency_key=f"{key}:fallback",
                    )
                conn.commit()
            active_queue = (
                config.FALLBACK_TASK_QUEUE if redispatched else config.AGENT_TASK_QUEUE
            )
            active_key = f"{key}:fallback" if redispatched else key
            resume = interrupt(
                {
                    "idempotency_key": active_key,
                    "task_type": task_type,
                    "workflow_id": workflow_id,
                    "queue": active_queue,
                }
            )
        return resume

    async def dispatch_classify(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        assert ticket is not None
        wakeup_at = enqueue_agent_task(
            task_type="classify",
            workflow_id=ticket.id,
            payload={"ticket": ticket.model_dump(mode="json")},
        )
        return {"status": TicketStatus.CLASSIFYING, "wakeup_at": wakeup_at}

    async def await_classify(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        assert ticket is not None
        result = await await_agent_task(
            task_type="classify",
            workflow_id=ticket.id,
            payload={"ticket": ticket.model_dump(mode="json")},
            wakeup_at=state.get("wakeup_at"),
        )
        if _is_task_failed(result):
            return {
                "classification": _failure_classification(),
                "draft": _failure_draft(),
                "needs_approval": False,
                "status": TicketStatus.ESCALATED,
                "wakeup_at": None,
            }
        classification = Classification.model_validate(result)
        return {
            "classification": classification,
            "status": TicketStatus.CLASSIFYING,
            "wakeup_at": None,
        }

    async def dispatch_draft(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        classification = state.get("classification")
        assert ticket is not None and classification is not None
        wakeup_at = enqueue_agent_task(
            task_type="draft",
            workflow_id=ticket.id,
            payload={
                "ticket": ticket.model_dump(mode="json"),
                "classification": classification.model_dump(mode="json"),
            },
        )
        return {"status": TicketStatus.DRAFTING, "wakeup_at": wakeup_at}

    async def await_draft(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        classification = state.get("classification")
        assert ticket is not None and classification is not None
        result = await await_agent_task(
            task_type="draft",
            workflow_id=ticket.id,
            payload={
                "ticket": ticket.model_dump(mode="json"),
                "classification": classification.model_dump(mode="json"),
            },
            wakeup_at=state.get("wakeup_at"),
        )
        if _is_task_failed(result):
            return {
                "draft": _failure_draft(),
                "needs_approval": False,
                "status": TicketStatus.ESCALATED,
                "wakeup_at": None,
            }
        reply = DraftReply.model_validate(result)
        return {"draft": reply, "status": TicketStatus.DRAFTING, "wakeup_at": None}

    async def decide_approval(state: TicketState) -> TicketState:
        reply = state.get("draft")
        assert reply is not None
        needs_approval = (
            reply.action.type == ActionType.REFUND
            or reply.confidence < CONFIDENCE_THRESHOLD
        )
        return {"needs_approval": needs_approval}

    async def prepare_approval(state: TicketState) -> TicketState:
        _ = state
        return {
            "status": TicketStatus.AWAITING_APPROVAL,
            "wakeup_at": active_clock.now() + APPROVAL_TIMEOUT,
        }

    async def await_approval(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        reply = state.get("draft")
        wakeup_at = state.get("wakeup_at")
        assert ticket is not None and reply is not None and wakeup_at is not None

        resume = interrupt(
            {
                "kind": "approval_required",
                "ticket_id": ticket.id,
                "wakeup_at": wakeup_at.isoformat(),
                "draft": reply.model_dump(mode="json"),
            }
        )
        if not isinstance(resume, dict):
            raise ValueError("approval resume payload must be an object")

        kind = resume.get("kind")
        if kind == "timeout":
            return {"status": TicketStatus.ESCALATED}
        if kind != "decision":
            raise ValueError("approval resume kind must be 'decision' or 'timeout'")

        decision = ApprovalDecision.model_validate(resume.get("decision"))
        if decision.approved:
            return {"decision": decision}
        return {"decision": decision, "status": TicketStatus.REJECTED}

    async def execute(state: TicketState) -> TicketState:
        ticket = state.get("ticket")
        reply = state.get("draft")
        classification = state.get("classification")
        assert ticket is not None and reply is not None and classification is not None
        status = state.get("status")

        terminal_status = TicketStatus.RESOLVED
        reply_text = reply.reply_text
        if status == TicketStatus.REJECTED:
            terminal_status = TicketStatus.REJECTED
            reply_text = REJECTION_REPLY
        elif status == TicketStatus.ESCALATED:
            terminal_status = TicketStatus.ESCALATED
            reply_text = ESCALATION_REPLY

        expected_result = TicketResult(
            ticket_id=ticket.id,
            status=terminal_status,
            reply_text=reply_text,
            refund_executed=False,
            model_path=f"{classification.model}/{reply.model}",
        )
        key = f"{ticket.id}:finalize"
        payload = {
            "ticket": ticket.model_dump(mode="json"),
            "action": reply.action.model_dump(mode="json"),
            "result": expected_result.model_dump(mode="json"),
        }
        with pool.connection() as conn:
            taskqueue.enqueue(
                conn,
                queue_name=config.TASK_QUEUE,
                task_type="finalize_ticket",
                workflow_id=ticket.id,
                payload=payload,
                idempotency_key=key,
            )
            conn.commit()

        resume = interrupt(
            {
                "kind": "terminal_task",
                "idempotency_key": key,
                "task_type": "finalize_ticket",
                "workflow_id": ticket.id,
                "queue": config.TASK_QUEUE,
                "result": expected_result.model_dump(mode="json"),
            }
        )
        result = TicketResult.model_validate(resume)
        if result.ticket_id != ticket.id:
            raise ValueError("terminal result ticket_id does not match workflow")
        return {
            "result": result,
            "status": result.status,
            "refund_executed": result.refund_executed,
            "wakeup_at": None,
        }

    def route_after_decide(
        state: TicketState,
    ) -> Literal["prepare_approval", "execute"]:
        return "prepare_approval" if state.get("needs_approval") else "execute"

    def route_after_classify(
        state: TicketState,
    ) -> Literal["dispatch_draft", "execute"]:
        if state.get("status") == TicketStatus.ESCALATED:
            return "execute"
        return "dispatch_draft"

    def route_after_draft(state: TicketState) -> Literal["decide_approval", "execute"]:
        if state.get("status") == TicketStatus.ESCALATED:
            return "execute"
        return "decide_approval"

    builder: StateGraph = StateGraph(TicketState)
    builder.add_node("dispatch_classify", dispatch_classify)
    builder.add_node("await_classify", await_classify)
    builder.add_node("dispatch_draft", dispatch_draft)
    builder.add_node("await_draft", await_draft)
    builder.add_node("decide_approval", decide_approval)
    builder.add_node("prepare_approval", prepare_approval)
    builder.add_node("await_approval", await_approval)
    builder.add_node("execute", execute)

    builder.add_edge(START, "dispatch_classify")
    builder.add_edge("dispatch_classify", "await_classify")
    builder.add_conditional_edges(
        "await_classify",
        route_after_classify,
        {"dispatch_draft": "dispatch_draft", "execute": "execute"},
    )
    builder.add_edge("dispatch_draft", "await_draft")
    builder.add_conditional_edges(
        "await_draft",
        route_after_draft,
        {"decide_approval": "decide_approval", "execute": "execute"},
    )
    builder.add_conditional_edges(
        "decide_approval",
        route_after_decide,
        {"prepare_approval": "prepare_approval", "execute": "execute"},
    )
    builder.add_edge("prepare_approval", "await_approval")
    builder.add_edge("await_approval", "execute")
    builder.add_edge("execute", END)

    return builder


def compile_ticket_graph(
    activities: TicketActivities,
    checkpointer: BaseCheckpointSaver,
    pool: _Pool,
    *,
    clock: Clock | None = None,
) -> CompiledStateGraph:
    """Compile the ticket workflow graph with a durable ``checkpointer``."""
    return build_ticket_graph(activities, pool, clock=clock).compile(
        checkpointer=checkpointer
    )


def default_activities() -> TicketActivities:
    """Build the inline activities used by the demo workflow."""
    return TicketActivities(MockAgent(), database_url=config.DATABASE_URL)
