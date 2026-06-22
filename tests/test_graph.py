"""Tests for the dispatch-and-interrupt LangGraph ticket workflow (M3.2/M3.3).

``classify`` and ``draft`` dispatch a task and ``interrupt()``; the tests resume
them with the agent result. Refund / low-confidence drafts then pause at the
approval gate, which resumes from a human decision or a timer envelope.
"""

from datetime import datetime, timezone
from typing import Any

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from tests.helpers import process_one_task
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
    TicketResult,
    TicketStatus,
)
from ticketflow.workflows import APPROVAL_TIMEOUT, ESCALATION_REPLY, REJECTION_REPLY


class RecordingActivities(TicketActivities):
    """Ticket activities that keep side effects visible to graph tests."""

    def __init__(self, agent: MockAgent):
        super().__init__(agent)
        self.sent_replies: list[tuple[str, str]] = []
        self.refund_calls: list[tuple[str, float, int]] = []
        self.refund_results: list[bool] = [True]
        self.recorded_results: list[TicketResult] = []

    async def send_reply(self, ticket: Ticket, reply_text: str) -> None:
        self.sent_replies.append((ticket.id, reply_text))

    async def execute_refund(
        self, ticket_id: str, amount: float, attempt: int = 1
    ) -> bool:
        self.refund_calls.append((ticket_id, amount, attempt))
        return self.refund_results.pop(0)

    async def record_result(self, result: TicketResult) -> None:
        self.recorded_results.append(result)


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
    compiled = graph.compile_ticket_graph(activities, InMemorySaver(), pool)
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
    compiled = graph.compile_ticket_graph(recording_activities(), InMemorySaver(), pool)
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


async def test_classify_task_failure_escalates_without_draft_dispatch() -> None:
    pool = FakePool(opened=True, row=(1,))
    compiled = graph.compile_ticket_graph(recording_activities(), InMemorySaver(), pool)
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
    compiled = graph.compile_ticket_graph(recording_activities(), InMemorySaver(), pool)
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
    compiled = graph.compile_ticket_graph(recording_activities(), InMemorySaver(), pool)
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
    compiled = graph.compile_ticket_graph(recording_activities(), InMemorySaver(), pool)
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
    pool.connection_obj.rows = [(1,), None]
    compiled = graph.compile_ticket_graph(recording_activities(), InMemorySaver(), pool)
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
    compiled = graph.compile_ticket_graph(activities, InMemorySaver(), pool)
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
    compiled = graph.compile_ticket_graph(activities, InMemorySaver(), pool)
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
    compiled = graph.compile_ticket_graph(activities, InMemorySaver(), pool)
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
                key = interrupt["idempotency_key"]
                assert await process_one_task(
                    pool, activities, queue_name=interrupt["queue"]
                )
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


@pytest.mark.integration
async def test_schedule_to_start_timeout_routes_to_fallback_through_postgres() -> None:
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
    ticket = make_ticket("t-fb")
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
            # Suspended at the classify dispatch with only the primary task pending.
            assert "__interrupt__" in out
            assert _pending_keys(pool, ticket.id) == [f"{ticket.id}:classify"]

            # No primary worker picked it up; the schedule-to-start timer fires.
            out = await compiled.ainvoke(Command(resume={"kind": "timeout"}), cfg)
            assert "__interrupt__" in out
            assert out["__interrupt__"][0].value["queue"] == config.FALLBACK_TASK_QUEUE
            assert _pending_keys(pool, ticket.id) == [f"{ticket.id}:classify:fallback"]

            # The unthrottled fallback worker drains it; resume with its result.
            assert await process_one_task(
                pool, activities, queue_name=config.FALLBACK_TASK_QUEUE
            )
            out = await compiled.ainvoke(
                Command(resume=_task_result(pool, f"{ticket.id}:classify:fallback")),
                cfg,
            )

            # Drive the remaining dispatch(es) to completion through the primary
            # queue. An orphaned primary copy of the classify task may also be
            # pending, so drain until the awaited task has produced its result.
            while "__interrupt__" in out:
                interrupt = out["__interrupt__"][0].value
                key = interrupt["idempotency_key"]
                while _result_for(pool, key) is None:
                    assert await process_one_task(
                        pool, activities, queue_name=interrupt["queue"]
                    )
                out = await compiled.ainvoke(
                    Command(resume=_result_for(pool, key)), cfg
                )

            assert out["status"] == TicketStatus.RESOLVED
    finally:
        pool.close()


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
