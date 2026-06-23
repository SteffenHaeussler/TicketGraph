"""Tests for the workflow runner loop (Milestone 4.2).

Unit tests cover the envelope/resume helpers and ``step``'s claim -> resume ->
persist (and no-op release) decision against a fake graph and pool. The
integration test drives a seeded run ``received -> resolved`` purely through
``runner.step`` interleaved with the worker stub, against real Postgres.
"""

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from tests.helpers import (
    FrozenClock,
    billing_classification,
    drive_until_quiescent,
    refund_draft,
)
from tests.test_db import FakeConnection, FakePool
from ticketflow import config, db, runner, taskqueue
from ticketflow.db import WorkflowRun


class FakeInterrupt:
    def __init__(self, value: Any) -> None:
        self.value = value


class FakeSnapshot:
    def __init__(self, interrupts: list[FakeInterrupt]) -> None:
        self.interrupts = interrupts


class FakeCompiled:
    """Stands in for a compiled graph: records the resume passed to ``ainvoke``."""

    def __init__(self, snapshot: FakeSnapshot, invoke_result: dict | None = None):
        self._snapshot = snapshot
        self._invoke_result = invoke_result or {}
        self.invoked_with: list[Any] = []

    async def aget_state(self, config: Any) -> FakeSnapshot:
        return self._snapshot

    async def ainvoke(self, input: Any, config: Any) -> dict[str, Any]:
        self.invoked_with.append(input)
        return self._invoke_result


def make_run(
    status: str = "classifying", wakeup_at: datetime | None = None
) -> WorkflowRun:
    return WorkflowRun(
        ticket_id="t-1",
        status=status,
        wakeup_at=wakeup_at,
        lease_owner="runner-1",
        lease_expires_at=datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC),
        created_at=datetime(2026, 6, 16, 11, 0, tzinfo=UTC),
        updated_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
    )


def test_pending_envelope_returns_first_interrupt_value():
    envelope = {"idempotency_key": "t-1:classify", "task_type": "classify"}
    snapshot = FakeSnapshot([FakeInterrupt(envelope)])

    assert runner._pending_envelope(snapshot) == envelope


def test_pending_envelope_returns_none_without_interrupts():
    assert runner._pending_envelope(FakeSnapshot([])) is None


def test_resume_value_returns_stored_task_result():
    conn = FakeConnection(row=("done", {"category": "billing"}, None, False))

    value = runner._resume_value(conn, {"idempotency_key": "t-1:classify"})

    assert value == runner._ResumeValue(payload={"category": "billing"})
    assert "FROM task_queue" in conn.sql[-1]
    assert conn.params[-1] == ("t-1:classify",)


def test_resume_value_returns_failed_task_envelope():
    conn = FakeConnection(row=("failed", None, "invalid ticket input", True))

    value = runner._resume_value(conn, {"idempotency_key": "t-1:classify"})

    assert value == runner._ResumeValue(
        payload={
            "kind": "task_failed",
            "error": "invalid ticket input",
            "permanent": True,
        }
    )
    assert "status = 'failed'" in conn.sql[-1]
    assert conn.params[-1] == ("t-1:classify",)


def test_resume_value_is_none_when_result_not_ready():
    conn = FakeConnection(row=None)

    assert runner._resume_value(conn, {"idempotency_key": "t-1:classify"}) is None


def test_resume_value_is_none_for_approval_envelope_without_signal():
    conn = FakeConnection(row=None)

    assert (
        runner._resume_value(
            conn,
            {"kind": "approval_required", "ticket_id": "t-1"},
        )
        is None
    )
    assert "FROM pending_signal" in conn.sql[-1]
    assert conn.params[-1] == ("t-1", "approval_decision")


def test_resume_value_returns_decision_for_pending_approval_signal():
    conn = FakeConnection(
        row=(7, {"approved": True, "approver": "sam@example.com", "note": None})
    )

    value = runner._resume_value(
        conn,
        {"kind": "approval_required", "ticket_id": "t-1"},
    )

    assert value == runner._ResumeValue(
        payload={
            "kind": "decision",
            "decision": {
                "approved": True,
                "approver": "sam@example.com",
                "note": None,
            },
        },
        consumed_signal_id=7,
    )
    assert "FROM pending_signal" in conn.sql[-1]
    assert "ORDER BY created_at, id" in conn.sql[-1]
    assert conn.params[-1] == ("t-1", "approval_decision")


async def test_step_resumes_when_result_is_ready(monkeypatch):
    run = make_run()
    snapshot = FakeSnapshot([FakeInterrupt({"idempotency_key": "t-1:classify"})])
    compiled = FakeCompiled(
        snapshot, invoke_result={"status": "drafting", "wakeup_at": None}
    )
    pool = FakePool(opened=True, row=("done", {"category": "billing"}, None, False))

    monkeypatch.setattr(db, "claim_run", lambda worker_id, pool: run)
    saved: list[dict] = []

    def _save_run(
        ticket_id,
        *,
        status,
        wakeup_at,
        pool,
        consumed_signal_id=None,
        clock=None,
    ):
        saved.append({"ticket_id": ticket_id, "status": status, "wakeup_at": wakeup_at})

    monkeypatch.setattr(
        db,
        "save_run",
        _save_run,
    )

    advanced = await runner.step(compiled, pool, "runner-1")

    assert advanced is True
    assert len(compiled.invoked_with) == 1
    assert saved == [{"ticket_id": "t-1", "status": "drafting", "wakeup_at": None}]


async def test_step_resumes_when_task_failed(monkeypatch):
    run = make_run()
    snapshot = FakeSnapshot([FakeInterrupt({"idempotency_key": "t-1:classify"})])
    compiled = FakeCompiled(
        snapshot, invoke_result={"status": "escalated", "wakeup_at": None}
    )
    pool = FakePool(opened=True, row=("failed", None, "invalid ticket input", True))

    monkeypatch.setattr(db, "claim_run", lambda worker_id, pool: run)
    saved: list[dict] = []
    monkeypatch.setattr(
        db,
        "save_run",
        lambda ticket_id, *, status, wakeup_at, pool, consumed_signal_id=None: (
            saved.append(
                {"ticket_id": ticket_id, "status": status, "wakeup_at": wakeup_at}
            )
        ),
    )

    advanced = await runner.step(compiled, pool, "runner-1")

    assert advanced is True
    assert len(compiled.invoked_with) == 1
    assert compiled.invoked_with[0].resume == {
        "kind": "task_failed",
        "error": "invalid ticket input",
        "permanent": True,
    }
    assert saved == [{"ticket_id": "t-1", "status": "escalated", "wakeup_at": None}]


async def test_step_resumes_due_run_with_timeout_when_result_not_ready(monkeypatch):
    due_at = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)
    run = make_run(wakeup_at=due_at)
    snapshot = FakeSnapshot([FakeInterrupt({"idempotency_key": "t-1:classify"})])
    compiled = FakeCompiled(
        snapshot, invoke_result={"status": "classifying", "wakeup_at": due_at}
    )
    pool = FakePool(opened=True, row=None)  # no stored task result yet

    monkeypatch.setattr(db, "claim_run", lambda worker_id, pool: run)
    saved: list[dict] = []
    monkeypatch.setattr(
        db,
        "save_run",
        lambda ticket_id, *, status, wakeup_at, pool, consumed_signal_id=None: (
            saved.append(
                {"ticket_id": ticket_id, "status": status, "wakeup_at": wakeup_at}
            )
        ),
    )

    advanced = await runner.step(compiled, pool, "runner-1")

    assert advanced is True
    assert len(compiled.invoked_with) == 1
    assert compiled.invoked_with[0].resume == {"kind": "timeout"}
    assert saved == [{"ticket_id": "t-1", "status": "classifying", "wakeup_at": due_at}]


async def test_step_resumes_approval_signal_and_marks_it_consumed(monkeypatch):
    run = make_run(status="awaiting_approval")
    envelope = {"kind": "approval_required", "ticket_id": "t-1"}
    snapshot = FakeSnapshot([FakeInterrupt(envelope)])
    compiled = FakeCompiled(
        snapshot, invoke_result={"status": "resolved", "wakeup_at": None}
    )
    pool = FakePool(
        opened=True,
        row=(7, {"approved": True, "approver": "sam@example.com", "note": None}),
    )

    monkeypatch.setattr(db, "claim_run", lambda worker_id, pool: run)
    saved: list[dict] = []
    monkeypatch.setattr(
        db,
        "save_run",
        lambda ticket_id, *, status, wakeup_at, pool, consumed_signal_id=None: (
            saved.append(
                {
                    "ticket_id": ticket_id,
                    "status": status,
                    "wakeup_at": wakeup_at,
                    "consumed_signal_id": consumed_signal_id,
                }
            )
        ),
    )

    advanced = await runner.step(compiled, pool, "runner-1")

    assert advanced is True
    assert len(compiled.invoked_with) == 1
    assert saved == [
        {
            "ticket_id": "t-1",
            "status": "resolved",
            "wakeup_at": None,
            "consumed_signal_id": 7,
        }
    ]


async def test_step_prefers_ready_result_over_due_timeout(monkeypatch):
    due_at = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)
    run = make_run(wakeup_at=due_at)
    snapshot = FakeSnapshot([FakeInterrupt({"idempotency_key": "t-1:classify"})])
    compiled = FakeCompiled(
        snapshot, invoke_result={"status": "drafting", "wakeup_at": None}
    )
    pool = FakePool(opened=True, row=("done", {"category": "billing"}, None, False))

    monkeypatch.setattr(db, "claim_run", lambda worker_id, pool: run)
    saved: list[dict] = []
    monkeypatch.setattr(
        db,
        "save_run",
        lambda ticket_id, *, status, wakeup_at, pool, consumed_signal_id=None: (
            saved.append(
                {"ticket_id": ticket_id, "status": status, "wakeup_at": wakeup_at}
            )
        ),
    )

    advanced = await runner.step(compiled, pool, "runner-1")

    assert advanced is True
    assert compiled.invoked_with[0].resume == {"category": "billing"}
    assert saved == [{"ticket_id": "t-1", "status": "drafting", "wakeup_at": None}]


async def test_step_clears_stale_wakeup_after_timeout_reinterrupt(monkeypatch):
    due_at = datetime(2026, 6, 16, 12, 0, tzinfo=UTC)
    run = make_run(wakeup_at=due_at)
    snapshot = FakeSnapshot([FakeInterrupt({"idempotency_key": "t-1:classify"})])
    compiled = FakeCompiled(
        snapshot,
        invoke_result={
            "status": "classifying",
            "wakeup_at": due_at,
            "__interrupt__": [
                FakeInterrupt(
                    {
                        "idempotency_key": "t-1:classify:fallback",
                        "task_type": "classify",
                        "workflow_id": "t-1",
                        "queue": "ticketflow-agent-fallback",
                    }
                )
            ],
        },
    )
    pool = FakePool(opened=True, row=None)

    monkeypatch.setattr(db, "claim_run", lambda worker_id, pool: run)
    saved: list[dict] = []
    monkeypatch.setattr(
        db,
        "save_run",
        lambda ticket_id, *, status, wakeup_at, pool, consumed_signal_id=None: (
            saved.append(
                {"ticket_id": ticket_id, "status": status, "wakeup_at": wakeup_at}
            )
        ),
    )

    advanced = await runner.step(compiled, pool, "runner-1")

    assert advanced is True
    assert compiled.invoked_with[0].resume == {"kind": "timeout"}
    assert saved == [{"ticket_id": "t-1", "status": "classifying", "wakeup_at": None}]


async def test_step_releases_without_advancing_when_result_not_ready(monkeypatch):
    run = make_run(wakeup_at=datetime.now(UTC) + timedelta(hours=1))
    snapshot = FakeSnapshot([FakeInterrupt({"idempotency_key": "t-1:classify"})])
    compiled = FakeCompiled(snapshot)
    pool = FakePool(opened=True, row=None)  # no stored task result yet

    monkeypatch.setattr(db, "claim_run", lambda worker_id, pool: run)
    saved: list[dict] = []
    monkeypatch.setattr(
        db,
        "save_run",
        lambda ticket_id, *, status, wakeup_at, pool: saved.append(
            {"ticket_id": ticket_id, "status": status, "wakeup_at": wakeup_at}
        ),
    )

    advanced = await runner.step(compiled, pool, "runner-1")

    assert advanced is False
    assert compiled.invoked_with == []  # the graph was not resumed
    # The lease is released, leaving status/wakeup_at untouched for a later claim.
    assert saved == [
        {"ticket_id": "t-1", "status": "classifying", "wakeup_at": run.wakeup_at}
    ]


async def test_step_uses_injected_clock_before_and_after_timer_due(monkeypatch):
    now = datetime(2026, 6, 23, 12, 0, tzinfo=UTC)
    clock = FrozenClock(now)
    due_at = now + timedelta(minutes=5)
    run = make_run(wakeup_at=due_at)
    snapshot = FakeSnapshot([FakeInterrupt({"idempotency_key": "t-1:classify"})])
    compiled = FakeCompiled(
        snapshot, invoke_result={"status": "classifying", "wakeup_at": due_at}
    )
    pool = FakePool(opened=True, row=None)

    claim_calls: list[object] = []
    monkeypatch.setattr(
        db,
        "claim_run",
        lambda worker_id, *, pool, clock=None: claim_calls.append(clock) or run,
    )
    saved: list[dict] = []

    def _save_run(
        ticket_id,
        *,
        status,
        wakeup_at,
        pool,
        consumed_signal_id=None,
        clock=None,
    ):
        saved.append({"ticket_id": ticket_id, "status": status, "wakeup_at": wakeup_at})

    monkeypatch.setattr(
        db,
        "save_run",
        _save_run,
    )

    advanced = await runner.step(compiled, pool, "runner-1", clock=clock)

    assert advanced is False
    assert compiled.invoked_with == []
    assert claim_calls == [clock]
    assert saved == [{"ticket_id": "t-1", "status": "classifying", "wakeup_at": due_at}]

    clock.advance(timedelta(minutes=5))
    saved.clear()

    advanced = await runner.step(compiled, pool, "runner-1", clock=clock)

    assert advanced is True
    assert compiled.invoked_with[0].resume == {"kind": "timeout"}
    assert saved == [{"ticket_id": "t-1", "status": "classifying", "wakeup_at": due_at}]


async def test_step_returns_false_when_no_run_is_claimable(monkeypatch):
    monkeypatch.setattr(db, "claim_run", lambda worker_id, pool: None)
    called = False

    def _save(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(db, "save_run", _save)

    compiled = FakeCompiled(FakeSnapshot([]))
    assert await runner.step(compiled, FakePool(opened=True), "runner-1") is False
    assert called is False
    assert compiled.invoked_with == []


def test_reclaim_expired_leases_reclaims_tasks_and_runs(monkeypatch, caplog):
    pool = FakePool(opened=True)
    calls: list[tuple[str, object]] = []

    def _reclaim_tasks(conn):
        calls.append(("tasks", conn))
        return 3

    def _reclaim_runs(*, pool):
        calls.append(("runs", pool))
        return 2

    monkeypatch.setattr(taskqueue, "reclaim_expired", _reclaim_tasks)
    monkeypatch.setattr(db, "reclaim_expired_runs", _reclaim_runs)

    with caplog.at_level(logging.INFO, logger="ticketflow.runner"):
        result = runner.reclaim_expired_leases(pool)

    assert result == runner.JanitorResult(tasks=3, runs=2)
    assert calls == [("tasks", pool.connection_obj), ("runs", pool)]
    assert pool.connection_obj.commits == 1
    assert "reclaimed expired leases" in caplog.text


def test_reclaim_expired_leases_does_not_log_zero_counts(monkeypatch, caplog):
    pool = FakePool(opened=True)
    monkeypatch.setattr(taskqueue, "reclaim_expired", lambda conn: 0)
    monkeypatch.setattr(db, "reclaim_expired_runs", lambda *, pool: 0)

    with caplog.at_level(logging.INFO, logger="ticketflow.runner"):
        result = runner.reclaim_expired_leases(pool)

    assert result == runner.JanitorResult(tasks=0, runs=0)
    assert "reclaimed expired leases" not in caplog.text


async def test_run_forever_runs_janitor_on_startup_and_interval(monkeypatch):
    compiled = FakeCompiled(FakeSnapshot([]))
    pool = FakePool(opened=True)
    steps = 0
    janitor_calls = 0
    loop_times = iter([10.0, 14.0, 15.0, 19.0])

    class FakeLoop:
        def time(self):
            return next(loop_times)

    async def _step(compiled, pool, worker_id):
        nonlocal steps
        steps += 1
        return False

    async def _sleep(interval):
        assert interval == 0.01

    def _janitor(pool):
        nonlocal janitor_calls
        janitor_calls += 1
        return runner.JanitorResult(tasks=0, runs=0)

    monkeypatch.setattr(runner, "step", _step)
    monkeypatch.setattr(runner, "reclaim_expired_leases", _janitor)
    monkeypatch.setattr(runner.asyncio, "get_running_loop", lambda: FakeLoop())
    monkeypatch.setattr(runner.asyncio, "sleep", _sleep)

    def _stop():
        return steps >= 3

    await runner.run_forever(
        compiled,
        pool,
        "runner-1",
        poll_interval=0.01,
        janitor_interval=5.0,
        stop=_stop,
    )

    assert steps == 3
    assert janitor_calls == 2


@pytest.mark.integration
async def test_runner_drives_a_seeded_run_to_resolved_through_postgres(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    from langchain_core.runnables import RunnableConfig
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from ticketflow import graph
    from ticketflow.activities import TicketActivities
    from ticketflow.agent.mock import MockAgent
    from ticketflow.models import Ticket, TicketStatus

    activities = TicketActivities(
        MockAgent(
            seed=1, failure_rate=0.0, refund_rate=0.0, confidence_range=(0.8, 1.0)
        )
    )
    ticket = Ticket(
        id=f"t-runner-{uuid.uuid4().hex}",
        customer_email="customer@example.com",
        subject="Need help",
        body="My login keeps failing and I want it fixed.",
    )
    cfg: RunnableConfig = {"configurable": {"thread_id": ticket.id}}

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(activities, saver, postgres_pool)

        # Seed: the initial invoke creates the checkpoint + classify outbox and
        # parks at await_classify; the workflow_run row mirrors that state.
        out = await compiled.ainvoke(
            {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
        )
        assert "__interrupt__" in out
        snapshot = await compiled.aget_state(cfg)
        with postgres_pool.connection() as conn:
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

        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)

        final = await compiled.aget_state(cfg)

    with postgres_pool.connection() as conn:
        row = conn.execute(
            "SELECT status, wakeup_at, lease_owner, lease_expires_at "
            "FROM workflow_run WHERE ticket_id = %s",
            (ticket.id,),
        ).fetchone()

    assert final.values["status"] == TicketStatus.RESOLVED
    assert row is not None
    assert row[0] == "resolved"
    assert row[1] is None  # wakeup_at cleared at the terminal step
    assert row[2] is None  # lease released
    assert row[3] is None


@pytest.mark.integration
async def test_runner_delivers_approval_signal_through_postgres(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    from langchain_core.runnables import RunnableConfig
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from tests.helpers import ScriptedAgent, billing_classification, refund_draft
    from ticketflow import graph
    from ticketflow.activities import TicketActivities
    from ticketflow.models import ApprovalDecision, Ticket, TicketStatus

    ticket = Ticket(
        id=f"t-runner-approval-{uuid.uuid4().hex}",
        customer_email="customer@example.com",
        subject="Refund request",
        body="I was charged twice and need a refund.",
    )
    activities = TicketActivities(
        ScriptedAgent(billing_classification(), refund_draft())
    )
    cfg: RunnableConfig = {"configurable": {"thread_id": ticket.id}}
    decision = ApprovalDecision(approved=True, approver="sam@example.com")

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(activities, saver, postgres_pool)

        out = await compiled.ainvoke(
            {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
        )
        assert "__interrupt__" in out
        snapshot = await compiled.aget_state(cfg)
        with postgres_pool.connection() as conn:
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

        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        awaiting = await compiled.aget_state(cfg)
        assert awaiting.values["status"] == TicketStatus.AWAITING_APPROVAL

        signal_id = db.add_pending_signal(
            ticket.id,
            "approval_decision",
            decision.model_dump(mode="json"),
            pool=postgres_pool,
        )
        await drive_until_quiescent(compiled, postgres_pool, activities, ticket.id)
        final = await compiled.aget_state(cfg)

    with postgres_pool.connection() as conn:
        row = conn.execute(
            "SELECT consumed FROM pending_signal WHERE id = %s", (signal_id,)
        ).fetchone()

    assert final.values["status"] == TicketStatus.RESOLVED
    assert final.values["decision"] == decision
    assert row is not None
    assert row[0] is True


@pytest.mark.integration
async def test_runner_timeout_redispatches_due_agent_task_to_fallback(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    from langchain_core.runnables import RunnableConfig
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from ticketflow import graph
    from ticketflow.activities import TicketActivities
    from ticketflow.agent.mock import MockAgent
    from ticketflow.models import Ticket, TicketStatus

    activities = TicketActivities(MockAgent(seed=1, failure_rate=0.0))
    clock = FrozenClock(datetime(2026, 6, 23, 12, 0, tzinfo=UTC))
    ticket = Ticket(
        id=f"t-runner-timeout-fallback-{uuid.uuid4().hex}",
        customer_email="customer@example.com",
        subject="Need help",
        body="My login keeps failing and I want it fixed.",
    )
    cfg: RunnableConfig = {"configurable": {"thread_id": ticket.id}}

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(
            activities, saver, postgres_pool, clock=clock
        )

        out = await compiled.ainvoke(
            {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
        )
        assert "__interrupt__" in out
        snapshot = await compiled.aget_state(cfg)
        with postgres_pool.connection() as conn:
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

        clock.advance(timedelta(seconds=config.AGENT_SCHEDULE_TO_START_S))
        advanced = await runner.step(compiled, postgres_pool, "runner-1", clock=clock)
        snapshot = await compiled.aget_state(cfg)

    with postgres_pool.connection() as conn:
        run_row = conn.execute(
            "SELECT status, wakeup_at, lease_owner, lease_expires_at "
            "FROM workflow_run WHERE ticket_id = %s",
            (ticket.id,),
        ).fetchone()
        fallback_row = conn.execute(
            "SELECT queue_name, status FROM task_queue "
            "WHERE workflow_id = %s AND idempotency_key = %s",
            (ticket.id, f"{ticket.id}:classify:fallback"),
        ).fetchone()

    assert advanced is True
    assert snapshot.interrupts[0].value["queue"] == config.FALLBACK_TASK_QUEUE
    assert run_row is not None
    assert run_row[0] == "classifying"
    assert run_row[1] is None
    assert run_row[2] is None
    assert run_row[3] is None
    assert fallback_row == (config.FALLBACK_TASK_QUEUE, "pending")


@pytest.mark.integration
async def test_runner_timeout_escalates_due_approval_and_clears_wakeup(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    from langchain_core.runnables import RunnableConfig
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    from langgraph.types import Command

    from ticketflow import graph
    from ticketflow.activities import TicketActivities
    from ticketflow.agent.mock import MockAgent
    from ticketflow.models import Ticket, TicketStatus

    activities = TicketActivities(MockAgent(seed=1, failure_rate=0.0))
    clock = FrozenClock(datetime(2026, 6, 23, 12, 0, tzinfo=UTC))
    ticket = Ticket(
        id=f"t-runner-timeout-approval-{uuid.uuid4().hex}",
        customer_email="customer@example.com",
        subject="Refund request",
        body="I want a refund.",
    )
    cfg: RunnableConfig = {"configurable": {"thread_id": ticket.id}}

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        compiled = graph.compile_ticket_graph(
            activities, saver, postgres_pool, clock=clock
        )

        await compiled.ainvoke({"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg)
        await compiled.ainvoke(
            Command(resume=billing_classification().model_dump(mode="json")),
            cfg,
        )
        out = await compiled.ainvoke(
            Command(resume=refund_draft().model_dump(mode="json")),
            cfg,
        )
        assert "__interrupt__" in out
        assert out["__interrupt__"][0].value["kind"] == "approval_required"
        snapshot = await compiled.aget_state(cfg)
        with postgres_pool.connection() as conn:
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

        wakeup_at = snapshot.values["wakeup_at"]
        assert isinstance(wakeup_at, datetime)
        clock.advance(wakeup_at - clock.now())
        advanced = await runner.step(compiled, postgres_pool, "runner-1", clock=clock)
        snapshot = await compiled.aget_state(cfg)

    with postgres_pool.connection() as conn:
        run_row = conn.execute(
            "SELECT status, wakeup_at, lease_owner, lease_expires_at "
            "FROM workflow_run WHERE ticket_id = %s",
            (ticket.id,),
        ).fetchone()
        terminal_row = conn.execute(
            "SELECT queue_name, task_type, status FROM task_queue "
            "WHERE workflow_id = %s AND idempotency_key = %s",
            (ticket.id, f"{ticket.id}:finalize"),
        ).fetchone()

    assert advanced is True
    assert snapshot.interrupts[0].value["kind"] == "terminal_task"
    assert snapshot.values["status"] == TicketStatus.ESCALATED
    assert run_row is not None
    assert run_row[0] == "escalated"
    assert run_row[1] is None
    assert run_row[2] is None
    assert run_row[3] is None
    assert terminal_row == (config.TASK_QUEUE, "finalize_ticket", "pending")
