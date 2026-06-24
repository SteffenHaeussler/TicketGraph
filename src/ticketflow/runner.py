"""The workflow runner: lease runnable workflow runs and advance their graphs.

The runner is the bridge **queue -> graph** (Milestone 4.2). A worker drains the
task queue, produces an agent or terminal result, and *wakes* the run
(``db.wake_run``). The runner then leases the run (``db.claim_run``), reads the
pending ``interrupt()`` envelope from the checkpoint, looks up the awaited task's
result, and resumes the graph with ``Command(resume=<result>)``.

Persistence is a pragmatic outbox: the checkpoint (owned by the
``AsyncPostgresSaver``) and the outbox enqueues (committed inside the graph's
dispatch nodes) each commit in their own transaction, while the runner's own
``workflow_run`` projection + lease release land atomically in ``db.save_run``.
Crash-safety comes from idempotency keys (re-enqueue is a no-op) and lease
re-claim re-deriving state from the durable checkpoint.

The runner resumes when the awaited task result or approval signal is ready, or
when a durable timer is due and should resume with ``{"kind": "timeout"}``.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Protocol

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.types import Command

from ticketflow import config, db, graph, taskqueue
from ticketflow.clock import Clock, resolve_clock
from ticketflow.db import _Pool
from ticketflow.logging import setup_logging
from ticketflow.signals import APPROVAL_DECISION_SIGNAL

logger = logging.getLogger(__name__)

POLL_INTERVAL_S = 1.0


class _Interrupt(Protocol):
    value: Any


class _Snapshot(Protocol):
    @property
    def interrupts(self) -> Sequence[_Interrupt]: ...


class _Graph(Protocol):
    """The slice of the compiled graph the runner drives."""

    async def aget_state(self, config: Any) -> _Snapshot: ...

    async def ainvoke(self, input: Any, config: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class _ResumeValue:
    """A graph resume payload plus the signal row to consume, if any."""

    payload: Any
    consumed_signal_id: int | None = None


@dataclass(frozen=True)
class JanitorResult:
    """Counts of expired leases reclaimed by one janitor pass."""

    tasks: int
    runs: int


def _thread_config(ticket_id: str) -> RunnableConfig:
    """The LangGraph config that addresses a ticket's durable thread."""
    return {"configurable": {"thread_id": ticket_id}}


def _pending_envelope(snapshot: _Snapshot) -> dict[str, Any] | None:
    """Return the run's pending ``interrupt()`` envelope, or ``None``.

    A run parked at a dispatch/terminal/approval node exposes exactly one
    interrupt; a fresh or finished run exposes none.
    """
    interrupts = snapshot.interrupts
    if not interrupts:
        return None
    value = interrupts[0].value
    return value if isinstance(value, dict) else None


def _approval_resume_value(conn: Any, envelope: dict[str, Any]) -> _ResumeValue | None:
    """Return the oldest unconsumed approval decision signal, if one is ready."""
    if envelope.get("kind") != "approval_required":
        return None
    workflow_id = envelope.get("ticket_id") or envelope.get("workflow_id")
    if not isinstance(workflow_id, str):
        return None

    row = conn.execute(
        """
        SELECT id, payload
        FROM pending_signal
        WHERE workflow_id = %s
          AND kind = %s
          AND consumed = false
        ORDER BY created_at, id
        LIMIT 1
        """,
        (workflow_id, APPROVAL_DECISION_SIGNAL),
    ).fetchone()
    if row is None:
        return None
    return _ResumeValue(
        payload={"kind": "decision", "decision": row[1]},
        consumed_signal_id=row[0],
    )


def _resume_value(conn: Any, envelope: dict[str, Any]) -> _ResumeValue | None:
    """The value to resume the graph with, or ``None`` if it is not ready yet.

    Dispatch (``classify``/``draft``) and terminal (``finalize_ticket``)
    envelopes carry an ``idempotency_key``; their resume value is the worker's
    stored task result. Approval envelopes (``kind == "approval_required"``)
    resume from the oldest unconsumed ``approval_decision`` signal.
    """
    approval = _approval_resume_value(conn, envelope)
    if approval is not None:
        return approval

    key = envelope.get("idempotency_key")
    if key is None:
        return None
    row = conn.execute(
        """
        SELECT status, result, error, permanent
        FROM task_queue
        WHERE idempotency_key = %s
          AND (result IS NOT NULL OR status = 'failed')
        """,
        (key,),
    ).fetchone()
    if row is None:
        return None
    status, result, error, permanent = row
    if status == "failed":
        return _ResumeValue(
            {
                "kind": "task_failed",
                "error": error,
                "permanent": permanent,
            }
        )
    return _ResumeValue(result)


def _resume_for_run(
    conn: Any,
    envelope: dict[str, Any],
    wakeup_at: datetime | None,
    *,
    clock: Clock | None = None,
) -> _ResumeValue | None:
    """Return a ready task/signal value, a due timer envelope, or ``None``.

    ``db.claim_run`` should only return rows whose ``wakeup_at`` is null or due,
    and the explicit due check keeps this helper correct for tests and defensive
    callers. A stored task result wins that race so a late-but-ready primary
    result is not needlessly redispatched to fallback.
    """
    resume = _resume_value(conn, envelope)
    if resume is not None:
        return resume
    if _timer_is_due(wakeup_at, clock=clock):
        return _ResumeValue({"kind": "timeout"})
    return None


def _timer_is_due(wakeup_at: datetime | None, *, clock: Clock | None = None) -> bool:
    """Whether a durable timer should resume the parked graph now."""
    if wakeup_at is None:
        return False
    return wakeup_at <= resolve_clock(clock).now()


def _interrupt_envelope_from_output(output: dict[str, Any]) -> dict[str, Any] | None:
    """Return the first interrupt envelope emitted by ``ainvoke`` if present."""
    interrupts = output.get("__interrupt__")
    if not interrupts:
        return None
    value = interrupts[0].value
    return value if isinstance(value, dict) else None


def _parse_wakeup_at(value: Any) -> datetime | None:
    """Parse an interrupt ``wakeup_at`` value from the graph output."""
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"unexpected wakeup_at value: {value!r}")


def _next_wakeup_at(output: dict[str, Any], resume: _ResumeValue) -> datetime | None:
    """The ``workflow_run.wakeup_at`` value to persist after a graph step."""
    _ = resume
    envelope = _interrupt_envelope_from_output(output)
    if envelope is not None:
        return _parse_wakeup_at(envelope.get("wakeup_at"))
    return output.get("wakeup_at")


async def step(
    compiled: _Graph, pool: _Pool, worker_id: str, *, clock: Clock | None = None
) -> bool:
    """Advance at most one runnable workflow run by one resume.

    Returns ``True`` when a run was resumed and its new state persisted, ``False``
    when no run was claimable or the claimed run had no ready result (its lease is
    released without advancing).
    """
    run = db.claim_run(worker_id, pool=pool)
    if run is None:
        return False

    cfg = _thread_config(run.ticket_id)
    snapshot = await compiled.aget_state(cfg)
    envelope = _pending_envelope(snapshot)

    resume: _ResumeValue | None = None
    if envelope is not None:
        with pool.connection() as conn:
            resume = _resume_for_run(conn, envelope, run.wakeup_at, clock=clock)

    if envelope is None or resume is None:
        # Not actionable yet (no task result, approval signal, or due timer).
        # Release the lease, leaving status/wakeup_at untouched.
        db.save_run(
            run.ticket_id,
            status=run.status,
            wakeup_at=run.wakeup_at,
            pool=pool,
        )
        return False

    out = await compiled.ainvoke(Command(resume=resume.payload), cfg)
    db.save_run(
        run.ticket_id,
        status=out["status"],
        wakeup_at=_next_wakeup_at(out, resume),
        consumed_signal_id=resume.consumed_signal_id,
        pool=pool,
    )
    logger.info(
        "advanced run", extra={"ticket_id": run.ticket_id, "status": out["status"]}
    )
    return True


def reclaim_expired_leases(pool: _Pool, *, clock: Clock | None = None) -> JanitorResult:
    """Reclaim expired task and workflow-run leases."""
    with pool.connection() as conn:
        tasks = taskqueue.reclaim_expired(conn)
        conn.commit()

    runs = db.reclaim_expired_runs(pool=pool)
    result = JanitorResult(tasks=tasks, runs=runs)
    if tasks or runs:
        logger.info(
            "reclaimed expired leases",
            extra={"tasks_reclaimed": tasks, "runs_reclaimed": runs},
        )
    return result


async def run_forever(
    compiled: _Graph,
    pool: _Pool,
    worker_id: str,
    *,
    poll_interval: float = POLL_INTERVAL_S,
    janitor_interval: float = config.JANITOR_INTERVAL_S,
    stop: Callable[[], bool] | None = None,
    clock: Clock | None = None,
) -> None:
    """Poll for runnable runs, advancing them until ``stop`` (or cancellation).

    Sleeps ``poll_interval`` whenever a tick finds no work so an idle runner does
    not spin. A claimed-but-not-ready run also counts as no work.
    """
    loop = asyncio.get_running_loop()
    next_janitor_at = 0.0
    while stop is None or not stop():
        now = loop.time()
        if now >= next_janitor_at:
            if clock is None:
                reclaim_expired_leases(pool)
            else:
                reclaim_expired_leases(pool, clock=clock)
            next_janitor_at = now + janitor_interval

        if clock is None:
            advanced = await step(compiled, pool, worker_id)
        else:
            advanced = await step(compiled, pool, worker_id, clock=clock)
        if not advanced:
            await asyncio.sleep(poll_interval)


def _worker_id() -> str:
    """A stable-per-process runner identity for lease ownership."""
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


async def main() -> None:
    """Run the durable workflow runner against the configured Postgres."""
    setup_logging()
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    worker_id = _worker_id()
    logger.info("runner starting", extra={"worker_id": worker_id})
    try:
        async with AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) as saver:
            await saver.setup()
            compiled = graph.compile_ticket_graph(saver, pool)
            await run_forever(compiled, pool, worker_id)
    finally:
        pool.close()


if __name__ == "__main__":
    asyncio.run(main())
