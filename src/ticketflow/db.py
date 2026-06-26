"""Postgres connection and bootstrap helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from ticketflow import config

BOOTSTRAP_MIGRATION = "000_bootstrap"
TASK_QUEUE_MIGRATION = "001_task_queue"
READ_MODEL_MIGRATION = "002_read_model"
WORKFLOW_RUN_MIGRATION = "003_workflow_run"
PENDING_SIGNAL_MIGRATION = "004_pending_signal"
PENDING_SIGNAL_UNIQUE_MIGRATION = "005_pending_signal_unique_unconsumed"
SENT_REPLY_GUARD_MIGRATION = "006_sent_reply_guard"


@dataclass(frozen=True)
class QueuedTask:
    """Task row leased from the Postgres task queue."""

    id: int
    queue_name: str
    task_type: str
    workflow_id: str
    payload: dict[str, Any]
    idempotency_key: str
    status: str
    attempts: int
    max_attempts: int
    available_at: datetime
    enqueued_at: datetime
    lease_owner: str | None
    lease_expires_at: datetime | None
    result: dict[str, Any] | None
    error: str | None
    permanent: bool


@dataclass(frozen=True)
class WorkflowRun:
    """Workflow run row leased by the runner pool."""

    ticket_id: str
    status: str
    wakeup_at: datetime | None
    lease_owner: str | None
    lease_expires_at: datetime | None
    created_at: datetime
    updated_at: datetime


class _Cursor(Protocol):
    def fetchone(self) -> tuple[Any, ...] | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...


class _Connection(Protocol):
    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> _Cursor: ...

    def commit(self) -> None: ...


class _Pool(Protocol):
    def connection(self, timeout: float | None = None) -> Any: ...

    def open(self) -> None: ...

    def close(self) -> None: ...


class _AsyncPool(Protocol):
    def connection(self, timeout: float | None = None) -> Any: ...

    async def open(self) -> None: ...

    async def close(self) -> None: ...


def make_pool(database_url: str | None = None) -> ConnectionPool:
    """Create a Postgres connection pool for the configured database."""
    return ConnectionPool(
        conninfo=database_url or config.DATABASE_URL,
        min_size=1,
        max_size=10,
        open=False,
    )


def make_async_pool(database_url: str | None = None) -> AsyncConnectionPool:
    """Create an async Postgres connection pool for the configured database."""
    return AsyncConnectionPool(
        conninfo=database_url or config.DATABASE_URL,
        min_size=1,
        max_size=10,
        open=False,
    )


@contextmanager
def managed_pool(
    *, database_url: str | None = None, pool: _Pool | None = None
) -> Iterator[_Pool]:
    """Yield an open pool, owning lifecycle only when no pool was injected."""
    if pool is not None:
        yield pool
        return

    active_pool = make_pool(database_url)
    active_pool.open()
    try:
        yield active_pool
    finally:
        active_pool.close()


@asynccontextmanager
async def managed_async_pool(
    *, database_url: str | None = None, pool: _AsyncPool | None = None
) -> AsyncIterator[_AsyncPool]:
    """Yield an open async pool, owning lifecycle only when no pool was injected."""
    if pool is not None:
        yield pool
        return

    active_pool = make_async_pool(database_url)
    await active_pool.open()
    try:
        yield active_pool
    finally:
        await active_pool.close()


def ping(
    *,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> None:
    """Verify Postgres connectivity by running ``SELECT 1``.

    Raises on failure so callers can treat a clean return as "database
    reachable". An injected ``pool`` is assumed open and is left open for the
    caller; otherwise this owns the pool's lifecycle.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            conn.execute("SELECT 1")


def timestamp_after(conn: Any, delta: timedelta) -> datetime:
    """Return a timestamp ``delta`` after the database's current time."""
    row = conn.execute("SELECT now() + %s::interval", (delta,)).fetchone()
    assert row is not None
    return row[0]


async def atimestamp_after(conn: Any, delta: timedelta) -> datetime:
    """Async variant of ``timestamp_after``."""
    cursor = await conn.execute("SELECT now() + %s::interval", (delta,))
    row = await cursor.fetchone()
    assert row is not None
    return row[0]


def _task_from_row(row: tuple[Any, ...]) -> QueuedTask:
    return QueuedTask(
        id=row[0],
        queue_name=row[1],
        task_type=row[2],
        workflow_id=row[3],
        payload=row[4],
        idempotency_key=row[5],
        status=row[6],
        attempts=row[7],
        max_attempts=row[8],
        available_at=row[9],
        enqueued_at=row[10],
        lease_owner=row[11],
        lease_expires_at=row[12],
        result=row[13],
        error=row[14],
        permanent=row[15],
    )


def dequeue(
    queue_name: str,
    worker_id: str,
    *,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> QueuedTask | None:
    """Lease one due pending task from ``queue_name`` for ``worker_id``."""
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE task_queue
                SET status = 'leased',
                    lease_owner = %s,
                    lease_expires_at = now() + interval '30 seconds',
                    attempts = attempts + 1
                WHERE id = (
                    SELECT id
                    FROM task_queue
                    WHERE queue_name = %s
                      AND status = 'pending'
                      AND available_at <= now()
                    ORDER BY available_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, queue_name, task_type, workflow_id, payload,
                          idempotency_key, status, attempts, max_attempts,
                          available_at, enqueued_at, lease_owner,
                          lease_expires_at, result, error, permanent
                """,
                (worker_id, queue_name),
            ).fetchone()
            conn.commit()

    return _task_from_row(row) if row is not None else None


def _run_from_row(row: tuple[Any, ...]) -> WorkflowRun:
    return WorkflowRun(
        ticket_id=row[0],
        status=row[1],
        wakeup_at=row[2],
        lease_owner=row[3],
        lease_expires_at=row[4],
        created_at=row[5],
        updated_at=row[6],
    )


def claim_run(
    worker_id: str,
    *,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> WorkflowRun | None:
    """Lease one runnable workflow run for ``worker_id``.

    A run is claimable when it is not in a terminal status, its lease is free
    or expired, and it is due to run (``wakeup_at`` is null or in the past).
    Terminal rows are claimable only after a worker explicitly wakes them by
    setting ``wakeup_at``; this lets the runner consume terminal task results
    without polling already-settled rows. The oldest matching run is leased with
    ``FOR UPDATE SKIP LOCKED`` so several runner processes can claim disjoint
    runs concurrently.

    The finer "an agent result or approval signal is ready" predicate is layered
    on by the runner in later milestones; for now a future ``wakeup_at`` (an
    armed approval/fallback timer) keeps a run unclaimed until the timer is due.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE workflow_run
                SET lease_owner = %s,
                    lease_expires_at = now() + interval '30 seconds',
                    updated_at = now()
                WHERE ticket_id = (
                    SELECT ticket_id
                    FROM workflow_run
                    WHERE (
                        status NOT IN ('resolved', 'rejected', 'escalated')
                        OR (wakeup_at IS NOT NULL AND wakeup_at <= now())
                      )
                      AND (lease_expires_at IS NULL OR lease_expires_at < now())
                      AND (wakeup_at IS NULL OR wakeup_at <= now())
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING ticket_id, status, wakeup_at, lease_owner,
                          lease_expires_at, created_at, updated_at
                """,
                (worker_id,),
            ).fetchone()
            conn.commit()

    return _run_from_row(row) if row is not None else None


def reclaim_expired_runs(
    *,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> int:
    """Clear expired workflow-run leases so another runner can claim them."""
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            row = conn.execute(
                """
                WITH reclaimed AS (
                    UPDATE workflow_run
                    SET lease_owner = NULL,
                        lease_expires_at = NULL,
                        updated_at = now()
                    WHERE lease_expires_at < now()
                    RETURNING ticket_id
                )
                SELECT count(*) FROM reclaimed
                """,
            ).fetchone()
            conn.commit()

    assert row is not None
    return int(row[0])


def save_run(
    ticket_id: str,
    *,
    status: str,
    wakeup_at: datetime | None,
    consumed_signal_id: int | None = None,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> None:
    """Persist a workflow run's new ``status``/``wakeup_at`` and release its lease.

    The runner calls this after advancing the graph: the status projection, the
    next timer, and the lease release land in one ``UPDATE`` (one transaction) so
    a re-claim never sees a half-applied step. ``status`` must satisfy the
    ``workflow_run`` CHECK constraint.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            conn.execute(
                """
                UPDATE workflow_run
                SET status = %s,
                    wakeup_at = %s,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = now()
                WHERE ticket_id = %s
                """,
                (status, wakeup_at, ticket_id),
            )
            if consumed_signal_id is not None:
                conn.execute(
                    """
                    UPDATE pending_signal
                    SET consumed = true,
                        consumed_at = now()
                    WHERE id = %s
                    """,
                    (consumed_signal_id,),
                )
            conn.commit()


def create_run(
    ticket_id: str,
    *,
    status: str,
    wakeup_at: datetime | None,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> None:
    """Insert the initial ``workflow_run`` projection for a freshly started ticket.

    The API seeds the durable graph checkpoint and the initial outbox task, then
    calls this to record the run's status projection so the runner can lease it.
    ``lease_owner``/``lease_expires_at`` stay NULL (unleased) and the timestamps
    default to ``now()``; ``status`` must satisfy the ``workflow_run`` CHECK
    constraint.

    The insert is idempotent (``ON CONFLICT (ticket_id) DO NOTHING``): the runner's
    orphan reconciler may race this call and create the same row first from the
    checkpoint, deriving an identical ``status``/``wakeup_at``, so the loser is a
    harmless no-op rather than a duplicate-key error.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO workflow_run (ticket_id, status, wakeup_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (ticket_id) DO NOTHING
                """,
                (ticket_id, status, wakeup_at),
            )
            conn.commit()


async def acreate_run(
    ticket_id: str,
    *,
    status: str,
    wakeup_at: datetime | None,
    database_url: str | None = None,
    pool: _AsyncPool | None = None,
) -> None:
    """Async variant of ``create_run`` for FastAPI request paths.

    Idempotent like ``create_run``: ``ON CONFLICT (ticket_id) DO NOTHING`` lets the
    runner's orphan reconciler win the race without breaking ``create_ticket``.
    """
    async with managed_async_pool(database_url=database_url, pool=pool) as active_pool:
        async with active_pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO workflow_run (ticket_id, status, wakeup_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (ticket_id) DO NOTHING
                """,
                (ticket_id, status, wakeup_at),
            )
            await conn.commit()


def wake_run(
    ticket_id: str,
    *,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> None:
    """Make a run claimable now by pulling its ``wakeup_at`` to the present.

    An awaiting run carries a future ``wakeup_at`` (the 30s schedule-to-start or
    24h approval timer), so the runner will not claim it until the timer is due.
    When the awaited result lands a worker calls this to wake the run so the
    runner picks it up immediately rather than waiting out the timer.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            conn.execute(
                """
                UPDATE workflow_run
                SET wakeup_at = now(),
                    updated_at = now()
                WHERE ticket_id = %s
                """,
                (ticket_id,),
            )
            conn.commit()


def add_pending_signal(
    workflow_id: str,
    kind: str,
    payload: dict[str, Any],
    *,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> int:
    """Persist an unconsumed workflow signal and wake the target run.

    The insert and wake happen in one transaction so a caller cannot commit a
    signal without making its workflow run claimable.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            row = conn.execute(
                """
                INSERT INTO pending_signal (workflow_id, kind, payload)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (workflow_id, kind, Jsonb(payload)),
            ).fetchone()
            assert row is not None
            conn.execute(
                """
                UPDATE workflow_run
                SET wakeup_at = now(),
                    updated_at = now()
                WHERE ticket_id = %s
                """,
                (workflow_id,),
            )
            conn.commit()

    return int(row[0])


def list_runs_by_status(
    status: str,
    *,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> list[str]:
    """Return workflow ids whose projected run status matches ``status``."""
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT ticket_id
                FROM workflow_run
                WHERE status = %s
                ORDER BY created_at, ticket_id
                """,
                (status,),
            ).fetchall()
            conn.commit()

    return [str(row[0]) for row in rows]


def list_orphaned_checkpoint_threads(
    *,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> list[str]:
    """Return checkpoint thread ids that have no ``workflow_run`` projection.

    ``api.create_ticket`` seeds the durable checkpoint (owned by LangGraph's
    ``checkpoints`` table, keyed by ``thread_id`` = ticket id) and *then* inserts
    the ``workflow_run`` row in a separate transaction. A crash in between strands
    a checkpoint with no run row, which the runner can never lease. These threads
    are exactly the orphans the reconciler rebuilds.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT c.thread_id
                FROM checkpoints c
                WHERE NOT EXISTS (
                    SELECT 1 FROM workflow_run w WHERE w.ticket_id = c.thread_id
                )
                """,
            ).fetchall()
            conn.commit()

    return [str(row[0]) for row in rows]


def add_pending_signal_if_waiting(
    workflow_id: str,
    kind: str,
    payload: dict[str, Any],
    *,
    waiting_status: str,
    database_url: str | None = None,
    pool: _Pool | None = None,
) -> int | None:
    """Persist a signal only when its workflow is in ``waiting_status``.

    The conditional insert and wake run in one transaction. A partial unique
    index on unconsumed signals makes duplicate submissions lose the race and
    return ``None`` instead of inserting another pending decision.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            row = conn.execute(
                """
                WITH waiting AS (
                    SELECT ticket_id
                    FROM workflow_run
                    WHERE ticket_id = %s
                      AND status = %s
                    FOR UPDATE
                ),
                inserted AS (
                    INSERT INTO pending_signal (workflow_id, kind, payload)
                    SELECT ticket_id, %s, %s
                    FROM waiting
                    ON CONFLICT (workflow_id, kind) WHERE consumed = false DO NOTHING
                    RETURNING id, workflow_id
                ),
                woken AS (
                    UPDATE workflow_run
                    SET wakeup_at = now(),
                        updated_at = now()
                    WHERE ticket_id IN (SELECT workflow_id FROM inserted)
                    RETURNING ticket_id
                )
                SELECT id FROM inserted
                """,
                (workflow_id, waiting_status, kind, Jsonb(payload)),
            ).fetchone()
            conn.commit()

    return int(row[0]) if row is not None else None


def bootstrap(database_url: str | None = None, pool: _Pool | None = None) -> None:
    """Create the migration marker and durable workflow support tables.

    This function is intentionally idempotent so startup can call it safely.

    When no ``pool`` is supplied this owns the pool's lifecycle: it opens it
    before use and closes it afterwards. An injected ``pool`` is assumed to be
    already open and is left open for the caller to manage.
    """
    with managed_pool(database_url=database_url, pool=pool) as active_pool:
        with active_pool.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version text PRIMARY KEY,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES (%s)
                ON CONFLICT (version) DO NOTHING
                """,
                (BOOTSTRAP_MIGRATION,),
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_queue (
                    id              bigserial PRIMARY KEY,
                    queue_name      text        NOT NULL,
                    task_type       text        NOT NULL,
                    workflow_id     text        NOT NULL,
                    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
                    idempotency_key text        NOT NULL UNIQUE,
                    status          text        NOT NULL DEFAULT 'pending',
                    attempts        integer     NOT NULL DEFAULT 0,
                    max_attempts    integer     NOT NULL DEFAULT 5,
                    available_at    timestamptz NOT NULL DEFAULT now(),
                    enqueued_at     timestamptz NOT NULL DEFAULT now(),
                    lease_owner     text,
                    lease_expires_at timestamptz,
                    result          jsonb,
                    error           text,
                    permanent       boolean     NOT NULL DEFAULT false,
                    CHECK (status IN ('pending', 'leased', 'done', 'failed'))
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_task_queue_dispatch
                ON task_queue (queue_name, available_at)
                WHERE status = 'pending'
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES (%s)
                ON CONFLICT (version) DO NOTHING
                """,
                (TASK_QUEUE_MIGRATION,),
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS refunds (
                    ticket_id   text             PRIMARY KEY,
                    amount      double precision NOT NULL,
                    recorded_at timestamptz      NOT NULL DEFAULT now()
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS refund_attempts (
                    id           bigserial   PRIMARY KEY,
                    ticket_id    text        NOT NULL,
                    attempt      integer     NOT NULL,
                    attempted_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ticket_results (
                    ticket_id text PRIMARY KEY,
                    data      jsonb NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES (%s)
                ON CONFLICT (version) DO NOTHING
                """,
                (READ_MODEL_MIGRATION,),
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS workflow_run (
                    ticket_id        text        PRIMARY KEY,
                    status           text        NOT NULL DEFAULT 'received',
                    wakeup_at        timestamptz,
                    lease_owner      text,
                    lease_expires_at timestamptz,
                    created_at       timestamptz NOT NULL DEFAULT now(),
                    updated_at       timestamptz NOT NULL DEFAULT now(),
                    CHECK (status IN ('received', 'classifying', 'drafting',
                        'awaiting_approval', 'resolved', 'rejected', 'escalated'))
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_workflow_run_status
                ON workflow_run (status)
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES (%s)
                ON CONFLICT (version) DO NOTHING
                """,
                (WORKFLOW_RUN_MIGRATION,),
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_signal (
                    id          bigserial   PRIMARY KEY,
                    workflow_id text        NOT NULL,
                    kind        text        NOT NULL,
                    payload     jsonb       NOT NULL,
                    consumed    boolean     NOT NULL DEFAULT false,
                    created_at  timestamptz NOT NULL DEFAULT now(),
                    consumed_at timestamptz
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_pending_signal_unconsumed
                ON pending_signal (workflow_id, kind, created_at, id)
                WHERE consumed = false
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES (%s)
                ON CONFLICT (version) DO NOTHING
                """,
                (PENDING_SIGNAL_MIGRATION,),
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_signal_unconsumed_kind
                ON pending_signal (workflow_id, kind)
                WHERE consumed = false
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES (%s)
                ON CONFLICT (version) DO NOTHING
                """,
                (PENDING_SIGNAL_UNIQUE_MIGRATION,),
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sent_replies (
                    ticket_id      text        PRIMARY KEY,
                    customer_email text        NOT NULL,
                    reply_text     text        NOT NULL,
                    recorded_at    timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reply_attempts (
                    id           bigserial   PRIMARY KEY,
                    ticket_id    text        NOT NULL,
                    attempt      integer     NOT NULL,
                    attempted_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_reply_attempts_ticket
                ON reply_attempts (ticket_id, attempted_at, id)
                """
            )
            conn.execute(
                """
                INSERT INTO schema_migrations (version)
                VALUES (%s)
                ON CONFLICT (version) DO NOTHING
                """,
                (SENT_REPLY_GUARD_MIGRATION,),
            )
            conn.commit()
