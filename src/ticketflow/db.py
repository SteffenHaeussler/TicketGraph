"""Postgres connection and bootstrap helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from psycopg_pool import ConnectionPool

from ticketflow import config

BOOTSTRAP_MIGRATION = "000_bootstrap"
TASK_QUEUE_MIGRATION = "001_task_queue"


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


class _Cursor(Protocol):
    def fetchone(self) -> tuple[Any, ...] | None: ...


class _Connection(Protocol):
    def execute(self, sql: str, params: tuple[str, ...] | None = None) -> _Cursor: ...

    def commit(self) -> None: ...


class _Pool(Protocol):
    def connection(self, timeout: float | None = None) -> Any: ...

    def open(self) -> None: ...

    def close(self) -> None: ...


def make_pool(database_url: str | None = None) -> ConnectionPool:
    """Create a Postgres connection pool for the configured database."""
    return ConnectionPool(
        conninfo=database_url or config.DATABASE_URL,
        min_size=1,
        max_size=10,
        open=False,
    )


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
    owned_pool = pool is None
    active_pool = pool or make_pool(database_url)
    try:
        if owned_pool:
            active_pool.open()
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
    finally:
        if owned_pool:
            active_pool.close()

    return _task_from_row(row) if row is not None else None


def bootstrap(database_url: str | None = None, pool: _Pool | None = None) -> None:
    """Create the migration marker and task queue tables.

    Later milestones add the workflow run and read model tables. This function
    is intentionally idempotent so startup can call it safely.

    When no ``pool`` is supplied this owns the pool's lifecycle: it opens it
    before use and closes it afterwards. An injected ``pool`` is assumed to be
    already open and is left open for the caller to manage.
    """
    owned_pool = pool is None
    active_pool = pool or make_pool(database_url)
    try:
        if owned_pool:
            active_pool.open()
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
            conn.commit()
    finally:
        if owned_pool:
            active_pool.close()
