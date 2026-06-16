"""Postgres connection and bootstrap helpers."""

from __future__ import annotations

from typing import Protocol

from psycopg_pool import ConnectionPool

from ticketflow import config

BOOTSTRAP_MIGRATION = "000_bootstrap"
TASK_QUEUE_MIGRATION = "001_task_queue"


class _Connection(Protocol):
    def execute(self, sql: str, params: tuple[str, ...] | None = None) -> object: ...

    def commit(self) -> None: ...


class _ConnectionContext(Protocol):
    def __enter__(self) -> _Connection: ...

    def __exit__(self, exc_type, exc, tb) -> object: ...


class _Pool(Protocol):
    def connection(self) -> _ConnectionContext: ...

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
