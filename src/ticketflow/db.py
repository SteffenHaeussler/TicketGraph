"""Postgres connection and bootstrap helpers."""

from __future__ import annotations

from typing import Protocol

from psycopg_pool import ConnectionPool

from ticketflow import config

BOOTSTRAP_MIGRATION = "000_bootstrap"


class _Connection(Protocol):
    def execute(self, sql: str, params: tuple[str, ...] | None = None) -> object: ...

    def commit(self) -> None: ...


class _ConnectionContext(Protocol):
    def __enter__(self) -> _Connection: ...

    def __exit__(self, exc_type, exc, tb) -> object: ...


class _Pool(Protocol):
    def connection(self) -> _ConnectionContext: ...

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
    """Create the initial migration marker table.

    Later milestones add the task queue, workflow run, and read model tables.
    This function is intentionally idempotent so startup can call it safely.
    """
    owned_pool = pool is None
    active_pool = pool or make_pool(database_url)
    try:
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
            conn.commit()
    finally:
        if owned_pool:
            active_pool.close()
