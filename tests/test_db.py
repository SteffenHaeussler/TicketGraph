from datetime import UTC, datetime, timedelta

import pytest

from ticketflow import db

LEASED_ROW = (
    1,
    "q",
    "classify",
    "workflow-1",
    {"ticket_id": "workflow-1"},
    "workflow-1:classify",
    "leased",
    1,
    5,
    datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
    datetime(2026, 6, 16, 11, 0, tzinfo=UTC),
    "worker-1",
    datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC),
    None,
    None,
    False,
)

CLAIMED_RUN_ROW = (
    "ticket-1",
    "classifying",
    None,
    "runner-1",
    datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC),
    datetime(2026, 6, 16, 11, 0, tzinfo=UTC),
    datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
)


class FakeCursor:
    def __init__(
        self,
        row: tuple[object, ...] | None,
        rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.row = row
        self.rows = rows or []

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


class FakeConnection:
    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self.sql: list[str] = []
        self.params: list[tuple[object, ...]] = []
        self.commits = 0
        self.row = row
        self.rows: list[tuple[object, ...] | list[tuple[object, ...]] | None] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> FakeCursor:
        self.sql.append(sql)
        if params is not None:
            self.params.append(params)
        if "SELECT now() + %s::interval" in sql:
            if self.rows:
                result = self.rows.pop(0)
                if isinstance(result, tuple) or result is None:
                    return FakeCursor(result)
                return FakeCursor(None, result)
            if self.row is not None and self.row != (1,):
                return FakeCursor(self.row)
            assert params is not None
            delta = params[0]
            assert isinstance(delta, timedelta)
            return FakeCursor((datetime.now(UTC) + delta,))
        if self.rows:
            result = self.rows.pop(0)
            if isinstance(result, list):
                return FakeCursor(None, result)
            return FakeCursor(result)
        return FakeCursor(self.row)

    def commit(self) -> None:
        self.commits += 1


class FakeConnectionContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> object:
        return None


class FakePool:
    def __init__(
        self, *, opened: bool = False, row: tuple[object, ...] | None = None
    ) -> None:
        self.connection_obj = FakeConnection(row)
        self.opened = opened
        self.closed = False

    def open(self) -> None:
        self.opened = True

    def connection(self, timeout: float | None = None) -> FakeConnectionContext:
        del timeout
        if not self.opened:
            raise AssertionError("connection() called before open()")
        return FakeConnectionContext(self.connection_obj)

    def close(self) -> None:
        self.closed = True


class AsyncFakeCursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row

    async def fetchone(self) -> tuple[object, ...] | None:
        return self.row


class AsyncFakeConnection:
    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self.sql: list[str] = []
        self.params: list[tuple[object, ...]] = []
        self.commits = 0
        self.row = row

    async def execute(
        self, sql: str, params: tuple[object, ...] | None = None
    ) -> AsyncFakeCursor:
        self.sql.append(sql)
        if params is not None:
            self.params.append(params)
        if (
            "SELECT now() + %s::interval" in sql
            and self.row is None
            and params is not None
        ):
            delta = params[0]
            assert isinstance(delta, timedelta)
            return AsyncFakeCursor((datetime.now(UTC) + delta,))
        return AsyncFakeCursor(self.row)

    async def commit(self) -> None:
        self.commits += 1


class AsyncFakeConnectionContext:
    def __init__(self, connection: AsyncFakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> AsyncFakeConnection:
        return self.connection

    async def __aexit__(self, exc_type, exc, tb) -> object:
        return None


class AsyncFakePool:
    def __init__(self, *, opened: bool = False) -> None:
        self.connection_obj = AsyncFakeConnection()
        self.opened = opened
        self.closed = False

    async def open(self) -> None:
        self.opened = True

    def connection(self, timeout: float | None = None) -> AsyncFakeConnectionContext:
        del timeout
        if not self.opened:
            raise AssertionError("connection() called before open()")
        return AsyncFakeConnectionContext(self.connection_obj)

    async def close(self) -> None:
        self.closed = True


async def test_atimestamp_after_uses_database_now() -> None:
    conn = AsyncFakeConnection(row=(datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC),))

    timestamp = await db.atimestamp_after(conn, timedelta(seconds=30))

    assert timestamp == datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)
    assert "SELECT now() + %s::interval" in conn.sql[-1]
    assert conn.params[-1] == (timedelta(seconds=30),)


def test_make_pool_uses_database_url_from_config(monkeypatch):
    calls: list[dict[str, object]] = []

    class RecordingPool:
        def __init__(self, **kwargs: object) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(db, "ConnectionPool", RecordingPool)
    monkeypatch.setattr(db.config, "DATABASE_URL", "postgresql://example/tickets")
    monkeypatch.setattr(db.config, "DB_POOL_MAX_SIZE", 24)

    assert isinstance(db.make_pool(), RecordingPool)

    assert calls == [
        {
            "conninfo": "postgresql://example/tickets",
            "min_size": 1,
            "max_size": 24,
            "open": False,
        }
    ]


def test_make_pool_accepts_explicit_max_size(monkeypatch):
    calls: list[dict[str, object]] = []

    class RecordingPool:
        def __init__(self, **kwargs: object) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(db, "ConnectionPool", RecordingPool)

    assert isinstance(
        db.make_pool("postgresql://override/tickets", max_size=31), RecordingPool
    )

    assert calls == [
        {
            "conninfo": "postgresql://override/tickets",
            "min_size": 1,
            "max_size": 31,
            "open": False,
        }
    ]


def test_make_async_pool_uses_database_url_from_config(monkeypatch):
    calls: list[dict[str, object]] = []

    class RecordingAsyncPool:
        def __init__(self, **kwargs: object) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(db, "AsyncConnectionPool", RecordingAsyncPool)
    monkeypatch.setattr(db.config, "DATABASE_URL", "postgresql://example/tickets")
    monkeypatch.setattr(db.config, "DB_POOL_MAX_SIZE", 24)

    assert isinstance(db.make_async_pool(), RecordingAsyncPool)

    assert calls == [
        {
            "conninfo": "postgresql://example/tickets",
            "min_size": 1,
            "max_size": 24,
            "open": False,
        }
    ]


def test_make_async_pool_accepts_explicit_max_size(monkeypatch):
    calls: list[dict[str, object]] = []

    class RecordingAsyncPool:
        def __init__(self, **kwargs: object) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(db, "AsyncConnectionPool", RecordingAsyncPool)

    assert isinstance(
        db.make_async_pool("postgresql://override/tickets", max_size=31),
        RecordingAsyncPool,
    )

    assert calls == [
        {
            "conninfo": "postgresql://override/tickets",
            "min_size": 1,
            "max_size": 31,
            "open": False,
        }
    ]


def test_bootstrap_runs_pending_migrations_in_order_with_markers():
    pool = FakePool(opened=True)
    pool.connection_obj.rows = [None, None, []]

    migrations = (
        db.Migration(
            "001_create_probe", ("CREATE TABLE migration_probe (id integer)",)
        ),
        db.Migration(
            "002_alter_probe", ("ALTER TABLE migration_probe ADD COLUMN name text",)
        ),
    )

    db.bootstrap(pool=pool, migrations=migrations)

    sql = pool.connection_obj.sql
    assert pool.connection_obj.commits == 1
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in pool.connection_obj.sql[0]
    assert "pg_advisory_xact_lock(hashtext" in pool.connection_obj.sql[1]
    assert "SELECT version FROM schema_migrations" in pool.connection_obj.sql[2]
    assert "CREATE TABLE migration_probe" in sql[3]
    assert "INSERT INTO schema_migrations" in sql[4]
    assert "ALTER TABLE migration_probe ADD COLUMN name text" in sql[5]
    assert "INSERT INTO schema_migrations" in sql[6]
    assert pool.connection_obj.params == [
        ("001_create_probe",),
        ("002_alter_probe",),
    ]
    assert pool.closed is False


def test_bootstrap_skips_already_applied_migrations():
    pool = FakePool(opened=True)
    pool.connection_obj.rows = [None, None, [("001_create_probe",)]]

    migrations = (
        db.Migration(
            "001_create_probe", ("CREATE TABLE migration_probe (id integer)",)
        ),
    )

    db.bootstrap(pool=pool, migrations=migrations)

    sql = "\n".join(pool.connection_obj.sql)
    assert "CREATE TABLE migration_probe" not in sql
    assert pool.connection_obj.params == []
    assert pool.connection_obj.commits == 1


def test_bootstrap_rejects_duplicate_migration_versions():
    pool = FakePool(opened=True)
    migrations = (
        db.Migration("001_same", ("SELECT 1",)),
        db.Migration("001_same", ("SELECT 2",)),
    )

    with pytest.raises(ValueError, match="unique"):
        db.bootstrap(pool=pool, migrations=migrations)


def test_bootstrap_rejects_out_of_order_migration_versions():
    pool = FakePool(opened=True)
    migrations = (
        db.Migration("002_second", ("SELECT 2",)),
        db.Migration("001_first", ("SELECT 1",)),
    )

    with pytest.raises(ValueError, match="ordered"):
        db.bootstrap(pool=pool, migrations=migrations)


def test_bootstrap_rejects_unknown_applied_migrations():
    pool = FakePool(opened=True)
    pool.connection_obj.rows = [None, None, [("999_future",)]]

    with pytest.raises(RuntimeError, match="newer schema"):
        db.bootstrap(pool=pool, migrations=())


def test_bootstrap_creates_default_schema_migrations():
    pool = FakePool(opened=True)
    pool.connection_obj.rows = [None, None, []]

    db.bootstrap(pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in sql
    assert (
        "SELECT pg_advisory_xact_lock(hashtext('ticketflow:schema_migrations'))" in sql
    )
    assert "CREATE TABLE IF NOT EXISTS task_queue" in sql
    assert "idempotency_key text        NOT NULL UNIQUE" in sql
    assert "payload         jsonb       NOT NULL" in sql
    assert "CREATE INDEX IF NOT EXISTS ix_task_queue_dispatch" in sql
    assert "CREATE TABLE IF NOT EXISTS refunds" in sql
    assert "ticket_id   text             PRIMARY KEY" in sql
    assert "CREATE TABLE IF NOT EXISTS refund_attempts" in sql
    assert "CREATE TABLE IF NOT EXISTS ticket_results" in sql
    assert "ticket_id text PRIMARY KEY" in sql
    assert "data      jsonb NOT NULL" in sql
    assert "CREATE TABLE IF NOT EXISTS workflow_run" in sql
    assert "ticket_id        text        PRIMARY KEY" in sql
    assert "CREATE INDEX IF NOT EXISTS ix_workflow_run_status" in sql
    assert "CREATE TABLE IF NOT EXISTS pending_signal" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_signal_unconsumed_kind" in sql
    assert "DROP INDEX IF EXISTS ix_pending_signal_unconsumed" in sql
    assert "CREATE TABLE IF NOT EXISTS sent_replies" in sql
    assert "ticket_id      text        PRIMARY KEY" in sql
    assert "customer_email text        NOT NULL" in sql
    assert "reply_text     text        NOT NULL" in sql
    assert "CREATE TABLE IF NOT EXISTS reply_attempts" in sql
    assert "CREATE INDEX IF NOT EXISTS ix_reply_attempts_ticket" in sql
    assert pool.connection_obj.params == [
        ("000_bootstrap",),
        ("001_task_queue",),
        ("002_read_model",),
        ("003_workflow_run",),
        ("004_pending_signal",),
        ("005_pending_signal_unique_unconsumed",),
        ("006_sent_reply_guard",),
        ("007_pending_signal_drop_redundant_index",),
    ]
    # An injected pool is the caller's to manage: bootstrap must not close it.
    assert pool.closed is False


def test_bootstrap_creates_task_queue_table():
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert "CREATE TABLE IF NOT EXISTS task_queue" in sql
    assert "idempotency_key text        NOT NULL UNIQUE" in sql
    assert "CHECK (status IN ('pending', 'leased', 'done', 'failed'))" in sql
    assert "CREATE INDEX IF NOT EXISTS ix_task_queue_dispatch" in sql
    assert ("001_task_queue",) in pool.connection_obj.params


def test_bootstrap_creates_ticket_results_table():
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert "CREATE TABLE IF NOT EXISTS ticket_results" in sql
    assert "ticket_id text PRIMARY KEY" in sql
    assert "data      jsonb NOT NULL" in sql
    assert ("002_read_model",) in pool.connection_obj.params


def test_bootstrap_creates_workflow_run_table():
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert "CREATE TABLE IF NOT EXISTS workflow_run" in sql
    assert "ticket_id        text        PRIMARY KEY" in sql
    assert "CHECK (status IN ('received', 'classifying', 'drafting'," in sql
    assert "'awaiting_approval', 'resolved', 'rejected', 'escalated'))" in sql
    assert "CREATE INDEX IF NOT EXISTS ix_workflow_run_status" in sql
    assert ("003_workflow_run",) in pool.connection_obj.params


def test_bootstrap_creates_pending_signal_table():
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert "CREATE TABLE IF NOT EXISTS pending_signal" in sql
    assert "workflow_id text        NOT NULL" in sql
    assert "kind        text        NOT NULL" in sql
    assert "payload     jsonb       NOT NULL" in sql
    assert "consumed    boolean     NOT NULL DEFAULT false" in sql
    assert "CREATE INDEX IF NOT EXISTS ix_pending_signal_unconsumed" not in sql
    assert ("004_pending_signal",) in pool.connection_obj.params


def test_bootstrap_creates_unique_unconsumed_pending_signal_index():
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert "CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_signal_unconsumed_kind" in sql
    assert "ON pending_signal (workflow_id, kind)" in sql
    assert "WHERE consumed = false" in sql
    assert ("005_pending_signal_unique_unconsumed",) in pool.connection_obj.params


def test_bootstrap_drops_redundant_pending_signal_lookup_index():
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert "DROP INDEX IF EXISTS ix_pending_signal_unconsumed" in sql
    assert ("007_pending_signal_drop_redundant_index",) in pool.connection_obj.params


@pytest.mark.integration
def test_bootstrap_drops_obsolete_pending_signal_lookup_index(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_pending_signal_unconsumed
            ON pending_signal (workflow_id, kind, created_at, id)
            WHERE consumed = false
            """
        )
        conn.execute(
            "DELETE FROM schema_migrations WHERE version = %s",
            (db.PENDING_SIGNAL_DROP_REDUNDANT_INDEX_MIGRATION,),
        )
        created = conn.execute(
            "SELECT to_regclass('ix_pending_signal_unconsumed')"
        ).fetchone()
        conn.commit()

    db.bootstrap(pool=postgres_pool)

    with postgres_pool.connection() as conn:
        dropped = conn.execute(
            "SELECT to_regclass('ix_pending_signal_unconsumed')"
        ).fetchone()

    assert created == ("ix_pending_signal_unconsumed",)
    assert dropped == (None,)


def test_bootstrap_creates_sent_reply_guard_tables():
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert "CREATE TABLE IF NOT EXISTS sent_replies" in sql
    assert "ticket_id      text        PRIMARY KEY" in sql
    assert "customer_email text        NOT NULL" in sql
    assert "reply_text     text        NOT NULL" in sql
    assert "CREATE TABLE IF NOT EXISTS reply_attempts" in sql
    assert "ticket_id    text        NOT NULL" in sql
    assert "attempt      integer     NOT NULL" in sql
    assert "CREATE INDEX IF NOT EXISTS ix_reply_attempts_ticket" in sql
    assert "ON reply_attempts (ticket_id, attempted_at, id)" in sql
    assert ("006_sent_reply_guard",) in pool.connection_obj.params


def test_bootstrap_leaves_injected_pool_unopened_to_caller():
    # An injected pool is assumed already open; bootstrap must not re-open it.
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)

    assert pool.closed is False


def test_bootstrap_opens_and_closes_owned_pool(monkeypatch):
    pool = FakePool()
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.bootstrap(database_url="postgresql://example/tickets")

    assert pool.opened is True
    assert pool.closed is True


def test_dequeue_leases_due_pending_task_with_skip_locked():
    pool = FakePool(opened=True, row=LEASED_ROW)

    task = db.dequeue("q", "worker-1", pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "lease_expires_at = now() + interval '30 seconds'" in sql
    assert "attempts = attempts + 1" in sql
    assert "RETURNING id, queue_name, task_type" in sql
    assert pool.connection_obj.params[-1] == ("worker-1", "q")
    assert pool.connection_obj.commits == 1
    assert task is not None
    assert task.id == 1
    assert task.queue_name == "q"
    assert task.task_type == "classify"
    assert task.workflow_id == "workflow-1"
    assert task.payload == {"ticket_id": "workflow-1"}
    assert task.idempotency_key == "workflow-1:classify"
    assert task.status == "leased"
    assert task.attempts == 1
    assert task.max_attempts == 5
    assert task.lease_owner == "worker-1"
    assert task.lease_expires_at == datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)
    assert task.result is None
    assert task.error is None
    assert task.permanent is False


def test_timestamp_after_uses_database_now() -> None:
    conn = FakeConnection(row=(datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC),))

    timestamp = db.timestamp_after(conn, timedelta(seconds=30))

    assert timestamp == datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)
    assert "SELECT now() + %s::interval" in conn.sql[-1]
    assert conn.params[-1] == (timedelta(seconds=30),)


def test_dequeue_returns_none_when_no_task_is_available():
    pool = FakePool(opened=True)

    task = db.dequeue("q", "worker-1", pool=pool)

    assert task is None
    assert pool.connection_obj.commits == 1


def test_dequeue_leaves_injected_pool_unopened_to_caller():
    pool = FakePool(opened=True, row=LEASED_ROW)

    db.dequeue("q", "worker-1", pool=pool)

    assert pool.opened is True
    assert pool.closed is False


def test_dequeue_opens_and_closes_owned_pool(monkeypatch):
    pool = FakePool(row=LEASED_ROW)
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.dequeue("q", "worker-1", database_url="postgresql://example/tickets")

    assert pool.opened is True
    assert pool.closed is True


def test_claim_run_leases_due_unleased_run_with_skip_locked():
    pool = FakePool(opened=True, row=CLAIMED_RUN_ROW)

    run = db.claim_run("runner-1", pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "lease_expires_at = now() + interval '30 seconds'" in sql
    assert "updated_at = now()" in sql
    assert "status NOT IN ('resolved', 'rejected', 'escalated')" in sql
    assert "wakeup_at IS NOT NULL AND wakeup_at <= now()" in sql
    assert "lease_expires_at IS NULL OR lease_expires_at < now()" in sql
    assert "wakeup_at IS NULL OR wakeup_at <= now()" in sql
    assert "ORDER BY created_at" in sql
    assert "RETURNING ticket_id, status, wakeup_at" in sql
    params = pool.connection_obj.params[-1]
    assert params == ("runner-1",)
    assert pool.connection_obj.commits == 1
    assert run is not None
    assert run.ticket_id == "ticket-1"
    assert run.status == "classifying"
    assert run.wakeup_at is None
    assert run.lease_owner == "runner-1"
    assert run.lease_expires_at == datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)
    assert run.created_at == datetime(2026, 6, 16, 11, 0, tzinfo=UTC)
    assert run.updated_at == datetime(2026, 6, 16, 12, 0, tzinfo=UTC)


def test_claim_run_returns_none_when_no_run_is_claimable():
    pool = FakePool(opened=True)

    run = db.claim_run("runner-1", pool=pool)

    assert run is None
    assert pool.connection_obj.commits == 1


def test_claim_run_leaves_injected_pool_unopened_to_caller():
    pool = FakePool(opened=True, row=CLAIMED_RUN_ROW)

    db.claim_run("runner-1", pool=pool)

    assert pool.opened is True
    assert pool.closed is False


def test_claim_run_opens_and_closes_owned_pool(monkeypatch):
    pool = FakePool(row=CLAIMED_RUN_ROW)
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.claim_run("runner-1", database_url="postgresql://example/tickets")

    assert pool.opened is True
    assert pool.closed is True


def test_reclaim_expired_runs_clears_stale_run_leases():
    pool = FakePool(opened=True, row=(2,))

    reclaimed = db.reclaim_expired_runs(pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert reclaimed == 2
    assert "UPDATE workflow_run" in sql
    assert "lease_owner = NULL" in sql
    assert "lease_expires_at = NULL" in sql
    assert "updated_at = now()" in sql
    assert "lease_expires_at < now()" in sql
    assert "RETURNING ticket_id" in sql
    assert pool.connection_obj.params == []
    assert pool.connection_obj.commits == 1


def test_reclaim_expired_runs_opens_and_closes_owned_pool(monkeypatch):
    pool = FakePool(row=(1,))
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.reclaim_expired_runs(database_url="postgresql://example/tickets")

    assert pool.opened is True
    assert pool.closed is True


def test_reclaim_expired_runs_leaves_injected_pool_unopened_to_caller():
    pool = FakePool(opened=True, row=(0,))

    db.reclaim_expired_runs(pool=pool)

    assert pool.opened is True
    assert pool.closed is False


def test_save_run_persists_status_and_releases_lease():
    pool = FakePool(opened=True)
    wakeup_at = datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)

    db.save_run("ticket-1", status="awaiting_approval", wakeup_at=wakeup_at, pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert "UPDATE workflow_run" in sql
    assert "status = %s" in sql
    assert "wakeup_at = %s" in sql
    assert "lease_owner = NULL" in sql
    assert "lease_expires_at = NULL" in sql
    assert "updated_at = now()" in sql
    assert "WHERE ticket_id = %s" in sql
    params = pool.connection_obj.params[-1]
    assert params[0:2] == ("awaiting_approval", wakeup_at)
    assert params[2] == "ticket-1"
    assert pool.connection_obj.commits == 1


def test_save_run_can_mark_signal_consumed_with_lease_release():
    pool = FakePool(opened=True)

    db.save_run(
        "ticket-1",
        status="awaiting_approval",
        wakeup_at=None,
        consumed_signal_id=7,
        pool=pool,
    )

    sql = "\n".join(pool.connection_obj.sql)
    assert "UPDATE workflow_run" in sql
    assert "UPDATE pending_signal" in sql
    assert "consumed = true" in sql
    assert "WHERE id = %s" in sql
    run_params = pool.connection_obj.params[-2]
    signal_params = pool.connection_obj.params[-1]
    assert run_params[0:2] == ("awaiting_approval", None)
    assert run_params[2] == "ticket-1"
    assert signal_params == (7,)
    assert pool.connection_obj.commits == 1


def test_save_run_accepts_null_wakeup_at():
    pool = FakePool(opened=True)

    db.save_run("ticket-1", status="resolved", wakeup_at=None, pool=pool)

    params = pool.connection_obj.params[-1]
    assert params[0:2] == ("resolved", None)
    assert params[2] == "ticket-1"


def test_save_run_opens_and_closes_owned_pool(monkeypatch):
    pool = FakePool()
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.save_run("ticket-1", status="resolved", wakeup_at=None)

    assert pool.opened is True
    assert pool.closed is True


def test_save_run_leaves_injected_pool_unopened_to_caller():
    pool = FakePool(opened=True)

    db.save_run("ticket-1", status="resolved", wakeup_at=None, pool=pool)

    assert pool.opened is True
    assert pool.closed is False


def test_wake_run_pulls_wakeup_at_to_now():
    pool = FakePool(opened=True)

    db.wake_run("ticket-1", pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert "UPDATE workflow_run" in sql
    assert "wakeup_at = now()" in sql
    assert "updated_at = now()" in sql
    assert "WHERE ticket_id = %s" in sql
    params = pool.connection_obj.params[-1]
    assert params == ("ticket-1",)
    assert pool.connection_obj.commits == 1


def test_wake_run_opens_and_closes_owned_pool(monkeypatch):
    pool = FakePool()
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.wake_run("ticket-1")

    assert pool.opened is True
    assert pool.closed is True


def test_create_run_inserts_initial_workflow_run():
    pool = FakePool(opened=True)
    wakeup_at = datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)

    db.create_run("ticket-1", status="classifying", wakeup_at=wakeup_at, pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert "INSERT INTO workflow_run" in sql
    assert "ticket_id" in sql
    assert "status" in sql
    assert "wakeup_at" in sql
    assert pool.connection_obj.params[-1] == ("ticket-1", "classifying", wakeup_at)
    assert pool.connection_obj.commits == 1


def test_create_run_insert_is_idempotent():
    pool = FakePool(opened=True)

    db.create_run("ticket-1", status="classifying", wakeup_at=None, pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert "ON CONFLICT (ticket_id) DO NOTHING" in sql


def test_create_run_accepts_null_wakeup_at():
    pool = FakePool(opened=True)

    db.create_run("ticket-1", status="received", wakeup_at=None, pool=pool)

    assert pool.connection_obj.params[-1] == ("ticket-1", "received", None)


def test_create_run_opens_and_closes_owned_pool(monkeypatch):
    pool = FakePool()
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.create_run("ticket-1", status="classifying", wakeup_at=None)

    assert pool.opened is True
    assert pool.closed is True


async def test_acreate_run_inserts_initial_workflow_run():
    pool = AsyncFakePool(opened=True)
    wakeup_at = datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)

    await db.acreate_run(
        "ticket-1", status="classifying", wakeup_at=wakeup_at, pool=pool
    )

    sql = pool.connection_obj.sql[-1]
    assert "INSERT INTO workflow_run" in sql
    assert "ticket_id" in sql
    assert "status" in sql
    assert "wakeup_at" in sql
    assert pool.connection_obj.params[-1] == ("ticket-1", "classifying", wakeup_at)
    assert pool.connection_obj.commits == 1


async def test_acreate_run_insert_is_idempotent():
    pool = AsyncFakePool(opened=True)

    await db.acreate_run("ticket-1", status="classifying", wakeup_at=None, pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert "ON CONFLICT (ticket_id) DO NOTHING" in sql


def test_list_orphaned_checkpoint_threads_returns_unprojected_threads():
    pool = FakePool(opened=True)
    pool.connection_obj.rows = [[("t-orphan-1",), ("t-orphan-2",)]]

    threads = db.list_orphaned_checkpoint_threads(pool=pool)

    sql = pool.connection_obj.sql[-1]
    assert "checkpoints" in sql
    assert "workflow_run" in sql
    assert threads == ["t-orphan-1", "t-orphan-2"]


async def test_acreate_run_opens_and_closes_owned_pool(monkeypatch):
    pool = AsyncFakePool()
    monkeypatch.setattr(db, "make_async_pool", lambda database_url=None: pool)

    await db.acreate_run("ticket-1", status="classifying", wakeup_at=None)

    assert pool.opened is True
    assert pool.closed is True


@pytest.mark.integration
async def test_acreate_run_inserts_real_postgres_workflow_run(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    del postgres_pool
    async_pool = db.make_async_pool(postgres_database_url)
    await async_pool.open()
    wakeup_at = datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)
    try:
        await db.acreate_run(
            "ticket-async-1",
            status="classifying",
            wakeup_at=wakeup_at,
            pool=async_pool,
        )
        async with async_pool.connection() as conn:
            cursor = await conn.execute(
                "SELECT status, wakeup_at FROM workflow_run WHERE ticket_id = %s",
                ("ticket-async-1",),
            )
            row = await cursor.fetchone()
    finally:
        await async_pool.close()

    assert row == ("classifying", wakeup_at)


def test_list_runs_by_status_returns_ticket_ids_in_creation_order():
    pool = FakePool(opened=True)
    pool.connection_obj.rows = [[("ticket-1",), ("ticket-2",)]]

    ticket_ids = db.list_runs_by_status("awaiting_approval", pool=pool)

    sql = "\n".join(pool.connection_obj.sql)
    assert ticket_ids == ["ticket-1", "ticket-2"]
    assert "FROM workflow_run" in sql
    assert "WHERE status = %s" in sql
    assert "ORDER BY created_at, ticket_id" in sql
    assert pool.connection_obj.params[-1] == ("awaiting_approval",)


def test_add_pending_signal_if_waiting_inserts_signal_and_wakes_run():
    pool = FakePool(opened=True, row=(42,))

    signal_id = db.add_pending_signal_if_waiting(
        "ticket-1",
        "approval_decision",
        {"approved": True, "approver": "sam@example.com"},
        waiting_status="awaiting_approval",
        pool=pool,
    )

    sql = "\n".join(pool.connection_obj.sql)
    assert signal_id == 42
    assert "WITH waiting AS" in sql
    assert "status = %s" in sql
    assert "ON CONFLICT (workflow_id, kind) WHERE consumed = false DO NOTHING" in sql
    assert "UPDATE workflow_run" in sql
    assert "wakeup_at = now()" in sql
    assert "updated_at = now()" in sql
    params = pool.connection_obj.params[-1]
    assert params[0:3] == (
        "ticket-1",
        "awaiting_approval",
        "approval_decision",
    )
    assert len(params) == 4
    assert pool.connection_obj.commits == 1


def test_add_pending_signal_if_waiting_returns_none_when_not_waiting():
    pool = FakePool(opened=True, row=None)

    signal_id = db.add_pending_signal_if_waiting(
        "ticket-1",
        "approval_decision",
        {"approved": True, "approver": "sam@example.com"},
        waiting_status="awaiting_approval",
        pool=pool,
    )

    assert signal_id is None
    assert pool.connection_obj.commits == 1


@pytest.mark.integration
def test_save_run_round_trips_status_and_lease_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run "
            "(ticket_id, status, lease_owner, lease_expires_at) "
            "VALUES ('saved', 'classifying', 'runner-1', now())"
        )
        conn.commit()

    wakeup_at = datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)
    db.save_run(
        "saved", status="awaiting_approval", wakeup_at=wakeup_at, pool=postgres_pool
    )

    with postgres_pool.connection() as conn:
        row = conn.execute(
            "SELECT status, wakeup_at, lease_owner, lease_expires_at "
            "FROM workflow_run WHERE ticket_id = 'saved'"
        ).fetchone()
    assert row is not None
    assert row[0] == "awaiting_approval"
    assert row[1] == wakeup_at
    assert row[2] is None
    assert row[3] is None


@pytest.mark.integration
def test_wake_run_makes_a_future_run_claimable_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
            "VALUES ('asleep', 'classifying', now() + interval '1 hour')"
        )
        conn.commit()

    # Not yet due, so it cannot be claimed.
    assert db.claim_run("runner-1", pool=postgres_pool) is None

    db.wake_run("asleep", pool=postgres_pool)

    claimed = db.claim_run("runner-1", pool=postgres_pool)
    assert claimed is not None
    assert claimed.ticket_id == "asleep"


@pytest.mark.integration
def test_bootstrap_is_idempotent_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    db.bootstrap(pool=postgres_pool)

    with postgres_pool.connection() as conn:
        row = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE version = %s",
            (db.BOOTSTRAP_MIGRATION,),
        ).fetchone()

    assert row is not None
    assert row[0] == 1


@pytest.mark.integration
def test_bootstrap_creates_task_queue_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    db.bootstrap(pool=postgres_pool)
    expected_columns = {
        "id",
        "queue_name",
        "task_type",
        "workflow_id",
        "payload",
        "idempotency_key",
        "status",
        "attempts",
        "max_attempts",
        "available_at",
        "enqueued_at",
        "lease_owner",
        "lease_expires_at",
        "result",
        "error",
        "permanent",
    }

    with postgres_pool.connection() as conn:
        marker = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE version = %s",
            (db.TASK_QUEUE_MIGRATION,),
        ).fetchone()
        columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'task_queue'"
            ).fetchall()
        }

        # The UNIQUE constraint on idempotency_key holds.
        insert_dup = (
            "INSERT INTO task_queue (queue_name, task_type, workflow_id, "
            "idempotency_key) VALUES ('q', 't', 'w', 'dup-key')"
        )
        conn.execute(insert_dup)
        duplicate_rejected = False
        try:
            conn.execute(insert_dup)
        except Exception:
            duplicate_rejected = True
        conn.rollback()

    assert marker is not None
    assert marker[0] == 1
    assert expected_columns <= columns
    assert duplicate_rejected is True


@pytest.mark.integration
def test_bootstrap_creates_ticket_results_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    db.bootstrap(pool=postgres_pool)

    with postgres_pool.connection() as conn:
        marker = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE version = %s",
            (db.READ_MODEL_MIGRATION,),
        ).fetchone()
        columns = {
            (row[0], row[1])
            for row in conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'ticket_results'"
            ).fetchall()
        }

    assert marker is not None
    assert marker[0] == 1
    assert ("ticket_id", "text") in columns
    assert ("data", "jsonb") in columns


@pytest.mark.integration
def test_dequeue_leases_one_due_pending_task_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            """
            INSERT INTO task_queue (
                queue_name, task_type, workflow_id, payload, idempotency_key
            )
            VALUES (
                'q', 'classify', 'workflow-1', '{"ticket_id": "workflow-1"}',
                'workflow-1:classify'
            )
            """
        )
        conn.commit()

    task = db.dequeue("q", "worker-1", pool=postgres_pool)
    second_task = db.dequeue("q", "worker-2", pool=postgres_pool)

    assert task is not None
    assert task.status == "leased"
    assert task.lease_owner == "worker-1"
    assert task.attempts == 1
    assert task.lease_expires_at is not None
    assert task.lease_expires_at > datetime.now(UTC) - timedelta(seconds=5)
    assert second_task is None


@pytest.mark.integration
def test_bootstrap_creates_workflow_run_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    db.bootstrap(pool=postgres_pool)
    expected_columns = {
        "ticket_id",
        "status",
        "wakeup_at",
        "lease_owner",
        "lease_expires_at",
        "created_at",
        "updated_at",
    }

    with postgres_pool.connection() as conn:
        marker = conn.execute(
            "SELECT count(*) FROM schema_migrations WHERE version = %s",
            (db.WORKFLOW_RUN_MIGRATION,),
        ).fetchone()
        columns = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'workflow_run'"
            ).fetchall()
        }
        index = conn.execute(
            "SELECT count(*) FROM pg_indexes WHERE indexname = 'ix_workflow_run_status'"
        ).fetchone()

        # The ticket_id primary key rejects a duplicate run.
        conn.execute("INSERT INTO workflow_run (ticket_id) VALUES ('dup')")
        duplicate_rejected = False
        try:
            conn.execute("INSERT INTO workflow_run (ticket_id) VALUES ('dup')")
        except Exception:
            duplicate_rejected = True
        conn.rollback()

    assert marker is not None
    assert marker[0] == 1
    assert expected_columns <= columns
    assert index is not None and index[0] == 1
    assert duplicate_rejected is True


@pytest.mark.integration
def test_bootstrap_applies_later_alter_migration_against_real_postgres(
    postgres_database_url: str,
):
    first_migration = db.Migration(
        "001_create_probe",
        ("CREATE TABLE migration_probe (id integer PRIMARY KEY)",),
    )
    second_migration = db.Migration(
        "002_alter_probe",
        ("ALTER TABLE migration_probe ADD COLUMN name text NOT NULL DEFAULT 'unset'",),
    )

    db.bootstrap(database_url=postgres_database_url, migrations=(first_migration,))

    pool = db.make_pool(postgres_database_url)
    pool.open()
    try:
        with pool.connection() as conn:
            conn.execute("INSERT INTO migration_probe (id) VALUES (1)")
            conn.commit()
    finally:
        pool.close()

    db.bootstrap(
        database_url=postgres_database_url,
        migrations=(first_migration, second_migration),
    )

    pool = db.make_pool(postgres_database_url)
    pool.open()
    try:
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT id, name FROM migration_probe WHERE id = 1"
            ).fetchone()
            markers = conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
    finally:
        pool.close()

    assert row == (1, "unset")
    assert markers == [("001_create_probe",), ("002_alter_probe",)]


@pytest.mark.integration
def test_bootstrap_does_not_mark_failed_migration_against_real_postgres(
    postgres_database_url: str,
):
    migrations = (
        db.Migration(
            "001_create_probe",
            ("CREATE TABLE migration_probe (id integer PRIMARY KEY)",),
        ),
        db.Migration(
            "002_duplicate_probe",
            ("CREATE TABLE migration_probe (id integer PRIMARY KEY)",),
        ),
    )

    with pytest.raises(Exception):
        db.bootstrap(database_url=postgres_database_url, migrations=migrations)

    pool = db.make_pool(postgres_database_url)
    pool.open()
    try:
        with pool.connection() as conn:
            schema_table = conn.execute(
                "SELECT to_regclass('schema_migrations')"
            ).fetchone()
            probe_table = conn.execute(
                "SELECT to_regclass('migration_probe')"
            ).fetchone()
    finally:
        pool.close()

    assert schema_table == (None,)
    assert probe_table == (None,)


@pytest.mark.integration
def test_list_runs_by_status_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, created_at) "
            "VALUES ('older', 'awaiting_approval', now() - interval '10 seconds')"
        )
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, created_at) "
            "VALUES ('newer', 'awaiting_approval', now())"
        )
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, created_at) "
            "VALUES ('other', 'classifying', now() - interval '20 seconds')"
        )
        conn.commit()

    assert db.list_runs_by_status("awaiting_approval", pool=postgres_pool) == [
        "older",
        "newer",
    ]


@pytest.mark.integration
def test_add_pending_signal_if_waiting_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
            "VALUES ('waiting', 'awaiting_approval', now() + interval '1 hour')"
        )
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status) "
            "VALUES ('busy', 'classifying')"
        )
        conn.commit()

    accepted = db.add_pending_signal_if_waiting(
        "waiting",
        "approval_decision",
        {"approved": True, "approver": "sam@example.com"},
        waiting_status="awaiting_approval",
        pool=postgres_pool,
    )
    duplicate = db.add_pending_signal_if_waiting(
        "waiting",
        "approval_decision",
        {"approved": False, "approver": "sam@example.com"},
        waiting_status="awaiting_approval",
        pool=postgres_pool,
    )
    wrong_status = db.add_pending_signal_if_waiting(
        "busy",
        "approval_decision",
        {"approved": True, "approver": "sam@example.com"},
        waiting_status="awaiting_approval",
        pool=postgres_pool,
    )
    missing = db.add_pending_signal_if_waiting(
        "missing",
        "approval_decision",
        {"approved": True, "approver": "sam@example.com"},
        waiting_status="awaiting_approval",
        pool=postgres_pool,
    )

    with postgres_pool.connection() as conn:
        row = conn.execute(
            """
            SELECT payload, consumed
            FROM pending_signal
            WHERE workflow_id = 'waiting'
            """
        ).fetchone()
        run = conn.execute(
            "SELECT wakeup_at <= now() FROM workflow_run WHERE ticket_id = 'waiting'"
        ).fetchone()

    assert accepted is not None
    assert duplicate is None
    assert wrong_status is None
    assert missing is None
    assert row is not None
    assert row[0] == {"approved": True, "approver": "sam@example.com"}
    assert row[1] is False
    assert run is not None
    assert run[0] is True


@pytest.mark.integration
def test_claim_run_leases_runnable_runs_oldest_first_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        # Two runnable runs (older first), one with a future timer, and one
        # already held under a live lease -- only the runnable two qualify.
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, created_at) "
            "VALUES ('older', now() - interval '10 seconds')"
        )
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, created_at) VALUES ('newer', now())"
        )
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, wakeup_at) "
            "VALUES ('future-timer', now() + interval '1 hour')"
        )
        conn.execute(
            "INSERT INTO workflow_run "
            "(ticket_id, lease_owner, lease_expires_at) "
            "VALUES ('held', 'runner-9', now() + interval '30 seconds')"
        )
        conn.commit()

    first = db.claim_run("runner-1", pool=postgres_pool)
    second = db.claim_run("runner-2", pool=postgres_pool)
    third = db.claim_run("runner-3", pool=postgres_pool)

    assert first is not None and first.ticket_id == "older"
    assert first.lease_owner == "runner-1"
    assert first.lease_expires_at is not None
    assert second is not None and second.ticket_id == "newer"
    assert second.lease_owner == "runner-2"
    # The future-timer and live-leased runs are not claimable.
    assert third is None


@pytest.mark.integration
def test_claim_run_reclaims_expired_lease_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run "
            "(ticket_id, lease_owner, lease_expires_at) "
            "VALUES ('stale', 'runner-dead', now() - interval '1 second')"
        )
        conn.commit()

    claimed = db.claim_run("runner-1", pool=postgres_pool)

    assert claimed is not None and claimed.ticket_id == "stale"
    assert claimed.lease_owner == "runner-1"


@pytest.mark.integration
def test_claim_run_uses_database_clock_for_wakeup_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
            "VALUES ('clocked', 'classifying', now() + interval '1 hour')",
        )
        conn.commit()

    assert db.claim_run("runner-1", pool=postgres_pool) is None

    with postgres_pool.connection() as conn:
        conn.execute(
            "UPDATE workflow_run SET wakeup_at = now() WHERE ticket_id = %s",
            ("clocked",),
        )
        conn.commit()

    claimed = db.claim_run("runner-1", pool=postgres_pool)

    assert claimed is not None
    assert claimed.ticket_id == "clocked"
    assert claimed.lease_owner == "runner-1"


@pytest.mark.integration
def test_claim_run_leases_woken_terminal_run_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
            "VALUES ('terminal-woken', 'escalated', now())"
        )
        conn.execute(
            "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
            "VALUES ('terminal-quiet', 'resolved', NULL)"
        )
        conn.commit()

    claimed = db.claim_run("runner-1", pool=postgres_pool)
    second = db.claim_run("runner-2", pool=postgres_pool)

    assert claimed is not None
    assert claimed.ticket_id == "terminal-woken"
    assert claimed.status == "escalated"
    assert second is None


@pytest.mark.integration
def test_reclaim_expired_runs_clears_only_expired_leases_against_real_postgres(
    postgres_pool: db.ConnectionPool,
):
    with postgres_pool.connection() as conn:
        conn.execute(
            "INSERT INTO workflow_run "
            "(ticket_id, status, wakeup_at, lease_owner, lease_expires_at) "
            "VALUES ("
            "'stale', 'classifying', now() + interval '10 minutes', "
            "'runner-dead', now() - interval '1 second'"
            ")"
        )
        conn.execute(
            "INSERT INTO workflow_run "
            "(ticket_id, lease_owner, lease_expires_at) "
            "VALUES ('live', 'runner-live', now() + interval '30 seconds')"
        )
        conn.commit()

    reclaimed = db.reclaim_expired_runs(pool=postgres_pool)

    with postgres_pool.connection() as conn:
        stale = conn.execute(
            "SELECT status, wakeup_at IS NOT NULL, lease_owner, "
            "lease_expires_at FROM workflow_run WHERE ticket_id = 'stale'"
        ).fetchone()
        live = conn.execute(
            "SELECT lease_owner, lease_expires_at IS NOT NULL "
            "FROM workflow_run WHERE ticket_id = 'live'"
        ).fetchone()

    assert reclaimed == 1
    assert stale == ("classifying", True, None, None)
    assert live == ("runner-live", True)
