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
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row


class FakeConnection:
    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self.sql: list[str] = []
        self.params: list[tuple[str, ...]] = []
        self.commits = 0
        self.row = row
        self.rows: list[tuple[object, ...] | None] = []

    def execute(self, sql: str, params: tuple[str, ...] | None = None) -> FakeCursor:
        self.sql.append(sql)
        if params is not None:
            self.params.append(params)
        if self.rows:
            return FakeCursor(self.rows.pop(0))
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


def test_make_pool_uses_database_url_from_config(monkeypatch):
    calls: list[dict[str, object]] = []

    class RecordingPool:
        def __init__(self, **kwargs: object) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(db, "ConnectionPool", RecordingPool)
    monkeypatch.setattr(db.config, "DATABASE_URL", "postgresql://example/tickets")

    assert isinstance(db.make_pool(), RecordingPool)

    assert calls == [
        {
            "conninfo": "postgresql://example/tickets",
            "min_size": 1,
            "max_size": 10,
            "open": False,
        }
    ]


def test_bootstrap_creates_idempotent_migration_marker():
    pool = FakePool(opened=True)

    db.bootstrap(pool=pool)
    db.bootstrap(pool=pool)

    # Each call issues 15 statements: schema_migrations create + 000 marker,
    # task_queue create, dispatch index, 001 marker, refunds create,
    # refund_attempts create, ticket_results create, 002 marker,
    # workflow_run create, status index, 003 marker, pending_signal create,
    # signal lookup index, 004 marker.
    assert pool.connection_obj.commits == 2
    assert len(pool.connection_obj.sql) == 30
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in pool.connection_obj.sql[0]
    assert "ON CONFLICT (version) DO NOTHING" in pool.connection_obj.sql[1]
    assert "CREATE TABLE IF NOT EXISTS task_queue" in pool.connection_obj.sql[2]
    assert "idempotency_key text        NOT NULL UNIQUE" in pool.connection_obj.sql[2]
    assert "payload         jsonb       NOT NULL" in pool.connection_obj.sql[2]
    assert (
        "CREATE INDEX IF NOT EXISTS ix_task_queue_dispatch"
        in pool.connection_obj.sql[3]
    )
    assert "CREATE TABLE IF NOT EXISTS refunds" in pool.connection_obj.sql[5]
    assert "ticket_id   text             PRIMARY KEY" in pool.connection_obj.sql[5]
    assert "CREATE TABLE IF NOT EXISTS refund_attempts" in pool.connection_obj.sql[6]
    assert "CREATE TABLE IF NOT EXISTS ticket_results" in pool.connection_obj.sql[7]
    assert "ticket_id text PRIMARY KEY" in pool.connection_obj.sql[7]
    assert "data      jsonb NOT NULL" in pool.connection_obj.sql[7]
    assert "CREATE TABLE IF NOT EXISTS workflow_run" in pool.connection_obj.sql[9]
    assert "ticket_id        text        PRIMARY KEY" in pool.connection_obj.sql[9]
    assert (
        "CREATE INDEX IF NOT EXISTS ix_workflow_run_status"
        in pool.connection_obj.sql[10]
    )
    assert "CREATE TABLE IF NOT EXISTS pending_signal" in pool.connection_obj.sql[12]
    assert (
        "CREATE INDEX IF NOT EXISTS ix_pending_signal_unconsumed"
        in pool.connection_obj.sql[13]
    )
    assert pool.connection_obj.params == [
        ("000_bootstrap",),
        ("001_task_queue",),
        ("002_read_model",),
        ("003_workflow_run",),
        ("004_pending_signal",),
        ("000_bootstrap",),
        ("001_task_queue",),
        ("002_read_model",),
        ("003_workflow_run",),
        ("004_pending_signal",),
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
    assert "CREATE INDEX IF NOT EXISTS ix_pending_signal_unconsumed" in sql
    assert "WHERE consumed = false" in sql
    assert ("004_pending_signal",) in pool.connection_obj.params


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
    assert "status NOT IN ('resolved', 'rejected', 'escalated')" in sql
    assert "lease_expires_at IS NULL OR lease_expires_at < now()" in sql
    assert "wakeup_at IS NULL OR wakeup_at <= now()" in sql
    assert "ORDER BY created_at" in sql
    assert "RETURNING ticket_id, status, wakeup_at" in sql
    assert pool.connection_obj.params[-1] == ("runner-1",)
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
    assert "lease_expires_at < now()" in sql
    assert "RETURNING ticket_id" in sql
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
    assert pool.connection_obj.params[-1] == (
        "awaiting_approval",
        wakeup_at,
        "ticket-1",
    )
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
    assert pool.connection_obj.params[-2] == ("awaiting_approval", None, "ticket-1")
    assert pool.connection_obj.params[-1] == (7,)
    assert pool.connection_obj.commits == 1


def test_save_run_accepts_null_wakeup_at():
    pool = FakePool(opened=True)

    db.save_run("ticket-1", status="resolved", wakeup_at=None, pool=pool)

    assert pool.connection_obj.params[-1] == ("resolved", None, "ticket-1")


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
    assert pool.connection_obj.params[-1] == ("ticket-1",)
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


def test_add_pending_signal_inserts_signal_and_wakes_run():
    pool = FakePool(opened=True, row=(42,))

    signal_id = db.add_pending_signal(
        "ticket-1",
        "approval_decision",
        {"approved": True, "approver": "sam@example.com"},
        pool=pool,
    )

    sql = "\n".join(pool.connection_obj.sql)
    assert signal_id == 42
    assert "INSERT INTO pending_signal" in sql
    assert "UPDATE workflow_run" in sql
    assert "wakeup_at = now()" in sql
    assert pool.connection_obj.params[0][0:2] == ("ticket-1", "approval_decision")
    assert pool.connection_obj.params[1] == ("ticket-1",)
    assert pool.connection_obj.commits == 1


def test_add_pending_signal_opens_and_closes_owned_pool(monkeypatch):
    pool = FakePool(row=(42,))
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.add_pending_signal("ticket-1", "approval_decision", {"approved": True})

    assert pool.opened is True
    assert pool.closed is True


@pytest.mark.integration
def test_save_run_round_trips_status_and_lease_against_real_postgres():
    pool = _open_clean_run_pool()
    try:
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO workflow_run "
                "(ticket_id, status, lease_owner, lease_expires_at) "
                "VALUES ('saved', 'classifying', 'runner-1', now())"
            )
            conn.commit()

        wakeup_at = datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)
        db.save_run("saved", status="awaiting_approval", wakeup_at=wakeup_at, pool=pool)

        with pool.connection() as conn:
            row = conn.execute(
                "SELECT status, wakeup_at, lease_owner, lease_expires_at "
                "FROM workflow_run WHERE ticket_id = 'saved'"
            ).fetchone()
        assert row is not None
        assert row[0] == "awaiting_approval"
        assert row[1] == wakeup_at
        assert row[2] is None
        assert row[3] is None
    finally:
        pool.close()


@pytest.mark.integration
def test_wake_run_makes_a_future_run_claimable_against_real_postgres():
    pool = _open_clean_run_pool()
    try:
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO workflow_run (ticket_id, status, wakeup_at) "
                "VALUES ('asleep', 'classifying', now() + interval '1 hour')"
            )
            conn.commit()

        # Not yet due, so it cannot be claimed.
        assert db.claim_run("runner-1", pool=pool) is None

        db.wake_run("asleep", pool=pool)

        claimed = db.claim_run("runner-1", pool=pool)
        assert claimed is not None
        assert claimed.ticket_id == "asleep"
    finally:
        pool.close()


@pytest.mark.integration
def test_bootstrap_is_idempotent_against_real_postgres():
    db.bootstrap()
    db.bootstrap()

    pool = db.make_pool()
    pool.open()
    try:
        with pool.connection() as conn:
            row = conn.execute(
                "SELECT count(*) FROM schema_migrations WHERE version = %s",
                (db.BOOTSTRAP_MIGRATION,),
            ).fetchone()
    finally:
        pool.close()

    assert row is not None
    assert row[0] == 1


@pytest.mark.integration
def test_bootstrap_creates_task_queue_against_real_postgres():
    db.bootstrap()
    db.bootstrap()

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

    pool = db.make_pool()
    pool.open()
    try:
        with pool.connection() as conn:
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
    finally:
        pool.close()

    assert marker is not None
    assert marker[0] == 1
    assert expected_columns <= columns
    assert duplicate_rejected is True


@pytest.mark.integration
def test_bootstrap_creates_ticket_results_against_real_postgres():
    db.bootstrap()
    db.bootstrap()

    pool = db.make_pool()
    pool.open()
    try:
        with pool.connection() as conn:
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
    finally:
        pool.close()

    assert marker is not None
    assert marker[0] == 1
    assert ("ticket_id", "text") in columns
    assert ("data", "jsonb") in columns


@pytest.mark.integration
def test_dequeue_leases_one_due_pending_task_against_real_postgres():
    db.bootstrap()

    pool = db.make_pool()
    pool.open()
    try:
        with pool.connection() as conn:
            conn.execute("DELETE FROM task_queue")
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

        task = db.dequeue("q", "worker-1", pool=pool)
        second_task = db.dequeue("q", "worker-2", pool=pool)

        assert task is not None
        assert task.status == "leased"
        assert task.lease_owner == "worker-1"
        assert task.attempts == 1
        assert task.lease_expires_at is not None
        assert task.lease_expires_at > datetime.now(UTC) - timedelta(seconds=5)
        assert second_task is None
    finally:
        pool.close()


@pytest.mark.integration
def test_bootstrap_creates_workflow_run_against_real_postgres():
    db.bootstrap()
    db.bootstrap()

    expected_columns = {
        "ticket_id",
        "status",
        "wakeup_at",
        "lease_owner",
        "lease_expires_at",
        "created_at",
        "updated_at",
    }

    pool = db.make_pool()
    pool.open()
    try:
        with pool.connection() as conn:
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
                "SELECT count(*) FROM pg_indexes "
                "WHERE indexname = 'ix_workflow_run_status'"
            ).fetchone()

            # The ticket_id primary key rejects a duplicate run.
            conn.execute("DELETE FROM workflow_run")
            conn.execute("INSERT INTO workflow_run (ticket_id) VALUES ('dup')")
            duplicate_rejected = False
            try:
                conn.execute("INSERT INTO workflow_run (ticket_id) VALUES ('dup')")
            except Exception:
                duplicate_rejected = True
            conn.rollback()
    finally:
        pool.close()

    assert marker is not None
    assert marker[0] == 1
    assert expected_columns <= columns
    assert index is not None and index[0] == 1
    assert duplicate_rejected is True


def _open_clean_run_pool() -> db.ConnectionPool:
    """Bootstrap, open a pool, and truncate ``workflow_run`` for isolation."""
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    with pool.connection() as conn:
        conn.execute("DELETE FROM workflow_run")
        conn.commit()
    return pool


@pytest.mark.integration
def test_claim_run_leases_runnable_runs_oldest_first_against_real_postgres():
    pool = _open_clean_run_pool()
    try:
        with pool.connection() as conn:
            # Two runnable runs (older first), one with a future timer, and one
            # already held under a live lease -- only the runnable two qualify.
            conn.execute(
                "INSERT INTO workflow_run (ticket_id, created_at) "
                "VALUES ('older', now() - interval '10 seconds')"
            )
            conn.execute(
                "INSERT INTO workflow_run (ticket_id, created_at) "
                "VALUES ('newer', now())"
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

        first = db.claim_run("runner-1", pool=pool)
        second = db.claim_run("runner-2", pool=pool)
        third = db.claim_run("runner-3", pool=pool)
    finally:
        pool.close()

    assert first is not None and first.ticket_id == "older"
    assert first.lease_owner == "runner-1"
    assert first.lease_expires_at is not None
    assert second is not None and second.ticket_id == "newer"
    assert second.lease_owner == "runner-2"
    # The future-timer and live-leased runs are not claimable.
    assert third is None


@pytest.mark.integration
def test_claim_run_reclaims_expired_lease_against_real_postgres():
    pool = _open_clean_run_pool()
    try:
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO workflow_run "
                "(ticket_id, lease_owner, lease_expires_at) "
                "VALUES ('stale', 'runner-dead', now() - interval '1 second')"
            )
            conn.commit()

        claimed = db.claim_run("runner-1", pool=pool)
    finally:
        pool.close()

    assert claimed is not None and claimed.ticket_id == "stale"
    assert claimed.lease_owner == "runner-1"


@pytest.mark.integration
def test_reclaim_expired_runs_clears_only_expired_leases_against_real_postgres():
    pool = _open_clean_run_pool()
    try:
        with pool.connection() as conn:
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

        reclaimed = db.reclaim_expired_runs(pool=pool)

        with pool.connection() as conn:
            stale = conn.execute(
                "SELECT status, wakeup_at IS NOT NULL, lease_owner, "
                "lease_expires_at FROM workflow_run WHERE ticket_id = 'stale'"
            ).fetchone()
            live = conn.execute(
                "SELECT lease_owner, lease_expires_at IS NOT NULL "
                "FROM workflow_run WHERE ticket_id = 'live'"
            ).fetchone()
    finally:
        pool.close()

    assert reclaimed == 1
    assert stale == ("classifying", True, None, None)
    assert live == ("runner-live", True)
