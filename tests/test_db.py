import pytest

from ticketflow import db


class FakeConnection:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.params: list[tuple[str, ...]] = []
        self.commits = 0

    def execute(self, sql: str, params: tuple[str, ...] | None = None) -> None:
        self.sql.append(sql)
        if params is not None:
            self.params.append(params)

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
    def __init__(self, *, opened: bool = False) -> None:
        self.connection_obj = FakeConnection()
        self.opened = opened
        self.closed = False

    def open(self) -> None:
        self.opened = True

    def connection(self) -> FakeConnectionContext:
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

    # Each call issues 5 statements: schema_migrations create + 000 marker,
    # task_queue create, dispatch index, 001 marker.
    assert pool.connection_obj.commits == 2
    assert len(pool.connection_obj.sql) == 10
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in pool.connection_obj.sql[0]
    assert "ON CONFLICT (version) DO NOTHING" in pool.connection_obj.sql[1]
    assert pool.connection_obj.params == [
        ("000_bootstrap",),
        ("001_task_queue",),
        ("000_bootstrap",),
        ("001_task_queue",),
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
