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
    def __init__(self) -> None:
        self.connection_obj = FakeConnection()
        self.closed = False

    def connection(self) -> FakeConnectionContext:
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
    pool = FakePool()

    db.bootstrap(pool=pool)
    db.bootstrap(pool=pool)

    assert pool.connection_obj.commits == 2
    assert len(pool.connection_obj.sql) == 4
    assert "CREATE TABLE IF NOT EXISTS schema_migrations" in pool.connection_obj.sql[0]
    assert "ON CONFLICT (version) DO NOTHING" in pool.connection_obj.sql[1]
    assert pool.connection_obj.params == [("000_bootstrap",), ("000_bootstrap",)]
    assert pool.closed is False


def test_bootstrap_closes_owned_pool(monkeypatch):
    pool = FakePool()
    monkeypatch.setattr(db, "make_pool", lambda database_url=None: pool)

    db.bootstrap(database_url="postgresql://example/tickets")

    assert pool.closed is True
