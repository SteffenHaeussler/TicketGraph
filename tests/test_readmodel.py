"""Tests for the read model."""

import sqlite3

import pytest

from ticketflow import db, readmodel
from ticketflow.models import TicketResult, TicketStatus


class FakeCursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row


class FakeConnection:
    def __init__(self, rows: list[tuple[object, ...] | None]) -> None:
        self.rows = rows
        self.sql: list[str] = []
        self.params: list[tuple[object, ...]] = []
        self.commits = 0

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> FakeCursor:
        self.sql.append(sql)
        if params is not None:
            self.params.append(params)
        return FakeCursor(self.rows.pop(0))

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
    def __init__(self, rows: list[tuple[object, ...] | None]) -> None:
        self.connection_obj = FakeConnection(rows)
        self.opened = False
        self.closed = False

    def open(self) -> None:
        self.opened = True

    def connection(self, timeout: float | None = None) -> FakeConnectionContext:
        del timeout
        return FakeConnectionContext(self.connection_obj)

    def close(self) -> None:
        self.closed = True


def make_result(ticket_id: str = "t-1", **overrides: object) -> TicketResult:
    defaults: dict[str, object] = {
        "ticket_id": ticket_id,
        "status": TicketStatus.RESOLVED,
        "reply_text": "All done.",
        "refund_executed": False,
    }
    defaults.update(overrides)
    return TicketResult.model_validate(defaults)


def test_save_result_upserts_jsonb_and_commits() -> None:
    result = make_result(refund_executed=True)
    pool = FakePool(rows=[None])

    readmodel.save_result(result, pool=pool)

    sql = pool.connection_obj.sql[0]
    assert "INSERT INTO ticket_results" in sql
    assert "ON CONFLICT (ticket_id) DO UPDATE" in sql
    assert "data = EXCLUDED.data" in sql
    assert pool.connection_obj.params[0][0] == "t-1"
    assert pool.connection_obj.commits == 1
    assert pool.closed is False


def test_save_result_opens_and_closes_owned_pool(monkeypatch) -> None:
    pool = FakePool(rows=[None])
    monkeypatch.setattr(readmodel.db, "make_pool", lambda database_url=None: pool)

    readmodel.save_result(make_result(), database_url="postgresql://example/tickets")

    assert pool.opened is True
    assert pool.closed is True


def test_load_result_validates_jsonb_payload() -> None:
    result = make_result(refund_executed=True)
    pool = FakePool(rows=[(result.model_dump(mode="json"),)])

    loaded = readmodel.load_result("t-1", pool=pool)

    assert loaded == result
    assert (
        "SELECT data FROM ticket_results WHERE ticket_id = %s"
        in (pool.connection_obj.sql[0])
    )
    assert pool.connection_obj.params[0] == ("t-1",)


def test_load_result_returns_none_when_ticket_is_missing() -> None:
    pool = FakePool(rows=[None])

    assert readmodel.load_result("missing", pool=pool) is None


def test_clear_removes_ticket_results_and_reports_count() -> None:
    pool = FakePool(rows=[(2,)])

    deleted = readmodel.clear(pool=pool)

    assert deleted == 2
    assert "DELETE FROM ticket_results" in pool.connection_obj.sql[0]
    assert "SELECT count(*) FROM deleted" in pool.connection_obj.sql[0]
    assert pool.connection_obj.commits == 1


def test_record_refund_first_call_executes(tmp_path):
    db = str(tmp_path / "read.db")
    assert readmodel.record_refund("t-1", 42.0, attempt=1, db_path=db) is True


def test_record_refund_duplicate_ticket_is_noop(tmp_path):
    db = str(tmp_path / "read.db")
    readmodel.record_refund("t-1", 42.0, attempt=1, db_path=db)
    assert readmodel.record_refund("t-1", 42.0, attempt=2, db_path=db) is False


def test_record_refund_logs_every_attempt_but_refunds_once(tmp_path):
    db = str(tmp_path / "read.db")
    readmodel.record_refund("t-1", 42.0, attempt=1, db_path=db)
    readmodel.record_refund("t-1", 42.0, attempt=2, db_path=db)
    conn = sqlite3.connect(db)
    try:
        attempts = conn.execute(
            "SELECT attempt FROM refund_attempts WHERE ticket_id = 't-1'"
        ).fetchall()
        refunds = conn.execute(
            "SELECT COUNT(*) FROM refunds WHERE ticket_id = 't-1'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert attempts == [(1,), (2,)]
    assert refunds == 1


def test_record_refund_different_tickets_both_execute(tmp_path):
    db = str(tmp_path / "read.db")
    assert readmodel.record_refund("t-1", 42.0, attempt=1, db_path=db) is True
    assert readmodel.record_refund("t-2", 13.0, attempt=1, db_path=db) is True


@pytest.mark.integration
def test_save_and_load_roundtrip_against_real_postgres() -> None:
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    try:
        readmodel.clear(pool=pool)

        result = make_result(refund_executed=True)
        readmodel.save_result(result, pool=pool)

        assert readmodel.load_result("t-1", pool=pool) == result
    finally:
        pool.close()


@pytest.mark.integration
def test_save_result_overwrites_existing_result_against_real_postgres() -> None:
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    try:
        readmodel.clear(pool=pool)

        readmodel.save_result(make_result(reply_text="first"), pool=pool)
        readmodel.save_result(make_result(reply_text="second"), pool=pool)

        loaded = readmodel.load_result("t-1", pool=pool)
    finally:
        pool.close()

    assert loaded is not None
    assert loaded.reply_text == "second"


@pytest.mark.integration
def test_clear_removes_only_ticket_results_against_real_postgres() -> None:
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    try:
        with pool.connection() as conn:
            conn.execute("DELETE FROM refunds")
            conn.execute("DELETE FROM refund_attempts")
            conn.execute(
                "INSERT INTO refunds (ticket_id, amount) VALUES (%s, %s)",
                ("refund-only", 10.0),
            )
            conn.commit()

        readmodel.save_result(make_result("a"), pool=pool)
        readmodel.save_result(make_result("b"), pool=pool)

        deleted = readmodel.clear(pool=pool)

        with pool.connection() as conn:
            refund_count = conn.execute("SELECT count(*) FROM refunds").fetchone()
    finally:
        pool.close()

    assert deleted == 2
    assert refund_count == (1,)
