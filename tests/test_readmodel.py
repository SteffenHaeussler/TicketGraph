"""Tests for the SQLite read model."""

import sqlite3

from ticketflow import config, readmodel
from ticketflow.models import TicketResult, TicketStatus


def make_result(ticket_id: str = "t-1", **overrides: object) -> TicketResult:
    defaults: dict[str, object] = {
        "ticket_id": ticket_id,
        "status": TicketStatus.RESOLVED,
        "reply_text": "All done.",
        "refund_executed": False,
    }
    defaults.update(overrides)
    return TicketResult.model_validate(defaults)


def test_save_and_load_roundtrip(tmp_path):
    db = str(tmp_path / "read.db")
    result = make_result(refund_executed=True)
    readmodel.save_result(result, db)
    assert readmodel.load_result("t-1", db) == result


def test_save_overwrites_existing_result(tmp_path):
    db = str(tmp_path / "read.db")
    readmodel.save_result(make_result(reply_text="first"), db)
    readmodel.save_result(make_result(reply_text="second"), db)
    loaded = readmodel.load_result("t-1", db)
    assert loaded is not None
    assert loaded.reply_text == "second"


def test_load_missing_ticket_returns_none(tmp_path):
    db = str(tmp_path / "read.db")
    readmodel.save_result(make_result(), db)
    assert readmodel.load_result("other", db) is None


def test_load_without_db_file_returns_none(tmp_path):
    assert readmodel.load_result("t-1", str(tmp_path / "missing.db")) is None


def test_clear_removes_all_rows(tmp_path):
    db = str(tmp_path / "read.db")
    readmodel.save_result(make_result("a"), db)
    readmodel.save_result(make_result("b"), db)
    assert readmodel.clear(db) == 2
    assert readmodel.load_result("a", db) is None


def test_clear_without_db_file_returns_zero(tmp_path):
    assert readmodel.clear(str(tmp_path / "missing.db")) == 0


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


def test_clear_also_removes_refund_records(tmp_path):
    db = str(tmp_path / "read.db")
    readmodel.record_refund("t-1", 42.0, attempt=1, db_path=db)
    readmodel.clear(db)
    assert readmodel.record_refund("t-1", 42.0, attempt=1, db_path=db) is True


def test_default_db_path_resolves_from_config_at_call_time(tmp_path, monkeypatch):
    db = str(tmp_path / "default.db")
    monkeypatch.setattr(config, "DB_PATH", db)
    result = make_result()
    readmodel.save_result(result)
    assert readmodel.load_result("t-1") == result
