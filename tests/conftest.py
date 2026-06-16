import pytest

from ticketflow import config


@pytest.fixture(autouse=True)
def isolated_read_model(tmp_path, monkeypatch):
    """Keep tests from writing to the real read-model DB in the repo root."""
    monkeypatch.setattr(config, "DB_PATH", str(tmp_path / "readmodel.db"))
