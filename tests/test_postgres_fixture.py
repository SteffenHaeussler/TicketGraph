import pytest

from ticketflow import config

pytestmark = pytest.mark.integration


def test_postgres_database_url_patches_config(postgres_database_url: str) -> None:
    assert config.DATABASE_URL == postgres_database_url
    assert "search_path" in postgres_database_url


def test_postgres_pool_uses_an_isolated_schema(postgres_pool) -> None:
    with postgres_pool.connection() as conn:
        conn.execute("CREATE TABLE fixture_probe (id integer PRIMARY KEY)")
        conn.execute("INSERT INTO fixture_probe (id) VALUES (1)")
        row = conn.execute("SELECT count(*) FROM fixture_probe").fetchone()
        conn.commit()

    assert row == (1,)


def test_postgres_pool_does_not_reuse_previous_test_schema(postgres_pool) -> None:
    with postgres_pool.connection() as conn:
        row = conn.execute("SELECT to_regclass('fixture_probe')").fetchone()

    assert row == (None,)
