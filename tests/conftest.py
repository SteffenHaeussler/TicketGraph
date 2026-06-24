import os
import subprocess
import uuid
from collections.abc import Iterator
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg
import pytest
from psycopg import sql

from ticketflow import config, db


@pytest.fixture(scope="session")
def postgres_base_url() -> Iterator[str]:
    """Return a Postgres URL for integration tests.

    Set TEST_DATABASE_URL to reuse a developer/CI-managed Postgres instance.
    Otherwise a single Postgres container is started for the test session.
    """
    if database_url := os.environ.get("TEST_DATABASE_URL"):
        yield database_url
        return

    _set_docker_host_from_active_context()

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:17-alpine", driver=None) as postgres:
        yield postgres.get_connection_url()


def _database_url_with_search_path(database_url: str, schema_name: str) -> str:
    parsed = urlparse(database_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["options"] = f"-csearch_path={schema_name}"
    return urlunparse(parsed._replace(query=urlencode(query)))


def _set_docker_host_from_active_context() -> None:
    if os.environ.get("DOCKER_HOST"):
        return

    result = subprocess.run(
        ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    docker_host = result.stdout.strip()
    if result.returncode == 0 and docker_host:
        os.environ["DOCKER_HOST"] = docker_host


@pytest.fixture
def postgres_database_url(
    postgres_base_url: str, monkeypatch: pytest.MonkeyPatch
) -> Iterator[str]:
    """Create an isolated Postgres schema and point app config at it."""
    schema_name = f"tf_test_{uuid.uuid4().hex}"
    with psycopg.connect(postgres_base_url) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        conn.commit()

    database_url = _database_url_with_search_path(postgres_base_url, schema_name)
    monkeypatch.setattr(config, "DATABASE_URL", database_url)
    try:
        yield database_url
    finally:
        with psycopg.connect(postgres_base_url) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )
            conn.commit()


@pytest.fixture
def postgres_pool(postgres_database_url: str) -> Iterator[db.ConnectionPool]:
    """Bootstrap and open a pool against a per-test Postgres schema."""
    db.bootstrap(database_url=postgres_database_url)
    pool = db.make_pool(postgres_database_url)
    pool.open()
    try:
        yield pool
    finally:
        pool.close()
