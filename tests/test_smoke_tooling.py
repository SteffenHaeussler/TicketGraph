from __future__ import annotations

import httpx
import pytest


def test_default_compose_does_not_publish_postgres_port() -> None:
    compose = open("docker-compose.yml", encoding="utf-8").read()

    assert '"5432:5432"' not in compose
    assert "POSTGRES_PORT" not in compose


def test_default_compose_publishes_configurable_api_port() -> None:
    compose = open("docker-compose.yml", encoding="utf-8").read()

    assert '"${API_PORT:-8000}:8000"' in compose


def test_host_postgres_override_publishes_configurable_postgres_port() -> None:
    override = open("docker-compose.host-postgres.yml", encoding="utf-8").read()

    assert "postgres:" in override
    assert '"${POSTGRES_PORT:-5432}:5432"' in override


def test_make_smoke_manages_stack_and_test_docker_delegates() -> None:
    makefile = open("Makefile", encoding="utf-8").read()

    assert "smoke:\n\t@set -e;" in makefile
    assert "docker compose up --build -d" in makefile
    assert "uv run python scripts/wait_for_api.py --base-url $(API_URL)" in makefile
    assert "uv run pytest tests/test_smoke_stack.py -o addopts=" in makefile
    assert "trap 'docker compose down' EXIT" in makefile
    assert "test-docker: smoke" in makefile


@pytest.mark.asyncio
async def test_wait_for_api_retries_until_health_is_healthy() -> None:
    from scripts import wait_for_api

    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"status": "healthy"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        result = await wait_for_api.wait_until_healthy(
            client, timeout_seconds=1.0, interval_seconds=0.0
        )

    assert result is None
    assert calls == 3


@pytest.mark.asyncio
async def test_wait_for_api_reports_timeout_after_unhealthy_responses() -> None:
    from scripts import wait_for_api

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"status": "starting"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        result = await wait_for_api.wait_until_healthy(
            client, timeout_seconds=0.0, interval_seconds=0.0
        )

    assert result == "api: unavailable after 0.0s (last status: HTTP 503)"
