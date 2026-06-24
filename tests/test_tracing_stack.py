"""Tracing smoke checks for the Docker stack with Jaeger enabled."""

from __future__ import annotations

import os
import time

import httpx
import pytest

BASE_URL = os.environ.get("API_URL", "http://localhost:8000")
JAEGER_URL = os.environ.get("JAEGER_URL", "http://localhost:16686")
TRACE_TIMEOUT_S = 90.0
POLL_INTERVAL_S = 2.0

pytestmark = pytest.mark.smoke


@pytest.fixture
async def app_client():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        yield client


@pytest.fixture
async def jaeger_client():
    async with httpx.AsyncClient(base_url=JAEGER_URL, timeout=10.0) as client:
        yield client


async def test_docker_stack_exports_api_traces_to_jaeger(
    app_client: httpx.AsyncClient, jaeger_client: httpx.AsyncClient
):
    response = await app_client.get("/health")
    assert response.status_code == 200

    deadline = time.monotonic() + TRACE_TIMEOUT_S
    while True:
        services_response = await jaeger_client.get("/api/services")
        services_response.raise_for_status()
        services = set(services_response.json().get("data") or [])
        if "ticketflow-api" in services:
            return
        if time.monotonic() >= deadline:
            pytest.fail(f"Jaeger did not receive ticketflow-api traces: {services}")
