"""Tracing smoke tests against a Docker stack with Jaeger enabled."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Iterable
from typing import Any

import httpx
import pytest

from scripts import doctor
from scripts.batch import SETTLED_STATUSES

pytestmark = pytest.mark.smoke

BASE_URL = os.environ.get("API_URL", "http://localhost:8000")
JAEGER_URL = os.environ.get("JAEGER_URL", "http://localhost:16686")
READY_TIMEOUT_S = 120.0
TRACE_TIMEOUT_S = 90.0
POLL_INTERVAL_S = 2.0


@pytest.fixture(autouse=True)
async def ready_stack() -> None:
    """Block until the application stack reports healthy."""
    deadline = time.monotonic() + READY_TIMEOUT_S
    while True:
        result = await doctor.run(base_url=BASE_URL)
        if result.exit_code == 0:
            return
        if time.monotonic() >= deadline:
            diagnostics = "\n".join(result.lines)
            pytest.fail(
                f"stack at {BASE_URL} not ready after {READY_TIMEOUT_S:.0f}s "
                f"(run `make test-docker-tracing`):\n{diagnostics}"
            )
        await asyncio.sleep(POLL_INTERVAL_S)


@pytest.fixture
async def app_client():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        yield client


@pytest.fixture
async def jaeger_client():
    async with httpx.AsyncClient(base_url=JAEGER_URL, timeout=10.0) as client:
        yield client


async def _wait_for_settled_ticket(
    client: httpx.AsyncClient, ticket_id: str
) -> dict[str, Any]:
    deadline = time.monotonic() + TRACE_TIMEOUT_S
    while True:
        response = await client.get(f"/tickets/{ticket_id}")
        body = response.json() if response.status_code == 200 else {}
        if body.get("status") in SETTLED_STATUSES:
            return body
        if time.monotonic() >= deadline:
            pytest.fail(
                f"ticket {ticket_id} did not settle within "
                f"{TRACE_TIMEOUT_S:.0f}s; last response: "
                f"HTTP {response.status_code} {response.text}"
            )
        await asyncio.sleep(1.0)


def _operation_names(traces: Iterable[dict[str, Any]]) -> set[str]:
    return {
        span["operationName"]
        for trace in traces
        for span in trace.get("spans", [])
        if isinstance(span.get("operationName"), str)
    }


async def _query_traces(
    client: httpx.AsyncClient, service: str
) -> list[dict[str, Any]]:
    end_us = int(time.time() * 1_000_000)
    start_us = end_us - (60 * 60 * 1_000_000)
    response = await client.get(
        "/api/traces",
        params={
            "service": service,
            "start": start_us,
            "end": end_us,
            "limit": 20,
        },
    )
    response.raise_for_status()
    return response.json().get("data", [])


async def test_docker_stack_exports_traces_to_jaeger(
    app_client: httpx.AsyncClient, jaeger_client: httpx.AsyncClient
):
    created = await app_client.post(
        "/tickets",
        json={
            "customer_email": "tracing-smoke@example.com",
            "subject": "Refund request for duplicate charge",
            "body": "My card was charged twice, please refund the duplicate charge.",
        },
    )
    assert created.status_code == 201
    await _wait_for_settled_ticket(app_client, created.json()["ticket_id"])

    expected_services = {
        "ticketflow-api",
        "ticketflow-worker",
        "ticketflow-llm-worker",
    }
    deadline = time.monotonic() + TRACE_TIMEOUT_S
    last_services: set[str] = set()
    last_operations: set[str] = set()

    while True:
        services_response = await jaeger_client.get("/api/services")
        services_response.raise_for_status()
        last_services = set(services_response.json().get("data", []))

        traces = [
            trace
            for service in expected_services
            for trace in await _query_traces(jaeger_client, service)
        ]
        last_operations = _operation_names(traces)

        if (
            expected_services <= last_services
            and "POST /tickets" in last_operations
            and any(
                name.startswith("RunWorkflow:TicketWorkflow")
                for name in last_operations
            )
            and any(name.startswith("RunActivity:") for name in last_operations)
        ):
            return

        if time.monotonic() >= deadline:
            pytest.fail(
                "Jaeger did not receive the expected Ticketflow traces within "
                f"{TRACE_TIMEOUT_S:.0f}s; services={sorted(last_services)}, "
                f"operations={sorted(last_operations)}"
            )
        await asyncio.sleep(POLL_INTERVAL_S)
