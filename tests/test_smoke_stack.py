"""Smoke checks for the Docker stack.

Excluded from the default `make test` run via the `smoke` marker.
"""

from __future__ import annotations

import os

import httpx
import pytest

BASE_URL = os.environ.get("API_URL", "http://localhost:8000")

pytestmark = pytest.mark.smoke


@pytest.fixture
async def client():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        yield client


async def test_health_and_ready_report_stack_status(client: httpx.AsyncClient):
    health = await client.get("/health")
    assert health.status_code == 200

    ready = await client.get("/ready")
    assert ready.status_code == 200
    body = ready.json()
    assert body["database"]["status"] == "not_checked"
    assert body["orchestration"]["status"] == "not_implemented"


async def test_ticket_creation_returns_ticket_id(client: httpx.AsyncClient):
    created = await client.post(
        "/tickets",
        json={
            "customer_email": "smoke@example.com",
            "subject": "Refund request for duplicate charge",
            "body": "I need a refund because my card shows the same charge twice.",
        },
    )
    assert created.status_code == 201
    assert created.json()["ticket_id"]
