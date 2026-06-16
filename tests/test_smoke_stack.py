"""End-to-end smoke tests against a running docker stack.

Excluded from the default `make test` run (the `smoke` marker is deselected
via addopts); run them with `make smoke` after `make up`, or `make test-docker`
for the full up/test/down cycle.
"""

from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest

from scripts import doctor
from scripts.batch import SETTLED_STATUSES

pytestmark = pytest.mark.smoke

BASE_URL = os.environ.get("API_URL", "http://localhost:8000")
READY_TIMEOUT_S = 120.0
SETTLE_TIMEOUT_S = 90.0
POLL_INTERVAL_S = 2.0

TERMINAL_STATUSES = SETTLED_STATUSES - {"awaiting_approval"}


@pytest.fixture(autouse=True)
async def ready_stack() -> None:
    """Block until the doctor reports the stack healthy, or fail with diagnostics."""
    deadline = time.monotonic() + READY_TIMEOUT_S
    while True:
        result = await doctor.run(base_url=BASE_URL)
        if result.exit_code == 0:
            return
        if time.monotonic() >= deadline:
            diagnostics = "\n".join(result.lines)
            pytest.fail(
                f"stack at {BASE_URL} not ready after {READY_TIMEOUT_S:.0f}s "
                f"(run `make up`):\n{diagnostics}"
            )
        await asyncio.sleep(POLL_INTERVAL_S)


@pytest.fixture
async def client():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        yield client


async def _wait_for_status(
    client: httpx.AsyncClient, ticket_id: str, statuses: set[str]
) -> dict:
    """Poll the ticket status endpoint until it reaches one of `statuses`."""
    deadline = time.monotonic() + SETTLE_TIMEOUT_S
    while True:
        response = await client.get(f"/tickets/{ticket_id}")
        body = response.json() if response.status_code == 200 else {}
        if body.get("status") in statuses:
            return body
        if time.monotonic() >= deadline:
            pytest.fail(
                f"ticket {ticket_id} did not reach {sorted(statuses)} within "
                f"{SETTLE_TIMEOUT_S:.0f}s; last response: "
                f"HTTP {response.status_code} {response.text}"
            )
        await asyncio.sleep(1.0)


async def test_health_and_ready(client: httpx.AsyncClient):
    health = await client.get("/health")
    assert health.status_code == 200

    ready = await client.get("/ready")
    assert ready.status_code == 200
    body = ready.json()
    assert body["temporal"]["status"] == "healthy"
    assert body["worker"]["status"] == "healthy"
    assert body["llm_worker"]["status"] == "healthy"


async def test_ticket_lifecycle(client: httpx.AsyncClient):
    created = await client.post(
        "/tickets",
        json={
            "customer_email": "smoke@example.com",
            "subject": "Refund request for duplicate charge",
            "body": "I need a refund because my card shows the same charge twice.",
        },
    )
    assert created.status_code == 201
    ticket_id = created.json()["ticket_id"]

    settled = await _wait_for_status(client, ticket_id, SETTLED_STATUSES)

    if settled["status"] == "awaiting_approval":
        approval = await client.post(
            f"/tickets/{ticket_id}/approval",
            json={"approved": True, "approver": "smoke-test", "note": "smoke test"},
        )
        assert approval.status_code == 200
        settled = await _wait_for_status(client, ticket_id, TERMINAL_STATUSES)

    assert settled["status"] in TERMINAL_STATUSES
    assert settled["ticket_id"] == ticket_id
    # Escalation is reachable without agent output (retries exhausted), but
    # resolved/rejected tickets must carry the classification and draft.
    if settled["status"] in {"resolved", "rejected"}:
        assert settled["classification"] is not None
        assert settled["draft"] is not None
