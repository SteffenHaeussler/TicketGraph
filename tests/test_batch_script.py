import asyncio
from collections import Counter

import httpx
import pytest

from scripts import batch


def test_ticket_payloads_cover_mock_agent_keyword_categories():
    payloads = batch.make_ticket_payloads(8)

    combined = [
        f"{payload['subject']} {payload['body']}".lower() for payload in payloads
    ]

    assert any("refund" in text or "charge" in text for text in combined)
    assert any("crash" in text or "error" in text for text in combined)
    assert any("password" in text or "login" in text for text in combined)
    assert any(
        not any(
            keyword in text
            for keyword in (
                "refund",
                "money",
                "charge",
                "invoice",
                "crash",
                "error",
                "bug",
                "password",
                "login",
            )
        )
        for text in combined
    )


async def test_create_tickets_posts_count_with_bounded_concurrency():
    active = 0
    max_active = 0
    paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, max_active
        assert request.method == "POST"
        paths.append(request.url.path)
        ticket_number = len(paths)
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(
            201,
            json={"ticket_id": f"ticket-{ticket_number}"},
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        ticket_ids = await batch.create_tickets(
            client,
            batch.make_ticket_payloads(5),
            concurrency=2,
        )

    assert ticket_ids == ["ticket-1", "ticket-2", "ticket-3", "ticket-4", "ticket-5"]
    assert paths == ["/tickets"] * 5
    assert max_active == 2


async def test_poll_ticket_statuses_stops_when_all_are_settled():
    calls = Counter()

    async def handler(request: httpx.Request) -> httpx.Response:
        ticket_id = request.url.path.rsplit("/", 1)[-1]
        calls[ticket_id] += 1
        if ticket_id == "one" and calls[ticket_id] == 1:
            return httpx.Response(
                200, json={"ticket_id": ticket_id, "status": "drafting"}
            )
        if ticket_id == "one":
            return httpx.Response(
                200, json={"ticket_id": ticket_id, "status": "resolved"}
            )
        return httpx.Response(
            200,
            json={"ticket_id": ticket_id, "status": "awaiting_approval"},
        )

    async def no_sleep(_seconds: float) -> None:
        return None

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        statuses = await batch.poll_ticket_statuses(
            client,
            ["one", "two"],
            timeout=1.0,
            poll_interval=0.0,
            sleep=no_sleep,
        )

    assert statuses == {"one": "resolved", "two": "awaiting_approval"}
    assert calls == Counter({"one": 2, "two": 1})


async def test_poll_ticket_statuses_checks_pending_with_bounded_concurrency():
    active = 0
    max_active = 0
    calls = Counter()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, max_active
        ticket_id = request.url.path.rsplit("/", 1)[-1]
        calls[ticket_id] += 1
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(200, json={"ticket_id": ticket_id, "status": "resolved"})

    async def no_sleep(_seconds: float) -> None:
        return None

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        statuses = await batch.poll_ticket_statuses(
            client,
            ["one", "two", "three", "four", "five"],
            timeout=1.0,
            poll_interval=0.0,
            sleep=no_sleep,
            concurrency=2,
        )

    assert statuses == {ticket_id: "resolved" for ticket_id in calls}
    assert calls == Counter({"one": 1, "two": 1, "three": 1, "four": 1, "five": 1})
    assert max_active == 2


async def test_poll_ticket_statuses_retries_transient_status_errors():
    calls = Counter()

    async def handler(request: httpx.Request) -> httpx.Response:
        ticket_id = request.url.path.rsplit("/", 1)[-1]
        calls[ticket_id] += 1
        if calls[ticket_id] == 1:
            return httpx.Response(500, json={"detail": "query timed out"})
        return httpx.Response(200, json={"ticket_id": ticket_id, "status": "resolved"})

    async def no_sleep(_seconds: float) -> None:
        return None

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        statuses = await batch.poll_ticket_statuses(
            client,
            ["one"],
            timeout=1.0,
            poll_interval=0.0,
            sleep=no_sleep,
        )

    assert statuses == {"one": "resolved"}
    assert calls == Counter({"one": 2})


async def test_poll_ticket_statuses_retries_status_timeouts():
    calls = Counter()

    async def handler(request: httpx.Request) -> httpx.Response:
        ticket_id = request.url.path.rsplit("/", 1)[-1]
        calls[ticket_id] += 1
        if calls[ticket_id] == 1:
            raise httpx.ReadTimeout("query timed out", request=request)
        return httpx.Response(200, json={"ticket_id": ticket_id, "status": "resolved"})

    async def no_sleep(_seconds: float) -> None:
        return None

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        statuses = await batch.poll_ticket_statuses(
            client,
            ["one"],
            timeout=1.0,
            poll_interval=0.0,
            sleep=no_sleep,
        )

    assert statuses == {"one": "resolved"}
    assert calls == Counter({"one": 2})


def test_status_histogram_counts_statuses_deterministically():
    histogram = batch.status_histogram(
        {
            "one": "resolved",
            "two": "awaiting_approval",
            "three": "resolved",
            "four": "escalated",
        }
    )

    assert histogram == {
        "awaiting_approval": 1,
        "escalated": 1,
        "resolved": 2,
        "total": 4,
    }


def test_model_path_histogram_counts_known_model_paths():
    histogram = batch.model_path_histogram(
        {
            "one": batch.TicketSnapshot(
                status="resolved", model_path="primary/primary"
            ),
            "two": batch.TicketSnapshot(
                status="awaiting_approval", model_path="fallback/fallback"
            ),
            "three": batch.TicketSnapshot(
                status="awaiting_approval", model_path="fallback/fallback"
            ),
            "four": batch.TicketSnapshot(status="escalated", model_path=None),
        }
    )

    assert histogram == {
        "fallback/fallback": 2,
        "primary/primary": 1,
        "unknown": 1,
        "total": 4,
    }


def test_print_histogram_prints_statuses_and_models(capsys):
    summary = batch.BatchSummary(
        statuses={"resolved": 1, "total": 1},
        model_paths={"primary/primary": 1, "total": 1},
    )

    batch.print_histogram(summary)

    assert capsys.readouterr().out.splitlines() == [
        "statuses:",
        "resolved: 1",
        "total: 1",
        "model_paths:",
        "primary/primary: 1",
        "total: 1",
    ]


async def test_poll_ticket_statuses_times_out():
    async def handler(request: httpx.Request) -> httpx.Response:
        ticket_id = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json={"ticket_id": ticket_id, "status": "drafting"})

    async def no_sleep(_seconds: float) -> None:
        return None

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(batch.BatchTimeoutError, match="Timed out waiting"):
            await batch.poll_ticket_statuses(
                client,
                ["one"],
                timeout=0.0,
                poll_interval=0.0,
                sleep=no_sleep,
            )
