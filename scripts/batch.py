"""Create and monitor a batch of Ticketflow tickets."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

import httpx

SETTLED_STATUSES = {"resolved", "escalated", "rejected", "awaiting_approval"}
TRANSIENT_STATUS_CODES = {500, 502, 503, 504}

KEYWORD_TEMPLATES = [
    {
        "subject": "Refund request for duplicate charge",
        "body": "I need a refund because my card shows the same charge twice.",
    },
    {
        "subject": "App crash when opening report",
        "body": "The dashboard shows an error and then the app crashes.",
    },
    {
        "subject": "Password reset login help",
        "body": "I cannot login after changing my password.",
    },
    {
        "subject": "Question about account options",
        "body": "I would like to understand which plan is right for our team.",
    },
]


class BatchTimeoutError(RuntimeError):
    """Raised when a batch does not settle before the timeout."""


class PreflightError(RuntimeError):
    """Raised when the API setup is missing before a batch run."""


class FallbackRequirementError(RuntimeError):
    """Raised when a demo batch does not show enough fallback-routed tickets."""


@dataclass(frozen=True)
class TicketSnapshot:
    """Ticket status payload fields used for batch summaries."""

    status: str
    model_path: str | None = None


@dataclass(frozen=True)
class BatchSummary:
    """Status and model-path histograms for a batch run."""

    statuses: dict[str, int]
    model_paths: dict[str, int]


def make_ticket_payloads(count: int) -> list[dict[str, str]]:
    """Build varied ticket creation payloads for a batch run."""
    payloads = []
    for index in range(count):
        template = KEYWORD_TEMPLATES[index % len(KEYWORD_TEMPLATES)]
        payloads.append(
            {
                "customer_email": f"batch-{index + 1}@example.com",
                "subject": f"{template['subject']} #{index + 1}",
                "body": template["body"],
            }
        )
    return payloads


async def create_tickets(
    client: httpx.AsyncClient,
    payloads: Iterable[dict[str, str]],
    *,
    concurrency: int,
) -> list[str]:
    """Create tickets concurrently and return their ids."""
    semaphore = asyncio.Semaphore(concurrency)

    async def create_one(payload: dict[str, str]) -> str:
        async with semaphore:
            response = await client.post("/tickets", json=payload)
            response.raise_for_status()
            return str(response.json()["ticket_id"])

    return await asyncio.gather(*(create_one(payload) for payload in payloads))


async def poll_ticket_statuses(
    client: httpx.AsyncClient,
    ticket_ids: Iterable[str],
    *,
    timeout: float,
    poll_interval: float = 1.0,
    concurrency: int = 10,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, str]:
    """Poll tickets until every id reaches a settled status."""
    snapshots = await poll_ticket_snapshots(
        client,
        ticket_ids,
        timeout=timeout,
        poll_interval=poll_interval,
        concurrency=concurrency,
        sleep=sleep,
    )
    return {ticket_id: snapshot.status for ticket_id, snapshot in snapshots.items()}


async def poll_ticket_snapshots(
    client: httpx.AsyncClient,
    ticket_ids: Iterable[str],
    *,
    timeout: float,
    poll_interval: float = 1.0,
    concurrency: int = 10,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, TicketSnapshot]:
    """Poll tickets until every id settles, preserving status metadata."""
    pending = set(ticket_ids)
    snapshots: dict[str, TicketSnapshot] = {}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    semaphore = asyncio.Semaphore(concurrency)

    async def check_one(ticket_id: str) -> None:
        async with semaphore:
            try:
                response = await client.get(f"/tickets/{ticket_id}")
            except httpx.TimeoutException:
                return
            if response.status_code in TRANSIENT_STATUS_CODES:
                return
            response.raise_for_status()
            body = response.json()
            status = str(body["status"])
            snapshots[ticket_id] = TicketSnapshot(
                status=status,
                model_path=_extract_model_path(body),
            )
            if status in SETTLED_STATUSES:
                pending.remove(ticket_id)

    while pending:
        if loop.time() >= deadline:
            waiting = ", ".join(sorted(pending))
            raise BatchTimeoutError(f"Timed out waiting for tickets: {waiting}")

        await asyncio.gather(*(check_one(ticket_id) for ticket_id in sorted(pending)))

        if pending:
            await sleep(poll_interval)

    return snapshots


def status_histogram(statuses: dict[str, str]) -> dict[str, int]:
    """Summarize settled statuses and include the total ticket count."""
    counts = Counter(statuses.values())
    histogram = {status: counts[status] for status in sorted(counts)}
    histogram["total"] = len(statuses)
    return histogram


def model_path_histogram(snapshots: dict[str, TicketSnapshot]) -> dict[str, int]:
    """Summarize primary/fallback model paths and include the total count."""
    counts = Counter(
        snapshot.model_path or "unknown" for snapshot in snapshots.values()
    )
    histogram = {model_path: counts[model_path] for model_path in sorted(counts)}
    histogram["total"] = len(snapshots)
    return histogram


def fallback_model_path_count(summary: BatchSummary) -> int:
    """Count model paths with any fallback component."""
    count = 0
    for model_path, path_count in summary.model_paths.items():
        if model_path == "total":
            continue
        if "fallback" in model_path.split("/"):
            count += path_count
    return count


async def run_batch(
    *,
    count: int,
    base_url: str,
    concurrency: int,
    timeout: float,
) -> BatchSummary:
    """Create a batch of tickets and return the settled status histogram."""
    payloads = make_ticket_payloads(count)
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        ticket_ids = await create_tickets(client, payloads, concurrency=concurrency)
        snapshots = await poll_ticket_snapshots(
            client,
            ticket_ids,
            timeout=timeout,
            concurrency=concurrency,
        )
    return BatchSummary(
        statuses=status_histogram(
            {ticket_id: snapshot.status for ticket_id, snapshot in snapshots.items()}
        ),
        model_paths=model_path_histogram(snapshots),
    )


def print_histogram(summary: BatchSummary) -> None:
    """Print status and model-path histograms in CLI-friendly form."""
    print("statuses:")
    for status, count in summary.statuses.items():
        print(f"{status}: {count}")
    print("model_paths:")
    for model_path, count in summary.model_paths.items():
        print(f"{model_path}: {count}")


def _extract_model_path(body: dict[str, object]) -> str | None:
    result = body.get("result")
    if isinstance(result, dict):
        model_path = result.get("model_path")
        if isinstance(model_path, str):
            return model_path

    classification = body.get("classification")
    draft = body.get("draft")
    classification_model = (
        classification.get("model") if isinstance(classification, dict) else None
    )
    draft_model = draft.get("model") if isinstance(draft, dict) else None
    if isinstance(classification_model, str) and isinstance(draft_model, str):
        return f"{classification_model}/{draft_model}"
    return None


def parse_args() -> argparse.Namespace:
    """Parse batch driver command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Create many Ticketflow tickets and print a status histogram."
    )
    parser.add_argument("--count", type=_positive_int, default=100)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--concurrency", type=_positive_int, default=10)
    parser.add_argument("--timeout", type=_positive_float, default=120.0)
    parser.add_argument(
        "--require-fallback",
        action="store_true",
        help="fail unless the settled batch includes fallback-routed tickets",
    )
    parser.add_argument("--min-fallback-count", type=_positive_int, default=1)
    return parser.parse_args()


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def main() -> int:
    """Run the batch driver command."""
    args = parse_args()
    try:
        histogram = asyncio.run(
            run_batch(
                count=args.count,
                base_url=args.base_url,
                concurrency=args.concurrency,
                timeout=args.timeout,
            )
        )
    except (BatchTimeoutError, PreflightError, httpx.HTTPError) as exc:
        print(f"batch failed: {exc}")
        return 1

    print_histogram(histogram)
    if args.require_fallback:
        observed = fallback_model_path_count(histogram)
        if observed < args.min_fallback_count:
            exc = FallbackRequirementError(
                "expected at least "
                f"{args.min_fallback_count} fallback-routed ticket, observed "
                f"{observed}"
            )
            print(f"batch failed: {exc}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
