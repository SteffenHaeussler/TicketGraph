"""Diagnose whether the local Ticketflow stack is ready for demo commands."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(frozen=True)
class CheckResult:
    """Stack readiness result and lines to print to the user."""

    exit_code: int
    lines: list[str]


async def check_stack(client: httpx.AsyncClient) -> CheckResult:
    """Check API, Temporal, and worker readiness through the HTTP API."""
    try:
        health = await client.get("/health")
    except httpx.HTTPError:
        return CheckResult(
            exit_code=1,
            lines=[
                "api: unavailable (run `make api`)",
                "temporal: unknown",
                "worker: unknown",
            ],
        )

    if health.status_code != 200:
        return CheckResult(
            exit_code=1,
            lines=[
                f"api: unavailable (HTTP {health.status_code}; run `make api`)",
                "temporal: unknown",
                "worker: unknown",
            ],
        )

    lines = ["api: healthy"]
    try:
        ready = await client.get("/ready")
        body = ready.json()
    except (httpx.HTTPError, ValueError):
        lines.extend(
            [
                "temporal: unknown",
                "worker: unknown",
            ]
        )
        return CheckResult(exit_code=1, lines=lines)

    config = body.get("config", {})
    temporal = body.get("temporal", {})
    worker = body.get("worker", {})
    llm_worker = body.get("llm_worker", {})
    temporal_status = str(temporal.get("status", "unknown"))
    worker_status = str(worker.get("status", "unknown"))
    llm_worker_status = str(llm_worker.get("status", "unknown"))
    address = _config_value(config, "address")
    namespace = _config_value(config, "namespace")

    lines.append(f"temporal: {temporal_status} ({address}, namespace {namespace})")
    lines.append(_worker_line(worker, config))
    lines.append(_llm_worker_line(llm_worker, config))

    if worker_status == "degraded":
        lines.append("worker: no pollers found; run `make worker`")
    if llm_worker_status == "degraded":
        lines.append("llm-worker: no pollers found; run `make llm-worker`")

    exit_code = (
        1
        if (
            ready.status_code >= 500
            or temporal_status != "healthy"
            or worker_status != "healthy"
            or llm_worker_status != "healthy"
        )
        else 0
    )
    return CheckResult(exit_code=exit_code, lines=lines)


def _worker_line(worker: dict[str, Any], config: dict[str, Any]) -> str:
    worker_status = str(worker.get("status", "unknown"))
    if worker_status == "unknown":
        return "worker: unknown"
    return (
        f"worker: {worker_status} ({_config_value(config, 'task_queue')}; "
        f"workflow pollers={worker.get('workflow_pollers', 'unknown')}, "
        f"activity pollers={worker.get('activity_pollers', 'unknown')})"
    )


def _llm_worker_line(llm_worker: dict[str, Any], config: dict[str, Any]) -> str:
    llm_worker_status = str(llm_worker.get("status", "unknown"))
    if llm_worker_status == "unknown":
        return "llm-worker: unknown"
    primary_queue = str(
        llm_worker.get("primary_task_queue", _config_value(config, "agent_task_queue"))
    )
    fallback_queue = str(
        llm_worker.get(
            "fallback_task_queue", _config_value(config, "fallback_task_queue")
        )
    )
    return (
        f"llm-worker: {llm_worker_status} "
        f"(primary={primary_queue} "
        f"pollers={llm_worker.get('primary_activity_pollers', 'unknown')}, "
        f"fallback={fallback_queue} "
        f"pollers={llm_worker.get('fallback_activity_pollers', 'unknown')})"
    )


def _config_value(config: dict[str, Any], key: str) -> str:
    return str(config.get(key, "unknown"))


def lines_to_print(result: CheckResult, *, quiet: bool) -> list[str]:
    """Return diagnostics unless quiet mode suppresses successful checks."""
    if quiet and result.exit_code == 0:
        return []
    return result.lines


async def run(*, base_url: str) -> CheckResult:
    """Run the stack check against a base API URL."""
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        return await check_stack(client)


def parse_args() -> argparse.Namespace:
    """Parse doctor command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Check whether the local Ticketflow API, Temporal server, "
            "and worker are ready."
        )
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print diagnostics when the stack is not ready.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the doctor command."""
    args = parse_args()
    result = asyncio.run(run(base_url=args.base_url))
    for line in lines_to_print(result, quiet=args.quiet):
        print(line)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
