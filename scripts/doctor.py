"""Diagnose whether the local Ticketflow Milestone 0 stack is ready."""

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
    """Check API and Milestone 0 readiness through the HTTP API."""
    try:
        health = await client.get("/health")
    except httpx.HTTPError:
        return CheckResult(
            exit_code=1,
            lines=[
                "api: unavailable (run `make api`)",
                "database: unknown",
                "orchestration: unknown",
            ],
        )

    if health.status_code != 200:
        return CheckResult(
            exit_code=1,
            lines=[
                f"api: unavailable (HTTP {health.status_code}; run `make api`)",
                "database: unknown",
                "orchestration: unknown",
            ],
        )

    lines = ["api: healthy"]
    try:
        ready = await client.get("/ready")
        body = ready.json()
    except (httpx.HTTPError, ValueError):
        lines.extend(["database: unknown", "orchestration: unknown"])
        return CheckResult(exit_code=1, lines=lines)

    config = body.get("config", {})
    database = body.get("database", {})
    orchestration = body.get("orchestration", {})
    database_status = str(database.get("status", "unknown"))
    orchestration_status = str(orchestration.get("status", "unknown"))

    lines.append(
        f"database: {database_status} ({_config_value(config, 'database_url')})"
    )
    lines.append(f"orchestration: {orchestration_status}")

    message = orchestration.get("message")
    if isinstance(message, str) and message:
        lines.append(f"orchestration: {message}")

    exit_code = 0 if ready.status_code < 500 and body.get("status") == "healthy" else 1
    return CheckResult(exit_code=exit_code, lines=lines)


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
        description="Check whether the local Ticketflow API is ready."
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
