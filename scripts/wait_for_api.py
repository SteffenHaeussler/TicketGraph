"""Wait for the Ticketflow API health endpoint to become available."""

from __future__ import annotations

import argparse
import asyncio
import time

import httpx


async def wait_until_healthy(
    client: httpx.AsyncClient,
    *,
    timeout_seconds: float,
    interval_seconds: float,
) -> str | None:
    """Return None once /health is healthy, otherwise a timeout diagnostic."""
    deadline = time.monotonic() + timeout_seconds
    last_status = "not checked"

    while True:
        try:
            response = await client.get("/health")
            if response.status_code == 200:
                return None
            last_status = f"HTTP {response.status_code}"
        except httpx.HTTPError as exc:
            last_status = f"{type(exc).__name__}: {exc}"

        if time.monotonic() >= deadline:
            return (
                f"api: unavailable after {timeout_seconds:.1f}s "
                f"(last status: {last_status})"
            )

        await asyncio.sleep(interval_seconds)


async def run(
    *,
    base_url: str,
    timeout_seconds: float,
    interval_seconds: float,
) -> int:
    """Poll the API health endpoint and print a timeout diagnostic on failure."""
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        result = await wait_until_healthy(
            client,
            timeout_seconds=timeout_seconds,
            interval_seconds=interval_seconds,
        )
    if result is None:
        return 0
    print(result)
    return 1


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Wait for the Ticketflow API.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--interval", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    """Run the API wait loop."""
    args = parse_args()
    return asyncio.run(
        run(
            base_url=args.base_url,
            timeout_seconds=args.timeout,
            interval_seconds=args.interval,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
