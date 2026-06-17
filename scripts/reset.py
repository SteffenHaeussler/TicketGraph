"""Wipe Ticketflow read-model state."""

from __future__ import annotations

import argparse
import asyncio

from ticketflow import config, readmodel


async def run_reset(database_url: str | None = None) -> dict[str, int]:
    """Clear the Postgres read model."""
    return {"read_model_rows_cleared": readmodel.clear(database_url=database_url)}


def parse_args() -> argparse.Namespace:
    """Parse reset command-line arguments."""
    parser = argparse.ArgumentParser(description="Clear the Postgres read model.")
    parser.add_argument("--database-url", default=config.DATABASE_URL)
    return parser.parse_args()


async def amain(args: argparse.Namespace) -> dict[str, int]:
    """Run the reset command."""
    return await run_reset(database_url=args.database_url)


def main() -> int:
    """Run the reset command."""
    args = parse_args()
    summary = asyncio.run(amain(args))
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
