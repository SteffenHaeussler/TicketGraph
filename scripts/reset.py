"""Wipe local Ticketflow read-model state."""

from __future__ import annotations

import argparse
import asyncio

from ticketflow import config, readmodel


async def run_reset(db_path: str | None = None) -> dict[str, int]:
    """Clear the local read model."""
    return {"read_model_rows_cleared": readmodel.clear(db_path)}


def parse_args() -> argparse.Namespace:
    """Parse reset command-line arguments."""
    parser = argparse.ArgumentParser(description="Clear the local SQLite read model.")
    parser.add_argument("--db-path", default=config.DB_PATH)
    return parser.parse_args()


async def amain(args: argparse.Namespace) -> dict[str, int]:
    """Run the reset command."""
    return await run_reset(db_path=args.db_path)


def main() -> int:
    """Run the reset command."""
    args = parse_args()
    summary = asyncio.run(amain(args))
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
