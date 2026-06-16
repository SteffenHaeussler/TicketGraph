"""Agent worker entrypoint placeholder for the Postgres task queue migration."""

import asyncio
import logging

from ticketflow.logging import setup_logging

logger = logging.getLogger(__name__)


async def main() -> None:
    """Report that the Postgres task queue workers are not implemented yet."""
    setup_logging()
    logger.warning("Postgres-backed agent workers are not wired yet")


if __name__ == "__main__":
    asyncio.run(main())
