"""Worker entrypoint placeholder for the Postgres/LangGraph migration."""

import asyncio
import logging

from ticketflow.logging import setup_logging

logger = logging.getLogger(__name__)


async def main() -> None:
    """Report that the durable runner is not implemented in Milestone 0."""
    setup_logging()
    logger.warning("LangGraph/Postgres runner is not wired yet")


if __name__ == "__main__":
    asyncio.run(main())
