"""Unthrottled agent task worker for the fallback queue."""

from __future__ import annotations

import asyncio
import logging
import os
import socket

from ticketflow import agent_worker, config, db
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.logging import setup_logging

logger = logging.getLogger(__name__)


def _default_worker_id() -> str:
    return f"fallback-worker-{socket.gethostname()}-{os.getpid()}"


async def main() -> None:
    """Run the unthrottled fallback worker against configured Postgres."""
    setup_logging()
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    activities = TicketActivities(
        MockAgent.fallback(), database_url=config.DATABASE_URL
    )
    try:
        logger.info(
            "fallback worker starting",
            extra={"task_queue": config.FALLBACK_TASK_QUEUE},
        )
        await agent_worker.run_forever(
            pool,
            activities,
            worker_id=_default_worker_id(),
            queue_name=config.FALLBACK_TASK_QUEUE,
            max_per_second=None,
            max_concurrent=config.AGENT_MAX_CONCURRENT,
        )
    finally:
        pool.close()


if __name__ == "__main__":
    asyncio.run(main())
