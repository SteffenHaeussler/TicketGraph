"""Primary agent task worker for the Postgres-backed task queue."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from dataclasses import dataclass, field
from typing import Any

from ticketflow import config, db, taskqueue
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.db import _Pool
from ticketflow.logging import setup_logging
from ticketflow.models import Classification, Ticket

logger = logging.getLogger(__name__)


@dataclass
class TokenBucket:
    """Async single-token bucket that spaces acquisitions at a fixed rate."""

    rate_per_second: float
    _next_available: float | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self) -> None:
        """Reject invalid rate limits early."""
        if self.rate_per_second <= 0:
            raise ValueError("rate_per_second must be greater than zero")

    async def acquire(self) -> None:
        """Wait until the next token is available."""
        interval = 1.0 / self.rate_per_second
        async with self._lock:
            loop = asyncio.get_running_loop()
            now = loop.time()
            if self._next_available is None or now >= self._next_available:
                self._next_available = now + interval
                return
            wait_for = self._next_available - now
            self._next_available += interval

        await asyncio.sleep(wait_for)


async def _run_activity(task: db.QueuedTask, activities: TicketActivities) -> Any:
    if task.task_type == "classify":
        ticket = Ticket.model_validate(task.payload["ticket"])
        return await activities.classify_ticket(ticket)
    if task.task_type == "draft":
        ticket = Ticket.model_validate(task.payload["ticket"])
        classification = Classification.model_validate(task.payload["classification"])
        return await activities.draft_reply(ticket, classification)
    raise ValueError(f"unexpected agent task_type {task.task_type!r}")


def _complete_task(pool: _Pool, task_id: int, result: dict[str, Any]) -> str | None:
    with pool.connection() as conn:
        status = taskqueue.complete(conn, task_id, result=result)
        conn.commit()
    return status


def _fail_task(pool: _Pool, task_id: int, error: str) -> str | None:
    with pool.connection() as conn:
        status = taskqueue.fail(conn, task_id, error=error)
        conn.commit()
    return status


async def process_one_task(
    pool: _Pool,
    activities: TicketActivities,
    worker_id: str = "agent-worker",
    queue_name: str = config.AGENT_TASK_QUEUE,
) -> bool:
    """Lease and process one primary agent task.

    Returns ``True`` when a task was leased and completed or failed, and
    ``False`` when the queue had no due pending work.
    """
    task = await asyncio.to_thread(db.dequeue, queue_name, worker_id, pool=pool)
    if task is None:
        return False

    try:
        result = await _run_activity(task, activities)
    except Exception as exc:
        status = await asyncio.to_thread(_fail_task, pool, task.id, str(exc))
        logger.exception(
            "agent task failed",
            extra={
                "ticket_id": task.workflow_id,
                "task_queue": queue_name,
                "task_type": task.task_type,
                "task_status": status,
            },
        )
        return True

    status = await asyncio.to_thread(
        _complete_task, pool, task.id, result.model_dump(mode="json")
    )
    if status == "done":
        await asyncio.to_thread(db.wake_run, task.workflow_id, pool=pool)
    else:
        logger.warning(
            "agent task completion skipped",
            extra={
                "ticket_id": task.workflow_id,
                "task_queue": queue_name,
                "task_type": task.task_type,
            },
        )
    return True


def _default_worker_id() -> str:
    return f"agent-worker-{socket.gethostname()}-{os.getpid()}"


async def run_forever(
    pool: _Pool,
    activities: TicketActivities,
    *,
    worker_id: str | None = None,
    queue_name: str = config.AGENT_TASK_QUEUE,
    max_per_second: float | None = config.AGENT_MAX_PER_SECOND,
    max_concurrent: int = config.AGENT_MAX_CONCURRENT,
    poll_interval: float = 0.1,
    stop: asyncio.Event | None = None,
) -> None:
    """Continuously drain agent tasks with bounded concurrency.

    Passing ``max_per_second=None`` runs unthrottled (no token bucket), as the
    fallback worker does; otherwise acquisitions are spaced at that rate.
    """
    if max_concurrent <= 0:
        raise ValueError("max_concurrent must be greater than zero")

    resolved_worker_id = worker_id or _default_worker_id()
    bucket = TokenBucket(max_per_second) if max_per_second is not None else None
    in_flight: set[asyncio.Task[bool]] = set()

    async def _wait_for_one() -> bool:
        done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
        processed = False
        for task in done:
            in_flight.remove(task)
            processed = task.result() or processed
        return processed

    try:
        while stop is None or not stop.is_set():
            while len(in_flight) < max_concurrent and (
                stop is None or not stop.is_set()
            ):
                if bucket is not None:
                    await bucket.acquire()
                in_flight.add(
                    asyncio.create_task(
                        process_one_task(
                            pool,
                            activities,
                            worker_id=resolved_worker_id,
                            queue_name=queue_name,
                        )
                    )
                )

            processed = await _wait_for_one() if in_flight else False
            if processed:
                continue
            # Unthrottled mode has no token bucket to pace empty-queue polling,
            # so back off explicitly; the throttled path keeps relying on the
            # bucket and only sleeps when there is nothing in flight.
            if bucket is None or not in_flight:
                await asyncio.sleep(poll_interval)
    except asyncio.CancelledError:
        if in_flight:
            await asyncio.gather(*in_flight, return_exceptions=True)
        raise
    finally:
        if in_flight:
            await asyncio.gather(*in_flight, return_exceptions=True)


async def main() -> None:
    """Run the primary agent worker against configured Postgres."""
    setup_logging()
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    latency_max = config.MOCK_AGENT_LATENCY_MAX_S
    activities = TicketActivities(
        MockAgent(latency_range=(0.0, latency_max)), database_url=config.DATABASE_URL
    )
    try:
        logger.info(
            "agent worker starting",
            extra={"task_queue": config.AGENT_TASK_QUEUE},
        )
        await run_forever(pool, activities, queue_name=config.AGENT_TASK_QUEUE)
    finally:
        pool.close()


if __name__ == "__main__":
    asyncio.run(main())
