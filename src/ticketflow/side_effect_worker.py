"""Side-effect task worker for the default queue.

Consumes ``finalize_ticket`` tasks from ``config.TASK_QUEUE`` and runs the
terminal side effects (``execute_refund`` -> ``send_reply`` -> ``record_result``)
before storing the result, which lets the parked workflow run resume. It reuses
the unthrottled drain loop from :mod:`ticketflow.agent_worker` and only swaps in
its own per-task router (Milestone 5.3).
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket

from ticketflow import agent_worker, config, db
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.logging import setup_logging
from ticketflow.models import (
    ActionType,
    ProposedAction,
    Ticket,
    TicketResult,
    TicketStatus,
)

logger = logging.getLogger(__name__)


def _default_worker_id() -> str:
    return f"side-effect-worker-{socket.gethostname()}-{os.getpid()}"


async def run_finalize(
    task: db.QueuedTask, activities: TicketActivities
) -> TicketResult:
    """Execute the terminal side effects for a ``finalize_ticket`` task.

    Issues the refund (only when the ticket resolved with a refund action),
    sends the reply, and records the result. The refund attempt number is the
    task's own attempt count so retries stay observable in the refund ledger.
    ``refund_executed`` is sourced from durable ledger state (whether a refunds
    row exists) rather than from the refund call's "first time?" return, so a
    finalize retry after a committed refund still reports that money moved.
    Returns the result that is stored as the task result and used to resume the
    workflow run.
    """
    if task.task_type != "finalize_ticket":
        raise ValueError(f"unexpected side-effect task_type {task.task_type!r}")

    ticket = Ticket.model_validate(task.payload["ticket"])
    action = ProposedAction.model_validate(task.payload["action"])
    result = TicketResult.model_validate(task.payload["result"])

    refund_executed = False
    if result.status == TicketStatus.RESOLVED and action.type == ActionType.REFUND:
        assert action.refund_amount is not None
        await activities.execute_refund(
            ticket.id, action.refund_amount, attempt=task.attempts
        )
        refund_executed = await activities.refund_recorded(ticket.id)

    await activities.send_reply(ticket, result.reply_text)
    result = result.model_copy(update={"refund_executed": refund_executed})
    await activities.record_result(result)
    return result


async def main() -> None:
    """Run the unthrottled side-effect worker against configured Postgres."""
    setup_logging()
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    activities = TicketActivities(MockAgent(), database_url=config.DATABASE_URL)
    try:
        logger.info(
            "side-effect worker starting",
            extra={"task_queue": config.TASK_QUEUE},
        )
        await agent_worker.run_forever(
            pool,
            activities,
            worker_id=_default_worker_id(),
            queue_name=config.TASK_QUEUE,
            max_per_second=None,
            max_concurrent=config.AGENT_MAX_CONCURRENT,
            run_activity=run_finalize,
        )
    finally:
        pool.close()


if __name__ == "__main__":
    asyncio.run(main())
