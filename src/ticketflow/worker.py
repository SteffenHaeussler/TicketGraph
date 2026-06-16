"""Worker entrypoint: hosts the workflow and activities."""

import asyncio
import logging

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from ticketflow import config
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.logging import setup_logging
from ticketflow.tracing import sandboxed_runner_with_otel, setup_tracing
from ticketflow.workflows import TicketWorkflow

logger = logging.getLogger(__name__)


async def main() -> None:
    """Run the Temporal worker until interrupted."""
    setup_logging()
    interceptor = setup_tracing(service_name="ticketflow-worker")
    client = await Client.connect(
        config.TEMPORAL_ADDRESS,
        namespace=config.TEMPORAL_NAMESPACE,
        data_converter=pydantic_data_converter,
        interceptors=[interceptor] if interceptor else [],
    )
    acts = TicketActivities(MockAgent())
    worker = Worker(
        client,
        task_queue=config.TASK_QUEUE,
        workflows=[TicketWorkflow],
        activities=[
            acts.send_reply,
            acts.execute_refund,
            acts.record_result,
        ],
        workflow_runner=sandboxed_runner_with_otel(),
    )
    logger.info(
        "Worker running",
        extra={"task_queue": config.TASK_QUEUE},
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
