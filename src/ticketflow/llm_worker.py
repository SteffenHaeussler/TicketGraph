"""LLM worker entrypoint: hosts primary and fallback agent activities."""

import asyncio
import logging

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from ticketflow import config
from ticketflow.activities import TicketActivities
from ticketflow.agent.mock import MockAgent
from ticketflow.logging import setup_logging
from ticketflow.tracing import setup_tracing

logger = logging.getLogger(__name__)


async def main() -> None:
    """Run primary and fallback LLM workers until interrupted."""
    setup_logging()
    interceptor = setup_tracing(service_name="ticketflow-llm-worker")
    client = await Client.connect(
        config.TEMPORAL_ADDRESS,
        namespace=config.TEMPORAL_NAMESPACE,
        data_converter=pydantic_data_converter,
        interceptors=[interceptor] if interceptor else [],
    )

    primary_activities = TicketActivities(
        MockAgent(
            latency_range=(0.0, config.MOCK_AGENT_LATENCY_MAX_S),
            model="primary",
        )
    )
    fallback_activities = TicketActivities(MockAgent.fallback())

    primary_worker = Worker(
        client,
        task_queue=config.AGENT_TASK_QUEUE,
        activities=[
            primary_activities.classify_ticket,
            primary_activities.draft_reply,
        ],
        max_concurrent_activities=config.AGENT_MAX_CONCURRENT,
        max_task_queue_activities_per_second=config.AGENT_MAX_PER_SECOND,
    )
    fallback_worker = Worker(
        client,
        task_queue=config.FALLBACK_TASK_QUEUE,
        activities=[
            fallback_activities.classify_ticket,
            fallback_activities.draft_reply,
        ],
    )

    logger.info(
        "LLM workers running",
        extra={
            "primary_task_queue": config.AGENT_TASK_QUEUE,
            "fallback_task_queue": config.FALLBACK_TASK_QUEUE,
        },
    )
    await asyncio.gather(primary_worker.run(), fallback_worker.run())


if __name__ == "__main__":
    asyncio.run(main())
