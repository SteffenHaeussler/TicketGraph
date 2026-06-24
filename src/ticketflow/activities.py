"""Service methods that wrap the agent and side effects."""

import asyncio
import logging

from ticketflow import readmodel
from ticketflow.agent.base import Agent
from ticketflow.models import Classification, DraftReply, Ticket, TicketResult

logger = logging.getLogger(__name__)


class TicketActivities:
    """Agent and side-effect operations used by queue workers."""

    def __init__(
        self,
        agent: Agent,
        database_url: str | None = None,
    ):
        """Create operations backed by an agent and optional persistence settings."""
        self._agent = agent
        self._database_url = database_url

    async def classify_ticket(self, ticket: Ticket) -> Classification:
        """Classify a ticket through the configured agent."""
        return await self._agent.classify(ticket)

    async def draft_reply(
        self, ticket: Ticket, classification: Classification
    ) -> DraftReply:
        """Ask the configured agent to draft a response."""
        return await self._agent.draft_reply(ticket, classification)

    async def send_reply(self, ticket: Ticket, reply_text: str) -> None:
        """Send or log the customer reply."""
        logger.info("Sending reply to %s: %s", ticket.customer_email, reply_text)

    async def execute_refund(
        self, ticket_id: str, amount: float, attempt: int = 1
    ) -> bool:
        """Execute a refund at most once per ticket id."""
        first = await asyncio.to_thread(
            readmodel.record_refund,
            ticket_id,
            amount,
            attempt,
            database_url=self._database_url,
        )
        if first:
            logger.info(
                "Refunding %.2f for ticket %s (attempt %d)", amount, ticket_id, attempt
            )
        else:
            logger.info(
                "Refund for ticket %s already executed; attempt %d is a no-op",
                ticket_id,
                attempt,
            )
        return first

    async def record_result(self, result: TicketResult) -> None:
        """Persist the terminal workflow result to the read model."""
        await asyncio.to_thread(
            readmodel.save_result, result, database_url=self._database_url
        )
