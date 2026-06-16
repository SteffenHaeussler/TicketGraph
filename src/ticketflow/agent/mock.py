"""Mock agent with seedable classification randomness."""

import asyncio
import random

from ticketflow.agent.base import AgentOverloadedError
from ticketflow.models import (
    ActionType,
    Classification,
    DraftReply,
    ProposedAction,
    Ticket,
    TicketCategory,
)

# Multiple keywords can indicate the same support category.
KEYWORD_CATEGORIES: dict[str, TicketCategory] = {
    "refund": TicketCategory.BILLING,
    "money": TicketCategory.BILLING,
    "charge": TicketCategory.BILLING,
    "invoice": TicketCategory.BILLING,
    "crash": TicketCategory.TECHNICAL,
    "error": TicketCategory.TECHNICAL,
    "bug": TicketCategory.TECHNICAL,
    "password": TicketCategory.ACCOUNT,
    "login": TicketCategory.ACCOUNT,
}

REPLY_TEMPLATES: dict[TicketCategory, str] = {
    TicketCategory.BILLING: (
        "Thanks for reaching out about your billing concern. "
        "I've reviewed your account and here is what I can do."
    ),
    TicketCategory.TECHNICAL: (
        "Sorry you hit a technical issue. Please try the steps below - "
        "we've also flagged this to our engineers."
    ),
    TicketCategory.ACCOUNT: (
        "Thanks for contacting us about your account. "
        "I've checked your account settings and here is how to proceed."
    ),
    TicketCategory.GENERAL: (
        "Thanks for your message! Here is some information that should help."
    ),
}


class MockAgent:
    """Seedable agent implementation for local demos and tests."""

    def __init__(
        self,
        seed: int | None = None,
        failure_rate: float = 0.1,
        refund_rate: float = 0.10,
        latency_range: tuple[float, float] = (0.0, 0.0),
        confidence_range: tuple[float, float] = (0.8, 1.0),
        model: str = "primary",
    ):
        """Create a mock agent with configurable transient failures."""
        self._rng = random.Random(seed)
        self._failure_rate = failure_rate
        self._refund_rate = refund_rate
        self._latency_range = latency_range
        self._confidence_range = confidence_range
        self._model = model

    @classmethod
    def fallback(cls, seed: int | None = None) -> "MockAgent":
        """Create a fast, reliable, lower-confidence fallback agent."""
        return cls(
            seed=seed,
            failure_rate=0.0,
            refund_rate=0.0,
            latency_range=(0.0, 0.0),
            confidence_range=(0.0, 0.6),
            model="fallback",
        )

    def _maybe_fail(self) -> None:
        if self._rng.random() < self._failure_rate:
            raise AgentOverloadedError("mock agent backend overloaded")

    async def _maybe_sleep(self) -> None:
        minimum, maximum = self._latency_range
        if maximum <= 0:
            return
        await asyncio.sleep(self._rng.uniform(minimum, maximum))

    def _confidence(self) -> float:
        minimum, maximum = self._confidence_range
        return self._rng.uniform(minimum, maximum)

    async def classify(self, ticket: Ticket) -> Classification:
        """Classify by keyword with randomized confidence."""
        self._maybe_fail()
        await self._maybe_sleep()
        text = f"{ticket.subject} {ticket.body}".lower()
        category = next(
            (
                category
                for keyword, category in KEYWORD_CATEGORIES.items()
                if keyword in text
            ),
            TicketCategory.GENERAL,
        )
        return Classification(
            category=category,
            confidence=self._confidence(),
            model=self._model,
        )

    async def draft_reply(
        self, ticket: Ticket, classification: Classification
    ) -> DraftReply:
        """Draft a template reply and occasionally propose a refund."""
        self._maybe_fail()
        await self._maybe_sleep()
        if self._rng.random() < self._refund_rate:
            action = ProposedAction(
                type=ActionType.REFUND,
                refund_amount=round(self._rng.uniform(5.0, 100.0), 2),
            )
        else:
            action = ProposedAction(type=ActionType.REPLY_ONLY)
        return DraftReply(
            reply_text=REPLY_TEMPLATES[classification.category],
            action=action,
            confidence=self._confidence(),
            model=self._model,
        )
