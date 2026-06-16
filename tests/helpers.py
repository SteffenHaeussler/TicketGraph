"""Test doubles and factories shared across test modules."""

import uuid

from ticketflow.agent.base import AgentOverloadedError
from ticketflow.models import (
    ActionType,
    Classification,
    DraftReply,
    ProposedAction,
    Ticket,
    TicketCategory,
)


def make_ticket(**overrides: object) -> Ticket:
    defaults: dict[str, object] = {
        "id": uuid.uuid4().hex,
        "customer_email": "jo@example.com",
        "subject": "Help",
        "body": "Something broke",
    }
    defaults.update(overrides)
    return Ticket.model_validate(defaults)


def billing_classification(
    confidence: float = 0.9, model: str = "primary"
) -> Classification:
    return Classification(
        category=TicketCategory.BILLING, confidence=confidence, model=model
    )


def refund_draft(
    amount: float = 42.0, confidence: float = 0.9, model: str = "primary"
) -> DraftReply:
    return DraftReply(
        reply_text="We can refund you.",
        action=ProposedAction(type=ActionType.REFUND, refund_amount=amount),
        confidence=confidence,
        model=model,
    )


def reply_only_draft(confidence: float = 0.9, model: str = "primary") -> DraftReply:
    return DraftReply(
        reply_text="Try restarting the app.",
        action=ProposedAction(type=ActionType.REPLY_ONLY),
        confidence=confidence,
        model=model,
    )


class ScriptedAgent:
    """Agent stub returning fixed responses; counts calls."""

    def __init__(self, classification: Classification, draft: DraftReply):
        self.classification = classification
        self.draft = draft
        self.classify_calls = 0
        self.draft_calls = 0

    async def classify(self, ticket: Ticket) -> Classification:
        self.classify_calls += 1
        return self.classification

    async def draft_reply(
        self, ticket: Ticket, classification: Classification
    ) -> DraftReply:
        self.draft_calls += 1
        return self.draft


class FlakyAgent:
    """Fails the first `failures` classify calls, then delegates to `inner`."""

    def __init__(self, inner: ScriptedAgent, failures: int):
        self.inner = inner
        self.remaining = failures
        self.classify_calls = 0

    async def classify(self, ticket: Ticket) -> Classification:
        self.classify_calls += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise AgentOverloadedError("flaky")
        return await self.inner.classify(ticket)

    async def draft_reply(
        self, ticket: Ticket, classification: Classification
    ) -> DraftReply:
        return await self.inner.draft_reply(ticket, classification)
