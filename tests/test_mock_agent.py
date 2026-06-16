import pytest

from ticketflow.agent.base import AgentOverloadedError
from ticketflow.agent.mock import MockAgent
from ticketflow.models import ActionType, Classification, Ticket, TicketCategory


def make_ticket(subject: str = "Help", body: str = "Something broke") -> Ticket:
    return Ticket(id="t1", customer_email="jo@example.com", subject=subject, body=body)


async def test_classifies_billing_by_keyword():
    agent = MockAgent(seed=1, failure_rate=0.0, model="primary")

    result = await agent.classify(make_ticket(subject="Please refund my last charge"))

    assert result.category == TicketCategory.BILLING
    assert 0.5 <= result.confidence <= 1.0
    assert result.model == "primary"


async def test_classifies_technical_by_keyword():
    agent = MockAgent(seed=1, failure_rate=0.0)

    result = await agent.classify(
        make_ticket(body="the app shows an error and crashes")
    )

    assert result.category == TicketCategory.TECHNICAL


async def test_falls_back_to_general_category():
    agent = MockAgent(seed=1, failure_rate=0.0)

    result = await agent.classify(make_ticket(subject="hello", body="just saying hi"))

    assert result.category == TicketCategory.GENERAL


async def test_proposes_refund_when_refund_rate_is_one():
    agent = MockAgent(seed=1, failure_rate=0.0, refund_rate=1.0)
    ticket = make_ticket(subject="refund my charge")
    classification = await agent.classify(ticket)

    draft = await agent.draft_reply(ticket, classification)

    assert draft.action.type == ActionType.REFUND
    assert draft.action.refund_amount is not None
    assert draft.action.refund_amount > 0


async def test_reply_only_when_refund_rate_is_zero():
    agent = MockAgent(seed=1, failure_rate=0.0, refund_rate=0.0)
    ticket = make_ticket(body="the app crashes")
    classification = await agent.classify(ticket)

    draft = await agent.draft_reply(ticket, classification)

    assert draft.action.type == ActionType.REPLY_ONLY
    assert draft.action.refund_amount is None
    assert draft.reply_text
    assert 0.5 <= draft.confidence <= 1.0
    assert draft.model == "primary"


async def test_default_primary_draft_confidence_stays_above_approval_threshold():
    agent = MockAgent(seed=2, failure_rate=0.0, refund_rate=0.0)
    ticket = make_ticket(body="the app crashes")
    classification = Classification(category=TicketCategory.TECHNICAL, confidence=1.0)

    drafts = [await agent.draft_reply(ticket, classification) for _ in range(100)]

    assert min(draft.confidence for draft in drafts) >= 0.8


async def test_default_primary_refund_rate_is_about_ten_percent():
    agent = MockAgent(seed=3, failure_rate=0.0)
    ticket = make_ticket(subject="Question about account options")
    classification = Classification(category=TicketCategory.GENERAL, confidence=1.0)

    drafts = [await agent.draft_reply(ticket, classification) for _ in range(1_000)]
    refund_count = sum(draft.action.type == ActionType.REFUND for draft in drafts)

    assert 70 <= refund_count <= 130


async def test_fallback_agent_returns_low_confidence_model_outputs():
    agent = MockAgent.fallback(seed=1)
    ticket = make_ticket(body="the app crashes")

    classification = await agent.classify(ticket)
    draft = await agent.draft_reply(ticket, classification)

    assert classification.model == "fallback"
    assert draft.model == "fallback"
    assert 0.0 <= classification.confidence <= 0.6
    assert 0.0 <= draft.confidence <= 0.6


async def test_agent_latency_sleeps_within_seeded_range(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("ticketflow.agent.mock.asyncio.sleep", fake_sleep)
    agent = MockAgent(seed=1, failure_rate=0.0, latency_range=(1.0, 2.0))

    await agent.classify(make_ticket())

    assert len(sleeps) == 1
    assert 1.0 <= sleeps[0] <= 2.0


async def test_draft_reply_raises_transient_error_when_failure_rate_is_one():
    agent = MockAgent(seed=1, failure_rate=1.0, refund_rate=0.0)
    ticket = make_ticket()
    classification = Classification(category=TicketCategory.GENERAL, confidence=1.0)

    with pytest.raises(AgentOverloadedError):
        await agent.draft_reply(ticket, classification)


async def test_raises_transient_error_when_failure_rate_is_one():
    agent = MockAgent(seed=1, failure_rate=1.0)

    with pytest.raises(AgentOverloadedError):
        await agent.classify(make_ticket())


async def test_never_fails_when_failure_rate_is_zero():
    agent = MockAgent(seed=1, failure_rate=0.0)

    for _ in range(50):
        await agent.classify(make_ticket())


async def test_same_seed_produces_same_classification():
    ticket = make_ticket(subject="refund please")
    a = MockAgent(seed=42, failure_rate=0.0)
    b = MockAgent(seed=42, failure_rate=0.0)

    assert await a.classify(ticket) == await b.classify(ticket)
