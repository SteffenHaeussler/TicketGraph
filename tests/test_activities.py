from typing import Any

import pytest

from tests.helpers import (
    ScriptedAgent,
    billing_classification,
    make_ticket,
    refund_draft,
)
from ticketflow.activities import TicketActivities
from ticketflow.agent.base import AgentPermanentError


async def test_classify_ticket_delegates_to_agent():
    agent = ScriptedAgent(billing_classification(), refund_draft())
    acts = TicketActivities(agent)

    result = await acts.classify_ticket(make_ticket())

    assert result == agent.classification
    assert agent.classify_calls == 1


async def test_draft_reply_delegates_to_agent():
    agent = ScriptedAgent(billing_classification(), refund_draft())
    acts = TicketActivities(agent)

    result = await acts.draft_reply(make_ticket(), agent.classification)

    assert result == agent.draft
    assert agent.draft_calls == 1


async def test_agent_permanent_error_is_not_wrapped_in_runtime_dependency():
    class FailingAgent(ScriptedAgent):
        async def classify(self, ticket):
            raise AgentPermanentError("invalid ticket input")

    acts = TicketActivities(FailingAgent(billing_classification(), refund_draft()))

    with pytest.raises(AgentPermanentError, match="invalid ticket input"):
        await acts.classify_ticket(make_ticket())


async def test_side_effect_methods_complete(monkeypatch):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    monkeypatch.setattr(
        "ticketflow.activities.readmodel.record_refund",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        "ticketflow.activities.readmodel.record_sent_reply",
        lambda *args, **kwargs: True,
    )
    acts = TicketActivities(agent)

    await acts.send_reply(make_ticket(), "hello")
    await acts.execute_refund("t1", 42.0)


async def test_execute_refund_returns_readmodel_result(monkeypatch):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    monkeypatch.setattr(
        "ticketflow.activities.readmodel.record_refund",
        lambda *args, **kwargs: False,
    )
    acts = TicketActivities(agent)

    first = await acts.execute_refund("t1", 42.0)

    assert first is False


async def test_execute_refund_passes_refund_details_to_readmodel(monkeypatch):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    calls: list[tuple[str, float, int, str | None, Any]] = []

    def fake_record_refund(
        ticket_id: str,
        amount: float,
        attempt: int,
        *,
        database_url: str | None = None,
        pool: Any | None = None,
    ) -> bool:
        calls.append((ticket_id, amount, attempt, database_url, pool))
        return False

    monkeypatch.setattr(
        "ticketflow.activities.readmodel.record_refund", fake_record_refund
    )
    acts = TicketActivities(agent, database_url="postgresql://example/tickets")

    first = await acts.execute_refund("t1", 42.0, attempt=2)

    assert first is False
    assert calls == [("t1", 42.0, 2, "postgresql://example/tickets", None)]


async def test_send_reply_returns_readmodel_result(monkeypatch):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    monkeypatch.setattr(
        "ticketflow.activities.readmodel.record_sent_reply",
        lambda *args, **kwargs: False,
    )
    acts = TicketActivities(agent)

    first = await acts.send_reply(make_ticket(id="t1"), "hello")

    assert first is False


async def test_send_reply_passes_reply_details_to_readmodel(monkeypatch):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    calls: list[tuple[str, str, str, int, str | None, Any]] = []

    def fake_record_sent_reply(
        ticket_id: str,
        customer_email: str,
        reply_text: str,
        attempt: int,
        *,
        database_url: str | None = None,
        pool: Any | None = None,
    ) -> bool:
        calls.append(
            (ticket_id, customer_email, reply_text, attempt, database_url, pool)
        )
        return True

    monkeypatch.setattr(
        "ticketflow.activities.readmodel.record_sent_reply",
        fake_record_sent_reply,
    )
    acts = TicketActivities(agent, database_url="postgresql://example/tickets")

    first = await acts.send_reply(
        make_ticket(id="t1", customer_email="customer@example.com"),
        "hello",
        attempt=3,
    )

    assert first is True
    assert calls == [
        (
            "t1",
            "customer@example.com",
            "hello",
            3,
            "postgresql://example/tickets",
            None,
        )
    ]


async def test_execute_refund_threads_injected_pool(monkeypatch):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    pool = object()
    seen: list[Any] = []

    def fake_record_refund(*args: Any, pool: Any | None = None, **kwargs: Any) -> bool:
        seen.append(pool)
        return True

    monkeypatch.setattr(
        "ticketflow.activities.readmodel.record_refund", fake_record_refund
    )
    acts = TicketActivities(agent, pool=pool)

    await acts.execute_refund("t1", 42.0)

    assert seen == [pool]


async def test_refund_recorded_threads_injected_pool(monkeypatch):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    pool = object()
    seen: list[Any] = []

    def fake_refund_recorded(
        *args: Any, pool: Any | None = None, **kwargs: Any
    ) -> bool:
        seen.append(pool)
        return True

    monkeypatch.setattr(
        "ticketflow.activities.readmodel.refund_recorded", fake_refund_recorded
    )
    acts = TicketActivities(agent, pool=pool)

    await acts.refund_recorded("t1")

    assert seen == [pool]


async def test_send_reply_threads_injected_pool(monkeypatch):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    pool = object()
    seen: list[Any] = []

    def fake_record_sent_reply(
        *args: Any, pool: Any | None = None, **kwargs: Any
    ) -> bool:
        seen.append(pool)
        return True

    monkeypatch.setattr(
        "ticketflow.activities.readmodel.record_sent_reply", fake_record_sent_reply
    )
    acts = TicketActivities(agent, pool=pool)

    await acts.send_reply(make_ticket(id="t1"), "hello")

    assert seen == [pool]


async def test_record_result_threads_injected_pool(monkeypatch):
    from ticketflow.models import TicketResult, TicketStatus

    agent = ScriptedAgent(billing_classification(), refund_draft())
    pool = object()
    seen: list[Any] = []

    def fake_save_result(*args: Any, pool: Any | None = None, **kwargs: Any) -> None:
        seen.append(pool)

    monkeypatch.setattr("ticketflow.activities.readmodel.save_result", fake_save_result)
    acts = TicketActivities(agent, pool=pool)

    await acts.record_result(
        TicketResult(ticket_id="t1", status=TicketStatus.RESOLVED, reply_text="hi")
    )

    assert seen == [pool]
