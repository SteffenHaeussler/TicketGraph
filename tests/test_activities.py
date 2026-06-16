import sqlite3

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


async def test_side_effect_methods_complete():
    agent = ScriptedAgent(billing_classification(), refund_draft())
    acts = TicketActivities(agent)

    await acts.send_reply(make_ticket(), "hello")
    await acts.execute_refund("t1", 42.0)


async def test_execute_refund_duplicate_run_refunds_once(tmp_path):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    db_path = str(tmp_path / "read.db")
    acts = TicketActivities(agent, db_path=db_path)

    await acts.execute_refund("t1", 42.0, attempt=1)
    await acts.execute_refund("t1", 42.0, attempt=2)

    conn = sqlite3.connect(db_path)
    try:
        attempts = conn.execute(
            "SELECT COUNT(*) FROM refund_attempts WHERE ticket_id = 't1'"
        ).fetchone()[0]
        refunds = conn.execute(
            "SELECT COUNT(*) FROM refunds WHERE ticket_id = 't1'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert attempts == 2
    assert refunds == 1
