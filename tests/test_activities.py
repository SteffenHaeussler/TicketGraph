import sqlite3

from temporalio.testing import ActivityEnvironment

from tests.helpers import (
    ScriptedAgent,
    billing_classification,
    make_ticket,
    refund_draft,
)
from ticketflow.activities import TicketActivities


async def test_classify_ticket_delegates_to_agent():
    agent = ScriptedAgent(billing_classification(), refund_draft())
    acts = TicketActivities(agent)
    result = await ActivityEnvironment().run(acts.classify_ticket, make_ticket())
    assert result == agent.classification
    assert agent.classify_calls == 1


async def test_draft_reply_delegates_to_agent():
    agent = ScriptedAgent(billing_classification(), refund_draft())
    acts = TicketActivities(agent)
    result = await ActivityEnvironment().run(
        acts.draft_reply, make_ticket(), agent.classification
    )
    assert result == agent.draft
    assert agent.draft_calls == 1


async def test_side_effect_activities_complete():
    agent = ScriptedAgent(billing_classification(), refund_draft())
    acts = TicketActivities(agent)
    env = ActivityEnvironment()
    await env.run(acts.send_reply, make_ticket(), "hello")
    await env.run(acts.execute_refund, "t1", 42.0)


async def test_execute_refund_duplicate_run_refunds_once(tmp_path):
    agent = ScriptedAgent(billing_classification(), refund_draft())
    db = str(tmp_path / "read.db")
    acts = TicketActivities(agent, db_path=db)
    env = ActivityEnvironment()
    await env.run(acts.execute_refund, "t1", 42.0)
    await env.run(acts.execute_refund, "t1", 42.0)
    conn = sqlite3.connect(db)
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
