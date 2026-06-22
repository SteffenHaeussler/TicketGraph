from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from ticketflow import config, db, graph, readmodel
from ticketflow.api import app
from ticketflow.models import (
    ActionType,
    ApprovalDecision,
    Classification,
    DraftReply,
    ProposedAction,
    TicketCategory,
    TicketResult,
    TicketStatus,
)


def http_client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


class FakeSnapshot:
    """Stand-in LangGraph state snapshot exposing only ``values``."""

    def __init__(self, values: dict[str, object]) -> None:
        self.values = values


class FakeCompiledGraph:
    """Stand-in compiled graph: records seed invocations, returns fixed state.

    ``output`` is returned by ``ainvoke`` (the POST seed path); ``state_values``
    is returned (wrapped in a snapshot) by ``aget_state`` (the GET read path).
    """

    def __init__(
        self,
        output: dict[str, object] | None = None,
        state_values: dict[str, object] | None = None,
    ) -> None:
        self._output = output or {}
        self._state_values = state_values or {}
        self.invocations: list[tuple[object, object]] = []
        self.state_reads: list[object] = []

    async def ainvoke(self, input: object, config: object) -> dict[str, object]:
        self.invocations.append((input, config))
        return self._output

    async def aget_state(self, config: object) -> FakeSnapshot:
        self.state_reads.append(config)
        return FakeSnapshot(self._state_values)


async def test_health_returns_alive_status():
    async with http_client() as http:
        response = await http.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "service": "ticketflow-api"}


async def test_ready_reports_milestone_zero_scaffolding():
    async with http_client() as http:
        response = await http.get("/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "degraded",
        "database": {"status": "not_checked"},
        "orchestration": {
            "status": "not_implemented",
            "message": "LangGraph/Postgres orchestration is not wired yet.",
        },
        "config": {
            "database_url": config.DATABASE_URL,
            "task_queue": config.TASK_QUEUE,
            "agent_task_queue": config.AGENT_TASK_QUEUE,
            "fallback_task_queue": config.FALLBACK_TASK_QUEUE,
        },
    }


async def test_create_ticket_seeds_run_and_returns_ticket_id(monkeypatch):
    wakeup_at = datetime(2026, 6, 16, 12, 0, 30, tzinfo=UTC)
    fake_graph = FakeCompiledGraph(
        {
            "status": TicketStatus.CLASSIFYING,
            "wakeup_at": wakeup_at,
            "__interrupt__": [object()],
        }
    )
    sentinel_pool = object()
    monkeypatch.setattr(app.state, "compiled", fake_graph, raising=False)
    monkeypatch.setattr(app.state, "pool", sentinel_pool, raising=False)

    created: list[tuple[object, ...]] = []

    def record_create_run(
        ticket_id, *, status, wakeup_at, pool=None, database_url=None
    ):
        created.append((ticket_id, status, wakeup_at, pool))

    monkeypatch.setattr(db, "create_run", record_create_run)

    async with http_client() as http:
        response = await http.post(
            "/tickets",
            json={
                "customer_email": "jo@example.com",
                "subject": "refund please",
                "body": "I was double charged.",
            },
        )

    assert response.status_code == 201
    ticket_id = response.json()["ticket_id"]
    assert ticket_id

    # The graph is seeded once with the new ticket on its own durable thread.
    assert len(fake_graph.invocations) == 1
    seed_input, cfg = fake_graph.invocations[0]
    assert isinstance(seed_input, dict)
    assert seed_input["status"] == TicketStatus.RECEIVED
    assert seed_input["ticket"].id == ticket_id
    assert seed_input["ticket"].customer_email == "jo@example.com"
    assert cfg == {"configurable": {"thread_id": ticket_id}}

    # The workflow_run projection mirrors the seeded graph state.
    assert created == [(ticket_id, TicketStatus.CLASSIFYING, wakeup_at, sentinel_pool)]


async def test_list_tickets_queries_workflow_run_status(monkeypatch):
    sentinel_pool = object()
    monkeypatch.setattr(app.state, "pool", sentinel_pool, raising=False)
    calls: list[tuple[object, object]] = []

    def fake_list_runs_by_status(status, *, pool=None, database_url=None):
        calls.append((status, pool))
        return ["ticket-1", "ticket-2"]

    monkeypatch.setattr(db, "list_runs_by_status", fake_list_runs_by_status)

    async with http_client() as http:
        response = await http.get("/tickets?status=awaiting_approval")

    assert response.status_code == 200
    assert response.json() == {"ticket_ids": ["ticket-1", "ticket-2"]}
    assert calls == [(TicketStatus.AWAITING_APPROVAL, sentinel_pool)]


async def test_get_ticket_reads_state_from_checkpoint(monkeypatch):
    classification = Classification(category=TicketCategory.BILLING, confidence=0.9)
    draft = DraftReply(
        reply_text="We will refund you.",
        action=ProposedAction(type=ActionType.REFUND, refund_amount=12.5),
        confidence=0.6,
    )
    fake_graph = FakeCompiledGraph(
        state_values={
            "status": TicketStatus.AWAITING_APPROVAL,
            "classification": classification,
            "draft": draft,
        }
    )
    monkeypatch.setattr(app.state, "compiled", fake_graph, raising=False)
    monkeypatch.setattr(app.state, "pool", object(), raising=False)

    async with http_client() as http:
        response = await http.get("/tickets/ticket-123")

    assert response.status_code == 200
    body = response.json()
    assert body["ticket_id"] == "ticket-123"
    assert body["status"] == "awaiting_approval"
    assert body["classification"] == classification.model_dump(mode="json")
    assert body["draft"] == draft.model_dump(mode="json")
    assert body["decision"] is None
    assert body["result"] is None

    # The checkpoint was read on the ticket's own durable thread.
    assert fake_graph.state_reads == [{"configurable": {"thread_id": "ticket-123"}}]


async def test_get_ticket_falls_back_to_read_model(monkeypatch):
    fake_graph = FakeCompiledGraph(state_values={})
    sentinel_pool = object()
    monkeypatch.setattr(app.state, "compiled", fake_graph, raising=False)
    monkeypatch.setattr(app.state, "pool", sentinel_pool, raising=False)

    result = TicketResult(
        ticket_id="ticket-123",
        status=TicketStatus.RESOLVED,
        reply_text="Refund issued.",
        refund_executed=True,
    )
    loaded: list[tuple[str, object]] = []

    def fake_load_result(ticket_id, *, pool=None, database_url=None):
        loaded.append((ticket_id, pool))
        return result

    monkeypatch.setattr(readmodel, "load_result", fake_load_result)

    async with http_client() as http:
        response = await http.get("/tickets/ticket-123")

    assert response.status_code == 200
    body = response.json()
    assert body["ticket_id"] == "ticket-123"
    assert body["status"] == "resolved"
    assert body["result"] == result.model_dump(mode="json")
    assert body["classification"] is None

    # The fallback uses the request's open pool.
    assert loaded == [("ticket-123", sentinel_pool)]


async def test_get_ticket_returns_404_when_unknown(monkeypatch):
    fake_graph = FakeCompiledGraph(state_values={})
    monkeypatch.setattr(app.state, "compiled", fake_graph, raising=False)
    monkeypatch.setattr(app.state, "pool", object(), raising=False)
    monkeypatch.setattr(readmodel, "load_result", lambda ticket_id, **kw: None)

    async with http_client() as http:
        response = await http.get("/tickets/does-not-exist")

    assert response.status_code == 404
    assert response.json() == {"detail": "ticket not found"}


async def test_submit_approval_writes_pending_signal(monkeypatch):
    sentinel_pool = object()
    monkeypatch.setattr(app.state, "pool", sentinel_pool, raising=False)
    calls: list[tuple[object, ...]] = []

    def fake_add_pending_signal_if_waiting(
        workflow_id,
        kind,
        payload,
        *,
        waiting_status,
        pool=None,
        database_url=None,
    ):
        calls.append((workflow_id, kind, payload, waiting_status, pool))
        return 42

    monkeypatch.setattr(
        db, "add_pending_signal_if_waiting", fake_add_pending_signal_if_waiting
    )

    async with http_client() as http:
        response = await http.post(
            "/tickets/ticket-123/approval",
            json={
                "approved": True,
                "approver": "sam@example.com",
                "note": "Looks good.",
            },
        )

    assert response.status_code == 200
    assert response.json() == {"status": "awaiting_approval"}
    assert calls == [
        (
            "ticket-123",
            "approval_decision",
            ApprovalDecision(
                approved=True, approver="sam@example.com", note="Looks good."
            ).model_dump(mode="json"),
            TicketStatus.AWAITING_APPROVAL,
            sentinel_pool,
        )
    ]


async def test_submit_approval_returns_409_when_not_awaiting_approval(monkeypatch):
    monkeypatch.setattr(app.state, "pool", object(), raising=False)
    monkeypatch.setattr(
        db,
        "add_pending_signal_if_waiting",
        lambda *args, **kwargs: None,
    )

    async with http_client() as http:
        response = await http.post(
            "/tickets/does-not-exist/approval",
            json={"approved": True, "approver": "sam@example.com"},
        )

    assert response.status_code == 409
    assert response.json() == {"detail": "ticket is not awaiting approval"}


@pytest.mark.integration
async def test_create_ticket_persists_workflow_run_and_outbox_through_postgres(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        app.state.pool = postgres_pool
        app.state.compiled = graph.compile_ticket_graph(
            graph.default_activities(), saver, postgres_pool
        )

        async with http_client() as http:
            response = await http.post(
                "/tickets",
                json={
                    "customer_email": "jo@example.com",
                    "subject": "refund please",
                    "body": "I was double charged.",
                },
            )

    assert response.status_code == 201
    ticket_id = response.json()["ticket_id"]
    assert ticket_id

    with postgres_pool.connection() as conn:
        run = conn.execute(
            "SELECT status, wakeup_at, lease_owner FROM workflow_run "
            "WHERE ticket_id = %s",
            (ticket_id,),
        ).fetchone()
        task = conn.execute(
            "SELECT queue_name, task_type, status, idempotency_key "
            "FROM task_queue WHERE workflow_id = %s",
            (ticket_id,),
        ).fetchone()

    # workflow_run projection: parked at classifying with a future timer, unleased.
    assert run is not None
    assert run[0] == "classifying"
    assert run[1] is not None and run[1] > datetime.now(UTC)
    assert run[2] is None

    # Initial outbox: the classify task is enqueued on the agent queue.
    assert task is not None
    assert task[0] == config.AGENT_TASK_QUEUE
    assert task[1] == "classify"
    assert task[2] == "pending"
    assert task[3] == f"{ticket_id}:classify"


@pytest.mark.integration
async def test_get_ticket_reads_state_through_postgres_checkpoint(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        app.state.pool = postgres_pool
        app.state.compiled = graph.compile_ticket_graph(
            graph.default_activities(), saver, postgres_pool
        )

        async with http_client() as http:
            created = await http.post(
                "/tickets",
                json={
                    "customer_email": "jo@example.com",
                    "subject": "refund please",
                    "body": "I was double charged.",
                },
            )
            ticket_id = created.json()["ticket_id"]

            response = await http.get(f"/tickets/{ticket_id}")

            # A ticket that never started has no checkpoint and no result.
            missing = await http.get("/tickets/never-existed")

    # The status is read straight from the durable checkpoint.
    assert response.status_code == 200
    body = response.json()
    assert body["ticket_id"] == ticket_id
    assert body["status"] == "classifying"
    assert body["result"] is None

    assert missing.status_code == 404


@pytest.mark.integration
async def test_get_ticket_falls_back_to_read_model_through_postgres(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    result = TicketResult(
        ticket_id="terminal-ticket",
        status=TicketStatus.RESOLVED,
        reply_text="Refund issued.",
        refund_executed=True,
    )
    readmodel.save_result(result, pool=postgres_pool)

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        app.state.pool = postgres_pool
        app.state.compiled = graph.compile_ticket_graph(
            graph.default_activities(), saver, postgres_pool
        )

        async with http_client() as http:
            # No checkpoint exists for this ticket; the read model answers.
            response = await http.get("/tickets/terminal-ticket")

    assert response.status_code == 200
    body = response.json()
    assert body["ticket_id"] == "terminal-ticket"
    assert body["status"] == "resolved"
    assert body["result"] == result.model_dump(mode="json")


@pytest.mark.integration
async def test_submit_approval_signal_is_consumed_through_postgres(
    postgres_pool: db.ConnectionPool, postgres_database_url: str
):
    from langchain_core.runnables import RunnableConfig

    from tests.helpers import (
        ScriptedAgent,
        billing_classification,
        drive_until_quiescent,
        refund_draft,
    )
    from ticketflow.activities import TicketActivities

    activities = TicketActivities(
        ScriptedAgent(billing_classification(), refund_draft())
    )

    async with AsyncPostgresSaver.from_conn_string(postgres_database_url) as saver:
        await saver.setup()
        app.state.pool = postgres_pool
        app.state.compiled = graph.compile_ticket_graph(
            activities, saver, postgres_pool
        )

        async with http_client() as http:
            created = await http.post(
                "/tickets",
                json={
                    "customer_email": "jo@example.com",
                    "subject": "refund please",
                    "body": "I was double charged.",
                },
            )
            ticket_id = created.json()["ticket_id"]
            cfg: RunnableConfig = {"configurable": {"thread_id": ticket_id}}

            await drive_until_quiescent(
                app.state.compiled, postgres_pool, activities, ticket_id
            )
            awaiting = await app.state.compiled.aget_state(cfg)
            assert awaiting.values["status"] == TicketStatus.AWAITING_APPROVAL

            approved = await http.post(
                f"/tickets/{ticket_id}/approval",
                json={
                    "approved": True,
                    "approver": "sam@example.com",
                    "note": "approved in API integration test",
                },
            )
            await drive_until_quiescent(
                app.state.compiled, postgres_pool, activities, ticket_id
            )
            final = await app.state.compiled.aget_state(cfg)

    with postgres_pool.connection() as conn:
        signal_row = conn.execute(
            """
            SELECT consumed, payload
            FROM pending_signal
            WHERE workflow_id = %s
            """,
            (ticket_id,),
        ).fetchone()

    assert approved.status_code == 200
    assert approved.json() == {"status": "awaiting_approval"}
    assert final.values["status"] == TicketStatus.RESOLVED
    assert final.values["decision"] == ApprovalDecision(
        approved=True,
        approver="sam@example.com",
        note="approved in API integration test",
    )
    assert signal_row is not None
    assert signal_row[0] is True
    assert signal_row[1]["approved"] is True
