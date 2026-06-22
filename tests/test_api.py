from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from ticketflow import config, db, graph
from ticketflow.api import app
from ticketflow.models import TicketStatus


def http_client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


class FakeCompiledGraph:
    """Stand-in compiled graph: records seed invocations, returns fixed state."""

    def __init__(self, output: dict[str, object]) -> None:
        self._output = output
        self.invocations: list[tuple[object, object]] = []

    async def ainvoke(self, input: object, config: object) -> dict[str, object]:
        self.invocations.append((input, config))
        return self._output


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


async def test_list_tickets_reports_orchestration_unavailable():
    async with http_client() as http:
        response = await http.get("/tickets?status=awaiting_approval")

    assert response.status_code == 503
    assert response.json() == {
        "detail": "LangGraph/Postgres orchestration is not wired yet."
    }


async def test_get_ticket_reports_orchestration_unavailable():
    async with http_client() as http:
        response = await http.get("/tickets/does-not-exist")

    assert response.status_code == 503
    assert response.json() == {
        "detail": "LangGraph/Postgres orchestration is not wired yet."
    }


async def test_submit_approval_reports_orchestration_unavailable():
    async with http_client() as http:
        response = await http.post(
            "/tickets/does-not-exist/approval",
            json={"approved": True, "approver": "sam@example.com"},
        )

    assert response.status_code == 503
    assert response.json() == {
        "detail": "LangGraph/Postgres orchestration is not wired yet."
    }


@pytest.mark.integration
async def test_create_ticket_persists_workflow_run_and_outbox_through_postgres():
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    try:
        with pool.connection() as conn:
            conn.execute("DELETE FROM task_queue")
            conn.execute("DELETE FROM workflow_run")
            conn.commit()

        async with AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) as saver:
            await saver.setup()
            app.state.pool = pool
            app.state.compiled = graph.compile_ticket_graph(
                graph.default_activities(), saver, pool
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

        with pool.connection() as conn:
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
    finally:
        pool.close()

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
