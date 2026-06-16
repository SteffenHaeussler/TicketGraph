from httpx import ASGITransport, AsyncClient

from ticketflow import config
from ticketflow.api import app


def http_client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


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


async def test_create_ticket_reports_orchestration_unavailable():
    async with http_client() as http:
        response = await http.post(
            "/tickets",
            json={
                "customer_email": "jo@example.com",
                "subject": "refund please",
                "body": "I was double charged.",
            },
        )

    assert response.status_code == 503
    assert response.json() == {
        "detail": "LangGraph/Postgres orchestration is not wired yet."
    }


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
