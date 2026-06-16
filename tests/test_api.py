import asyncio
import uuid
from datetime import timedelta
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient
from temporalio.client import WorkflowQueryFailedError
from temporalio.service import RPCError, RPCStatusCode

from tests.helpers import (
    ScriptedAgent,
    billing_classification,
    make_ticket,
    make_worker,
    refund_draft,
    reply_only_draft,
)
from ticketflow import config, readmodel
from ticketflow.api import CreateTicketRequest, app, create_ticket
from ticketflow.models import TicketResult, TicketStatus
from ticketflow.workflows import TicketWorkflow


def http_client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


class RecordingTemporalClient:
    def __init__(self) -> None:
        self.workflow_id: str | None = None

    async def start_workflow(self, _workflow, ticket, *, id: str, task_queue: str):
        self.workflow_id = id
        assert id == f"ticket-{ticket.id}"
        assert task_queue == config.TASK_QUEUE


class FakeWorkflowService:
    def __init__(self, workflow_pollers: int, activity_pollers: int) -> None:
        self.workflow_pollers = workflow_pollers
        self.activity_pollers = activity_pollers
        self.requests = []

    async def describe_task_queue(self, request, *, timeout: timedelta):
        self.requests.append(request)
        if request.task_queue_type == 1:
            poller_count = self.workflow_pollers
        else:
            poller_count = self.activity_pollers
        return SimpleNamespace(pollers=[object()] * poller_count)


class FakeServiceClient:
    def __init__(
        self,
        *,
        temporal_healthy: bool = True,
        workflow_pollers: int = 1,
        activity_pollers: int = 1,
    ) -> None:
        self.temporal_healthy = temporal_healthy
        self.workflow_service = FakeWorkflowService(workflow_pollers, activity_pollers)

    async def check_health(self, *, timeout: timedelta) -> bool:
        return self.temporal_healthy


class FakeTemporalClient:
    def __init__(self, service_client: FakeServiceClient) -> None:
        self.service_client = service_client


class FakeWorkflowSummary:
    def __init__(self, workflow_id: str) -> None:
        self.id = workflow_id


class FakeWorkflowIterator:
    def __init__(self, workflow_ids: list[str]) -> None:
        self._workflows = [
            FakeWorkflowSummary(workflow_id) for workflow_id in workflow_ids
        ]

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._workflows:
            raise StopAsyncIteration
        return self._workflows.pop(0)


class FailingWorkflowIterator:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise RPCError(
            "invalid search attribute",
            RPCStatusCode.INVALID_ARGUMENT,
            b"TicketStatus is not a registered search attribute",
        )


class ListingTemporalClient:
    def __init__(self, workflow_ids: list[str]) -> None:
        self.workflow_ids = workflow_ids
        self.queries: list[str] = []

    def list_workflows(self, query: str):
        self.queries.append(query)
        return FakeWorkflowIterator(self.workflow_ids)


class MissingSearchAttributeTemporalClient:
    def list_workflows(self, _query: str):
        return FailingWorkflowIterator()


async def test_create_ticket_uses_full_uuid_hex_id():
    temporal = RecordingTemporalClient()
    app.state.temporal = temporal

    response = await create_ticket(
        CreateTicketRequest(
            customer_email="jo@example.com",
            subject="refund please",
            body="I was double charged.",
        )
    )

    assert len(response.ticket_id) == 32
    int(response.ticket_id, 16)
    assert temporal.workflow_id == f"ticket-{response.ticket_id}"


async def test_list_tickets_filters_by_status_and_returns_ticket_ids():
    temporal = ListingTemporalClient(["ticket-abc", "ticket-def"])
    app.state.temporal = temporal

    async with http_client() as http:
        response = await http.get("/tickets?status=awaiting_approval")

    assert response.status_code == 200
    assert response.json() == {"ticket_ids": ["abc", "def"]}
    assert temporal.queries == [
        'WorkflowType = "TicketWorkflow" and TicketStatus = "awaiting_approval"'
    ]


async def test_list_tickets_reports_missing_ticket_status_search_attribute():
    app.state.temporal = MissingSearchAttributeTemporalClient()

    async with http_client() as http:
        response = await http.get("/tickets?status=awaiting_approval")

    assert response.status_code == 503
    assert response.json() == {
        "detail": (
            "search attribute TicketStatus is not registered - "
            "run `make search-attributes`"
        )
    }


async def test_health_returns_alive_status():
    async with http_client() as http:
        response = await http.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "service": "ticketflow-api"}


async def test_ready_returns_healthy_when_temporal_and_worker_pollers_are_available():
    service_client = FakeServiceClient(workflow_pollers=2, activity_pollers=1)
    app.state.temporal = FakeTemporalClient(service_client)

    async with http_client() as http:
        response = await http.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert body["temporal"]["status"] == "healthy"
    assert body["worker"]["status"] == "healthy"
    assert body["worker"]["workflow_pollers"] == 2
    assert body["worker"]["activity_pollers"] == 1
    assert body["llm_worker"]["status"] == "healthy"
    assert body["llm_worker"]["primary_activity_pollers"] == 1
    assert body["llm_worker"]["fallback_activity_pollers"] == 1
    assert body["config"]["address"] == config.TEMPORAL_ADDRESS
    assert body["config"]["namespace"] == config.TEMPORAL_NAMESPACE
    assert body["config"]["task_queue"] == config.TASK_QUEUE
    assert body["config"]["agent_task_queue"] == config.AGENT_TASK_QUEUE
    assert body["config"]["fallback_task_queue"] == config.FALLBACK_TASK_QUEUE
    task_queue_names = [
        request.task_queue.name for request in service_client.workflow_service.requests
    ]
    assert task_queue_names == [
        config.TASK_QUEUE,
        config.TASK_QUEUE,
        config.AGENT_TASK_QUEUE,
        config.FALLBACK_TASK_QUEUE,
    ]


async def test_ready_reports_degraded_when_worker_pollers_are_missing():
    app.state.temporal = FakeTemporalClient(
        FakeServiceClient(workflow_pollers=0, activity_pollers=0)
    )

    async with http_client() as http:
        response = await http.get("/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "degraded"
    assert body["temporal"]["status"] == "healthy"
    assert body["worker"] == {
        "status": "degraded",
        "task_queue": config.TASK_QUEUE,
        "workflow_pollers": 0,
        "activity_pollers": 0,
        "message": "No worker pollers found. Run `make worker`.",
    }
    assert body["llm_worker"] == {
        "status": "degraded",
        "primary_task_queue": config.AGENT_TASK_QUEUE,
        "fallback_task_queue": config.FALLBACK_TASK_QUEUE,
        "primary_activity_pollers": 0,
        "fallback_activity_pollers": 0,
        "message": "No LLM worker pollers found. Run `make llm-worker`.",
    }


async def test_ready_returns_503_when_temporal_is_unavailable():
    app.state.temporal = FakeTemporalClient(FakeServiceClient(temporal_healthy=False))

    async with http_client() as http:
        response = await http.get("/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unavailable"
    assert body["temporal"]["status"] == "unavailable"
    assert body["worker"]["status"] == "unknown"


async def test_ticket_lifecycle_via_api(env):
    app.state.temporal = env.client
    agent = ScriptedAgent(billing_classification(), refund_draft(amount=42.0))
    async with make_worker(env.client, agent, config.TASK_QUEUE):
        async with http_client() as http:
            created = await http.post(
                "/tickets",
                json={
                    "customer_email": "jo@example.com",
                    "subject": "refund please",
                    "body": "I was double charged.",
                },
            )
            assert created.status_code == 201
            ticket_id = created.json()["ticket_id"]

            for _ in range(100):
                status = await http.get(f"/tickets/{ticket_id}")
                assert status.status_code == 200
                if status.json()["status"] == TicketStatus.AWAITING_APPROVAL:
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("ticket never reached awaiting_approval")

            approved = await http.post(
                f"/tickets/{ticket_id}/approval",
                json={
                    "approved": True,
                    "approver": "sam@example.com",
                    "note": "looks good",
                },
            )
            assert approved.status_code == 200
            assert approved.json() == {"status": TicketStatus.RESOLVED}

            for _ in range(100):
                status = await http.get(f"/tickets/{ticket_id}")
                if status.json()["status"] == TicketStatus.RESOLVED:
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("ticket never resolved")
            assert status.json()["decision"]["approver"] == "sam@example.com"


async def test_unknown_ticket_returns_404(env):
    app.state.temporal = env.client
    async with http_client() as http:
        response = await http.get("/tickets/does-not-exist")
    assert response.status_code == 404


async def test_approval_on_unknown_ticket_returns_404(env):
    app.state.temporal = env.client
    async with http_client() as http:
        response = await http.post(
            "/tickets/does-not-exist/approval",
            json={"approved": True, "approver": "sam@example.com"},
        )
    assert response.status_code == 404


async def test_approval_on_resolved_ticket_returns_409(env):
    app.state.temporal = env.client
    agent = ScriptedAgent(billing_classification(), reply_only_draft(confidence=0.9))
    async with make_worker(env.client, agent, config.TASK_QUEUE):
        ticket = make_ticket()
        handle = await env.client.start_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=config.TASK_QUEUE,
        )
        result = await handle.result()
        assert result.status == TicketStatus.RESOLVED

        async with http_client() as http:
            response = await http.post(
                f"/tickets/{ticket.id}/approval",
                json={
                    "approved": True,
                    "approver": "sam@example.com",
                    "note": "too late",
                },
            )
    assert response.status_code == 409
    assert response.json()["detail"] == "ticket already decided"


async def test_create_existing_ticket_returns_409(env, monkeypatch):
    app.state.temporal = env.client
    ticket = make_ticket()
    await env.client.start_workflow(
        TicketWorkflow.run,
        ticket,
        id=f"ticket-{ticket.id}",
        task_queue=config.TASK_QUEUE,
    )

    # Patching uuid4 globally also fixes the request_id the Temporal SDK
    # generates per start call, so only one POST may happen under the patch:
    # a second would be deduplicated as a retry instead of rejected.
    monkeypatch.setattr(uuid, "uuid4", lambda: uuid.UUID(hex=ticket.id))
    async with http_client() as http:
        response = await http.post(
            "/tickets",
            json={
                "customer_email": ticket.customer_email,
                "subject": ticket.subject,
                "body": ticket.body,
            },
        )
    assert response.status_code == 409


class QueryUnreachableClient:
    """Simulates a worker that never answers the status query."""

    def __init__(self, status: RPCStatusCode) -> None:
        self._status = status

    def get_workflow_handle_for(self, _run, _workflow_id):
        return SimpleNamespace(query=self._query)

    async def _query(self, *_args, **_kwargs):
        raise RPCError("query failed", self._status, b"")


class QueryFailedClient:
    """Simulates a worker that rejects a status query during replay."""

    def get_workflow_handle_for(self, _run, _workflow_id):
        return SimpleNamespace(query=self._query)

    async def _query(self, *_args, **_kwargs):
        raise WorkflowQueryFailedError("query failed")


def stored_result(ticket_id: str) -> TicketResult:
    return TicketResult(
        ticket_id=ticket_id,
        status=TicketStatus.RESOLVED,
        reply_text="Archived reply.",
        refund_executed=True,
    )


async def test_get_ticket_falls_back_to_read_model_after_retention(env):
    app.state.temporal = env.client
    readmodel.save_result(stored_result("gone"))

    async with http_client() as http:
        response = await http.get("/tickets/gone")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == TicketStatus.RESOLVED
    assert body["result"]["reply_text"] == "Archived reply."
    assert body["result"]["refund_executed"] is True


@pytest.mark.parametrize(
    "status",
    [
        # Observed against the dev server with the worker stopped:
        # FAILED_PRECONDITION ("no poller seen for task queue recently"),
        # CANCELLED ("Timeout expired" from the client-side rpc_timeout),
        # plus the generic timeout/outage codes.
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.UNAVAILABLE,
        RPCStatusCode.FAILED_PRECONDITION,
        RPCStatusCode.CANCELLED,
    ],
)
async def test_get_ticket_falls_back_to_read_model_when_worker_is_down(status):
    app.state.temporal = QueryUnreachableClient(status)
    readmodel.save_result(stored_result("slow"))

    async with http_client() as http:
        response = await http.get("/tickets/slow")

    assert response.status_code == 200
    assert response.json()["status"] == TicketStatus.RESOLVED


async def test_get_ticket_query_timeout_without_read_model_returns_503():
    app.state.temporal = QueryUnreachableClient(RPCStatusCode.DEADLINE_EXCEEDED)

    async with http_client() as http:
        response = await http.get("/tickets/missing")

    assert response.status_code == 503
    assert response.json()["detail"] == "ticket status temporarily unavailable"


async def test_get_ticket_query_failure_without_read_model_returns_503():
    app.state.temporal = QueryFailedClient()

    async with http_client() as http:
        response = await http.get("/tickets/missing")

    assert response.status_code == 503
    assert response.json()["detail"] == "ticket status temporarily unavailable"
