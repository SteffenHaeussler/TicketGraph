"""HTTP layer: start tickets, inspect status, approve or reject."""

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from temporalio.api.enums.v1 import task_queue_pb2 as task_queue_enums_pb2
from temporalio.api.taskqueue.v1 import message_pb2 as task_queue_messages_pb2
from temporalio.api.workflowservice.v1 import request_response_pb2
from temporalio.client import (
    Client,
    WorkflowQueryFailedError,
    WorkflowUpdateFailedError,
)
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from ticketflow import config, readmodel
from ticketflow.logging import reset_ticket_context, set_ticket_context, setup_logging
from ticketflow.models import ApprovalDecision, Ticket, TicketStatus, TicketStatusInfo
from ticketflow.tracing import instrument_fastapi_app, setup_tracing_components
from ticketflow.workflows import TicketWorkflow

setup_logging()
tracing = setup_tracing_components(service_name="ticketflow-api")
tracing_interceptor = tracing.interceptor if tracing else None

logger = logging.getLogger(__name__)
READINESS_TIMEOUT = timedelta(seconds=2)
QUERY_TIMEOUT = timedelta(seconds=2)
TICKET_STATUS_SEARCH_ATTRIBUTE = "TicketStatus"
MISSING_TICKET_STATUS_SEARCH_ATTRIBUTE_DETAIL = (
    "search attribute TicketStatus is not registered - run `make search-attributes`"
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Create the shared Temporal client for the FastAPI app lifetime."""
    app.state.temporal = await Client.connect(
        config.TEMPORAL_ADDRESS,
        namespace=config.TEMPORAL_NAMESPACE,
        data_converter=pydantic_data_converter,
        interceptors=[tracing_interceptor] if tracing_interceptor else [],
    )
    yield


app = FastAPI(title="Ticketflow", lifespan=lifespan)

if tracing:
    instrument_fastapi_app(app, tracing)


@app.middleware("http")
async def ticket_context_middleware(request: Request, call_next):
    """Attach a ticket id to logs while handling ticket-specific routes."""
    parts = request.url.path.strip("/").split("/")
    token = None
    if len(parts) >= 2 and parts[0] == "tickets" and parts[1]:
        token = set_ticket_context(parts[1])
    try:
        return await call_next(request)
    finally:
        if token is not None:
            reset_ticket_context(token)


class CreateTicketRequest(BaseModel):
    """Request body for starting a ticket workflow."""

    customer_email: str
    subject: str
    body: str


class CreateTicketResponse(BaseModel):
    """Response body returned after a ticket workflow starts."""

    ticket_id: str


class ListTicketsResponse(BaseModel):
    """Response body for ticket id lists returned by visibility queries."""

    ticket_ids: list[str]


def _readiness_config() -> dict[str, str]:
    return {
        "address": config.TEMPORAL_ADDRESS,
        "namespace": config.TEMPORAL_NAMESPACE,
        "task_queue": config.TASK_QUEUE,
        "agent_task_queue": config.AGENT_TASK_QUEUE,
        "fallback_task_queue": config.FALLBACK_TASK_QUEUE,
    }


def _handle(ticket_id: str):
    return app.state.temporal.get_workflow_handle_for(
        TicketWorkflow.run, f"ticket-{ticket_id}"
    )


def _is_missing_ticket_status_search_attribute(exc: RPCError) -> bool:
    """Return whether Temporal rejected a visibility query for missing setup."""
    raw_status = exc.raw_grpc_status.decode("utf-8", errors="replace")
    error_text = f"{exc.message} {raw_status}"
    return (
        exc.status == RPCStatusCode.INVALID_ARGUMENT
        and TICKET_STATUS_SEARCH_ATTRIBUTE in error_text
    )


async def _task_queue_poller_count(
    task_queue: str,
    task_queue_type: task_queue_enums_pb2.TaskQueueType.ValueType,
) -> int:
    request = request_response_pb2.DescribeTaskQueueRequest(
        namespace=config.TEMPORAL_NAMESPACE,
        task_queue=task_queue_messages_pb2.TaskQueue(name=task_queue),
        task_queue_type=task_queue_type,
        report_pollers=True,
    )
    workflow_service = app.state.temporal.service_client.workflow_service
    response = await workflow_service.describe_task_queue(
        request, timeout=READINESS_TIMEOUT
    )
    return len(response.pollers)


@app.get("/health")
async def health() -> dict[str, str]:
    """Report whether the HTTP process is alive."""
    return {"status": "healthy", "service": "ticketflow-api"}


@app.get("/ready")
async def ready():
    """Report Temporal and worker readiness for demo commands."""
    try:
        temporal_healthy = await app.state.temporal.service_client.check_health(
            timeout=READINESS_TIMEOUT
        )
    except Exception:
        logger.warning("Temporal health check failed", exc_info=True)
        return JSONResponse(
            status_code=503,
            content={
                "status": "unavailable",
                "temporal": {
                    "status": "unavailable",
                    "message": "Temporal server is not reachable. Run `make server`.",
                },
                "worker": {"status": "unknown"},
                "config": _readiness_config(),
            },
        )

    if not temporal_healthy:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unavailable",
                "temporal": {
                    "status": "unavailable",
                    "message": "Temporal workflow service is not serving.",
                },
                "worker": {"status": "unknown"},
                "config": _readiness_config(),
            },
        )

    workflow_pollers = await _task_queue_poller_count(
        config.TASK_QUEUE, task_queue_enums_pb2.TASK_QUEUE_TYPE_WORKFLOW
    )
    activity_pollers = await _task_queue_poller_count(
        config.TASK_QUEUE, task_queue_enums_pb2.TASK_QUEUE_TYPE_ACTIVITY
    )
    primary_agent_pollers = await _task_queue_poller_count(
        config.AGENT_TASK_QUEUE,
        task_queue_enums_pb2.TASK_QUEUE_TYPE_ACTIVITY,
    )
    fallback_agent_pollers = await _task_queue_poller_count(
        config.FALLBACK_TASK_QUEUE,
        task_queue_enums_pb2.TASK_QUEUE_TYPE_ACTIVITY,
    )
    worker_healthy = workflow_pollers > 0 and activity_pollers > 0
    llm_worker_healthy = primary_agent_pollers > 0 and fallback_agent_pollers > 0
    worker = {
        "status": "healthy" if worker_healthy else "degraded",
        "task_queue": config.TASK_QUEUE,
        "workflow_pollers": workflow_pollers,
        "activity_pollers": activity_pollers,
    }
    if not worker_healthy:
        worker["message"] = "No worker pollers found. Run `make worker`."

    llm_worker = {
        "status": "healthy" if llm_worker_healthy else "degraded",
        "primary_task_queue": config.AGENT_TASK_QUEUE,
        "fallback_task_queue": config.FALLBACK_TASK_QUEUE,
        "primary_activity_pollers": primary_agent_pollers,
        "fallback_activity_pollers": fallback_agent_pollers,
    }
    if not llm_worker_healthy:
        llm_worker["message"] = "No LLM worker pollers found. Run `make llm-worker`."

    return {
        "status": "healthy" if worker_healthy and llm_worker_healthy else "degraded",
        "temporal": {"status": "healthy"},
        "worker": worker,
        "llm_worker": llm_worker,
        "config": _readiness_config(),
    }


@app.post("/tickets", status_code=201)
async def create_ticket(request: CreateTicketRequest) -> CreateTicketResponse:
    """Start a ticket workflow and return its public id."""
    ticket = Ticket(id=uuid.uuid4().hex, **request.model_dump())
    try:
        await app.state.temporal.start_workflow(
            TicketWorkflow.run,
            ticket,
            id=f"ticket-{ticket.id}",
            task_queue=config.TASK_QUEUE,
        )
    except WorkflowAlreadyStartedError as exc:
        raise HTTPException(status_code=409, detail="ticket already exists") from exc
    logger.info("Ticket workflow started", extra={"ticket_id": ticket.id})
    return CreateTicketResponse(ticket_id=ticket.id)


@app.get("/tickets")
async def list_tickets(status: TicketStatus) -> ListTicketsResponse:
    """List ticket ids with the requested workflow status."""
    query = f'WorkflowType = "TicketWorkflow" and TicketStatus = "{status.value}"'
    ticket_ids = []
    try:
        async for workflow in app.state.temporal.list_workflows(query):
            workflow_id = workflow.id
            if workflow_id.startswith("ticket-"):
                ticket_ids.append(workflow_id.removeprefix("ticket-"))
    except RPCError as exc:
        if _is_missing_ticket_status_search_attribute(exc):
            raise HTTPException(
                status_code=503,
                detail=MISSING_TICKET_STATUS_SEARCH_ATTRIBUTE_DETAIL,
            ) from exc
        raise
    return ListTicketsResponse(ticket_ids=ticket_ids)


@app.get("/tickets/{ticket_id}")
async def get_ticket(ticket_id: str) -> TicketStatusInfo:
    """Return live workflow status or archived read-model status."""
    try:
        return await _handle(ticket_id).query(
            TicketWorkflow.status, rpc_timeout=QUERY_TIMEOUT
        )
    except RPCError as exc:
        # NOT_FOUND: history deleted after retention. The rest mean the query
        # could not be answered: it needs a live worker to replay history, and
        # without one the dev server returns FAILED_PRECONDITION ("no poller
        # seen for task queue recently") or the client-side rpc_timeout fires
        # as CANCELLED ("Timeout expired").
        fallback_statuses = (
            RPCStatusCode.NOT_FOUND,
            RPCStatusCode.DEADLINE_EXCEEDED,
            RPCStatusCode.UNAVAILABLE,
            RPCStatusCode.FAILED_PRECONDITION,
            RPCStatusCode.CANCELLED,
        )
        if exc.status not in fallback_statuses:
            raise
        result = await asyncio.to_thread(readmodel.load_result, ticket_id)
        if result is None:
            if exc.status == RPCStatusCode.NOT_FOUND:
                raise HTTPException(status_code=404, detail="ticket not found") from exc
            raise HTTPException(
                status_code=503, detail="ticket status temporarily unavailable"
            ) from exc
        return TicketStatusInfo(
            ticket_id=ticket_id, status=result.status, result=result
        )
    except WorkflowQueryFailedError as exc:
        result = await asyncio.to_thread(readmodel.load_result, ticket_id)
        if result is None:
            raise HTTPException(
                status_code=503, detail="ticket status temporarily unavailable"
            ) from exc
        return TicketStatusInfo(
            ticket_id=ticket_id, status=result.status, result=result
        )


@app.post("/tickets/{ticket_id}/approval")
async def submit_approval(
    ticket_id: str, decision: ApprovalDecision
) -> dict[str, TicketStatus]:
    """Submit a human approval decision to a waiting workflow."""
    try:
        status = await _handle(ticket_id).execute_update(
            TicketWorkflow.submit_approval,
            decision,
            result_type=TicketStatus,
        )
    except WorkflowUpdateFailedError as exc:
        raise HTTPException(
            status_code=409, detail="ticket is not awaiting approval"
        ) from exc
    except RPCError as exc:
        if exc.status != RPCStatusCode.NOT_FOUND:
            raise
        # Updating a closed workflow is also NOT_FOUND; only the message
        # distinguishes it from a workflow id that never existed.
        message = exc.message.lower()
        if "completed" in message or (
            message.startswith("update ") and message.endswith(" not found")
        ):
            raise HTTPException(
                status_code=409, detail="ticket already decided"
            ) from exc
        raise HTTPException(status_code=404, detail="ticket not found") from exc
    return {"status": status}
