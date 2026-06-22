"""HTTP layer that drives the durable LangGraph/Postgres workflow engine."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from pydantic import BaseModel

from ticketflow import config, db, graph
from ticketflow.logging import reset_ticket_context, set_ticket_context, setup_logging
from ticketflow.models import ApprovalDecision, Ticket, TicketStatus
from ticketflow.tracing import instrument_fastapi_app, setup_tracing_components

setup_logging()
tracing = setup_tracing_components(service_name="ticketflow-api")

logger = logging.getLogger(__name__)
ORCHESTRATION_UNAVAILABLE_DETAIL = "LangGraph/Postgres orchestration is not wired yet."


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the Postgres pool and compile the durable graph for request handlers.

    Mirrors ``runner.main``: bootstrap the schema, open a connection pool, set up
    the checkpointer, and compile the workflow graph. Handlers read ``compiled``
    and ``pool`` from ``app.state``.
    """
    db.bootstrap()
    pool = db.make_pool()
    pool.open()
    try:
        async with AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) as saver:
            await saver.setup()
            app.state.pool = pool
            app.state.compiled = graph.compile_ticket_graph(
                graph.default_activities(), saver, pool
            )
            yield
    finally:
        pool.close()


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
        "database_url": config.DATABASE_URL,
        "task_queue": config.TASK_QUEUE,
        "agent_task_queue": config.AGENT_TASK_QUEUE,
        "fallback_task_queue": config.FALLBACK_TASK_QUEUE,
    }


def _orchestration_unavailable() -> HTTPException:
    return HTTPException(status_code=503, detail=ORCHESTRATION_UNAVAILABLE_DETAIL)


@app.get("/health")
async def health() -> dict[str, str]:
    """Report whether the HTTP process is alive."""
    return {"status": "healthy", "service": "ticketflow-api"}


@app.get("/ready")
async def ready() -> dict[str, object]:
    """Report Milestone 0 readiness for local tooling."""
    return {
        "status": "degraded",
        "database": {"status": "not_checked"},
        "orchestration": {
            "status": "not_implemented",
            "message": ORCHESTRATION_UNAVAILABLE_DETAIL,
        },
        "config": _readiness_config(),
    }


@app.post("/tickets", status_code=201)
async def create_ticket(
    request: Request, body: CreateTicketRequest
) -> CreateTicketResponse:
    """Start a durable ticket workflow and return its generated id.

    Seeds the graph checkpoint with one ``ainvoke`` (which enqueues the initial
    ``classify`` outbox task and parks at ``await_classify``), then records the
    ``workflow_run`` projection so the runner can lease and advance it.
    """
    ticket = Ticket(
        id=uuid4().hex,
        customer_email=body.customer_email,
        subject=body.subject,
        body=body.body,
    )
    compiled = request.app.state.compiled
    pool = request.app.state.pool
    cfg = {"configurable": {"thread_id": ticket.id}}
    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    db.create_run(
        ticket.id, status=out["status"], wakeup_at=out.get("wakeup_at"), pool=pool
    )
    logger.info("ticket workflow started", extra={"ticket_id": ticket.id})
    return CreateTicketResponse(ticket_id=ticket.id)


@app.get("/tickets")
async def list_tickets(status: TicketStatus) -> ListTicketsResponse:
    """Reject ticket listing until workflow runs are stored in Postgres."""
    _ = status
    raise _orchestration_unavailable()


@app.get("/tickets/{ticket_id}")
async def get_ticket(ticket_id: str):
    """Reject ticket status queries until workflow runs are stored in Postgres."""
    _ = ticket_id
    raise _orchestration_unavailable()


@app.post("/tickets/{ticket_id}/approval")
async def submit_approval(
    ticket_id: str, decision: ApprovalDecision
) -> dict[str, TicketStatus]:
    """Reject approvals until workflow signals are stored in Postgres."""
    _ = ticket_id, decision
    raise _orchestration_unavailable()
