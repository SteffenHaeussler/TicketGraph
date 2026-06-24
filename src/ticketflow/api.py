"""HTTP layer that drives the durable LangGraph/Postgres workflow engine."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from pydantic import BaseModel

from ticketflow import config, db, graph, readmodel
from ticketflow.logging import reset_ticket_context, set_ticket_context, setup_logging
from ticketflow.models import (
    ApprovalDecision,
    Ticket,
    TicketStatus,
    TicketStatusInfo,
)
from ticketflow.signals import APPROVAL_DECISION_SIGNAL
from ticketflow.tracing import instrument_fastapi_app, setup_tracing_components

setup_logging()
tracing = setup_tracing_components(service_name="ticketflow-api")

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the Postgres pool and compile the durable graph for request handlers.

    Mirrors ``runner.main``: bootstrap the schema, open connection pools, set up
    the checkpointer, and compile the workflow graph. Handlers read ``compiled``,
    ``pool``, and ``async_pool`` from ``app.state``.
    """
    db.bootstrap()
    pool = db.make_pool()
    async_pool = db.make_async_pool()
    pool.open()
    await async_pool.open()
    try:
        async with AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) as saver:
            await saver.setup()
            app.state.pool = pool
            app.state.async_pool = async_pool
            app.state.compiled = graph.compile_ticket_graph(
                saver, pool, async_pool=async_pool
            )
            yield
    finally:
        await async_pool.close()
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


def _approval_conflict() -> HTTPException:
    return HTTPException(status_code=409, detail="ticket is not awaiting approval")


@app.get("/health")
async def health() -> dict[str, str]:
    """Report whether the HTTP process is alive."""
    return {"status": "healthy", "service": "ticketflow-api"}


def _database_connected(pool: db.ConnectionPool | None) -> bool:
    """Return whether the configured Postgres is reachable through ``pool``."""
    if pool is None:
        return False
    try:
        db.ping(pool=pool)
    except Exception:
        logger.exception("readiness database check failed")
        return False
    return True


@app.get("/ready")
async def ready(request: Request) -> dict[str, object]:
    """Report whether the durable orchestration stack is ready to serve.

    Reflects live state rather than a fixed scaffold: the database is
    ``connected`` when Postgres answers ``SELECT 1`` through the request pool,
    and orchestration is ``ready`` once the durable graph has been compiled
    during lifespan startup. The top-level ``status`` is ``healthy`` only when
    both hold.
    """
    state = request.app.state
    database_connected = _database_connected(getattr(state, "pool", None))
    orchestration_ready = getattr(state, "compiled", None) is not None
    healthy = database_connected and orchestration_ready
    return {
        "status": "healthy" if healthy else "degraded",
        "database": {"status": "connected" if database_connected else "unavailable"},
        "orchestration": {
            "status": "ready" if orchestration_ready else "not_ready",
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
    async_pool = request.app.state.async_pool
    cfg = {"configurable": {"thread_id": ticket.id}}
    out = await compiled.ainvoke(
        {"ticket": ticket, "status": TicketStatus.RECEIVED}, cfg
    )
    await db.acreate_run(
        ticket.id,
        status=out["status"],
        wakeup_at=out.get("wakeup_at"),
        pool=async_pool,
    )
    logger.info("ticket workflow started", extra={"ticket_id": ticket.id})
    return CreateTicketResponse(ticket_id=ticket.id)


@app.get("/tickets")
async def list_tickets(request: Request, status: TicketStatus) -> ListTicketsResponse:
    """Return ticket ids whose workflow-run projection has ``status``."""
    return ListTicketsResponse(
        ticket_ids=db.list_runs_by_status(status, pool=request.app.state.pool)
    )


@app.get("/tickets/{ticket_id}")
async def get_ticket(request: Request, ticket_id: str) -> TicketStatusInfo:
    """Return a ticket's current state from the checkpoint, or the read model.

    The durable LangGraph checkpoint is the live source of truth: a non-empty
    snapshot yields the full in-flight state. When no checkpoint exists (an
    unknown thread returns empty ``values``), fall back to the read model's
    terminal ``ticket_results`` row, and ``404`` when neither knows the ticket.
    """
    compiled = request.app.state.compiled
    pool = request.app.state.pool
    cfg = {"configurable": {"thread_id": ticket_id}}
    snapshot = await compiled.aget_state(cfg)
    values = snapshot.values
    if values:
        return TicketStatusInfo(
            ticket_id=ticket_id,
            status=values["status"],
            classification=values.get("classification"),
            draft=values.get("draft"),
            decision=values.get("decision"),
            result=values.get("result"),
        )
    result = readmodel.load_result(ticket_id, pool=pool)
    if result is not None:
        return TicketStatusInfo(
            ticket_id=ticket_id, status=result.status, result=result
        )
    raise HTTPException(status_code=404, detail="ticket not found")


@app.post("/tickets/{ticket_id}/approval")
async def submit_approval(
    request: Request, ticket_id: str, decision: ApprovalDecision
) -> dict[str, TicketStatus]:
    """Accept a human approval decision by writing a durable workflow signal."""
    signal_id = db.add_pending_signal_if_waiting(
        ticket_id,
        APPROVAL_DECISION_SIGNAL,
        decision.model_dump(mode="json"),
        waiting_status=TicketStatus.AWAITING_APPROVAL,
        pool=request.app.state.pool,
    )
    if signal_id is None:
        raise _approval_conflict()
    return {"status": TicketStatus.AWAITING_APPROVAL}
