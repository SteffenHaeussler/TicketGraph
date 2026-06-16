# TicketFlow

TicketFlow is a LangGraph support-ticket workflow backed by Postgres. A mocked
AI agent classifies tickets, drafts replies, proposes refunds, and pauses for
human approval when a refund or low-confidence response needs review.

Postgres is the single coordination store:

- LangGraph checkpoints persist workflow state with `thread_id = ticket_id`.
- A hand-built `task_queue` table distributes work to separate workers.
- Timers, approval signals, ticket status, refund records, and final results
  live beside the queue so the system can recover from process restarts.

Migration plan: `plan.md`

## Flow

```text
POST /tickets
    |
    v
FastAPI writes workflow_run + initial graph input
    |
    v
runner leases runnable workflow_run rows
    |
    v
LangGraph StateGraph (classify -> draft -> decide -> execute -> record)
    |
    +--> enqueue classify/draft tasks in Postgres task_queue
             |
             +--> agent_worker drains ticketflow-agent
             |       rate limited, bounded concurrency
             |
             `--> fallback worker drains ticketflow-agent-fallback
                     used when primary work waits too long

agent result wakes runner
    |
    v
refund proposed OR confidence < 0.75?
    | no
    v
side-effect worker sends reply -> record result -> RESOLVED

    | yes
    v
await approval signal
    |- approved -> refund ledger + reply -> RESOLVED
    |- rejected -> fallback reply -> REJECTED
    `- timer    -> escalation reply -> ESCALATED
```

The primary worker uses the `Agent` protocol in
`src/ticketflow/agent/base.py`, with the local `MockAgent` implementation in
`src/ticketflow/agent/mock.py`. The orchestration layer treats agent work as
queue tasks, so real LLM-backed implementations can be swapped in behind the
same boundary.

## Run It

Prerequisites:

- [uv](https://docs.astral.sh/uv/)
- Docker, or a local Postgres instance reachable through `DATABASE_URL`

Install dependencies:

```bash
make install
```

Run the local target-state stack in separate terminals:

```bash
docker compose up postgres  # terminal 1: database, queue, checkpoints, read model
make runner                 # terminal 2: leases workflow_run rows and advances graphs
make agent_worker           # terminal 3: drains agent, fallback, and side-effect queues
make api                    # terminal 4: FastAPI app on http://localhost:8000
```

The services are:

- `postgres`: durable state for graph checkpoints, queued tasks, signals,
  timers, status indexes, refund records, and final ticket results.
- `runner`: workflow driver that resumes LangGraph runs when input, task
  results, signals, or timers are ready.
- `agent_worker`: queue worker for primary agent work, fallback agent work, and
  side effects such as replies, refunds, and result recording.
- `api`: HTTP boundary for creating tickets, reading status, listing by status,
  and submitting approval decisions.

Then drive a ticket through:

```bash
make doctor

make ticket
# => {"ticket_id": "<ID>"}

make status ID=<ID>

make approve ID=<ID>
make reject ID=<ID>
```

The mock agent is random. Check status to see which path a ticket took; refund
proposals and low-confidence drafts wait for approval.

For a batch demo, run:

```bash
make batch N=100
```

With the default local worker settings, a 100-ticket batch should mostly use
the primary agent path and roughly 5-15 tickets should wait for approval.

## Run It in Docker

One command instead of separate terminals:

```bash
make up           # builds the app image and starts the stack
make logs         # follow stack logs
make down         # stop it while preserving named volumes
make stack-reset  # stop it and remove database state
```

The stack runs Postgres, the API, the workflow runner, and queue workers. The
API and workers share the same Postgres database so status reads, queue
progress, checkpoint state, approval signals, and final results stay
consistent across restarts.

Smoke-test the deployment with `make smoke`: it waits until `/ready` reports
the stack healthy, then drives a real ticket through create, settle, and
approval against the running services. The smoke tests live in
`tests/test_smoke_stack.py` behind a `smoke` pytest marker, so `make test`
skips them. `make test-docker` runs the stack smoke cycle and leaves logs
available for debugging on failure.

## Tracing

OpenTelemetry tracing is off by default. Enable it with
`TICKETFLOW_TRACE_EXPORTER`:

- `none` (default): tracing disabled
- `console`: spans printed to stdout
- `otlp`: spans exported over OTLP HTTP to `TICKETFLOW_OTLP_ENDPOINT`
  (default `http://localhost:4318/v1/traces`)

For the Docker stack, enable the exporter and tracing profile in one go:

```bash
TICKETFLOW_TRACE_EXPORTER=otlp docker compose --profile tracing up --build
```

To test the traced Docker stack automatically, run:

```bash
make test-docker-tracing
```

This starts the stack with `TICKETFLOW_TRACE_EXPORTER=otlp`, runs the
deployment smoke tests, and verifies that the tracing backend received
TicketFlow spans through its query API.

Each ticket produces spans for the HTTP request, graph resume cycle, queued
agent work, side effects, approval handling, and result recording.

## Tests

```bash
make check
make test
make coverage
```

`make check` runs the local pre-PR gate: Ruff formatting check, Ruff linting,
Pyright, and the normal test suite. `make install` also installs a pre-push
hook that runs `make check` before a branch is pushed.

`make test` runs the normal suite. Queue and workflow tests use isolated
database state and injectable clocks so retries, schedule-to-start fallback,
approval expiry, and idempotent refund behavior can be verified
deterministically.

`make coverage` runs the same suite with a terminal coverage report and
missing-line details. Run `make format` to apply Ruff formatting and import
fixes locally.
