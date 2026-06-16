# Ticketflow

A [Temporal.io](https://temporal.io) learning project: a mocked AI agent
resolves support tickets inside a durable workflow, with conditional
human-in-the-loop approval for refunds and low-confidence drafts.

Design doc: `docs/superpowers/specs/2026-06-10-ticketflow-design.md`
Decision log (how and why each piece was built): `docs/context.md`

## Flow

```text
POST /tickets --> TicketWorkflow
                    classify --> draft reply
                    (primary agent queue; fallback queue if queued too long)
                       |
        refund proposed OR confidence < 0.75?
              | no                  | yes
              v                     v
          send reply        wait for approval update (max 24h)
          RESOLVED          |- approved -> refund + reply -> RESOLVED
                            |- rejected -> fallback reply -> REJECTED
                            `- timeout  -> escalation reply -> ESCALATED
```

The primary agent is a rate-limited `MockAgent` tuned for local demos: about
10% refund proposals, high-confidence non-refund replies, and about 10%
transient failures that demonstrate activity retries. If an agent task waits
too long to start, the workflow reroutes it to a fast fallback mock model with
lower confidence, so fallback tickets visibly wait for human approval. Both sit
behind the `Agent` protocol in
`src/ticketflow/agent/base.py`; swap in real LLM-backed implementations later.

## Run It

Prerequisites: [uv](https://docs.astral.sh/uv/), plus one of:

- the Temporal CLI (`brew install temporal`) for `make server`
- Docker for `make server-docker`

```bash
make install

make server   # terminal 1: Temporal dev server, Web UI at http://localhost:8233
              # or: make server-docker
make worker   # terminal 2: workflow worker
make llm-worker  # terminal 3: primary + fallback LLM activity workers
make api      # terminal 4: FastAPI app
```

The local demo needs all four long-running processes:

- `server`: Temporal dev server. It stores workflow state, schedules tasks, and
  hosts the Web UI.
- `worker`: Python Temporal worker. It polls the `ticketflow` task queue and
  runs workflows and fast side-effect activities.
- `llm-worker`: Python Temporal worker. It polls the primary
  `ticketflow-agent` queue with a shared rate limit and the unthrottled
  `ticketflow-agent-fallback` queue.
- `api`: FastAPI HTTP app. It accepts ticket requests and starts, queries, or
  updates Temporal workflows.

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

With the default local worker settings, a 100-ticket batch should mostly use the
primary agent path and roughly 5-15 tickets should wait for approval.

Watch the workflow history, including retries, updates, and timers, in the
Temporal Web UI at http://localhost:8233.

## Run It in Docker

One command instead of four terminals:

```bash
make up           # builds the app image and starts the whole stack (detached)
make logs         # follow the stack's logs
make down         # stop it (state survives in named volumes)
make stack-reset  # stop it and wipe Temporal + read-model state
```

The stack runs the same four processes as the manual flow — Temporal server,
workflow worker, LLM worker, and the API on http://localhost:8000 — plus a
one-shot `temporal-init` service that registers the `TicketStatus` search
attribute, so `make search-attributes` is not needed. Unlike `make server`,
Temporal state persists across restarts (the dev server writes to a named
volume). `make doctor`, `make ticket`, `make status`, and `make batch` work
against the stack unchanged.

Only three services are strictly required: Temporal, the API, and one worker
process that could host every task queue. The worker is split in two so the
rate-limited LLM tier scales and fails independently of workflow progress —
the split is an ops decision, not a requirement. The API and the workflow
worker share the SQLite read model through a common volume.

Smoke-test the deployment with `make smoke`: it waits until `/ready` reports
all components healthy, then drives a real ticket through create → settle →
approve against the running stack. The smoke tests live in
`tests/test_smoke_stack.py` behind a `smoke` pytest marker, so `make test`
skips them. `make test-docker` runs the full cycle — `make up`, smoke tests,
`docker compose down` — and leaves the stack up on failure for debugging.

## Tracing

OpenTelemetry tracing is off by default. Enable it with
`TICKETFLOW_TRACE_EXPORTER`:

- `none` (default): tracing disabled
- `console`: spans printed to stdout
- `otlp`: spans exported over OTLP HTTP to `TICKETFLOW_OTLP_ENDPOINT`
  (default `http://localhost:4318/v1/traces`)

`make server-docker` also starts Jaeger, which accepts OTLP exports; with
`make server` (Temporal CLI), start Jaeger separately via `make jaeger`. Run
the worker and API with the exporter enabled:

```bash
TICKETFLOW_TRACE_EXPORTER=otlp make worker
TICKETFLOW_TRACE_EXPORTER=otlp make api
```

For the Docker stack, enable the exporter and the Jaeger profile in one go:

```bash
TICKETFLOW_TRACE_EXPORTER=otlp docker compose --profile tracing up --build
```

To test the traced Docker stack automatically, run:

```bash
make test-docker-tracing
```

This starts the stack with `TICKETFLOW_TRACE_EXPORTER=otlp` and the
`tracing` compose profile, runs the deployment smoke tests, then verifies that
Jaeger received Ticketflow spans through its query API.

Create a ticket and open the Jaeger UI at http://localhost:16686: each ticket
produces one trace from `POST /tickets` through `StartWorkflow:TicketWorkflow`,
`RunWorkflow:TicketWorkflow`, and a `RunActivity:<name>` span per step
(classify, draft, refund, send reply).

## Tests

```bash
make check
make test
make coverage
```

Workflow tests run against Temporal's time-skipping test environment, so the
"wait 24 hours for approval" path completes instantly. The first run downloads
a test-server binary; no Temporal CLI, server, or Docker is needed for tests.

`make check` runs the local pre-PR gate: Ruff formatting check, Ruff linting,
and the normal test suite. `make install` also installs a pre-push hook that
runs `make check` automatically before a branch is pushed.

`make test` runs the normal suite. `make coverage` runs the same suite with a
terminal coverage report and missing-line details. Run `make format` to apply
Ruff formatting and import fixes locally.
