# TicketFlow

TicketFlow is a durable support-ticket agent with human-in-the-loop approval. It
runs a LangGraph workflow on top of Postgres, where Postgres is the single store
for the workflow checkpointer, a hand-built task queue, and the read model. It is
a full replacement for the original Temporal backend, keeping the external HTTP
API contract identical.

The migration that built this (and the *Designing Data-Intensive Applications*
concept behind each piece) is tracked in `plan.md`.

## How it works

A ticket flows through a LangGraph state machine
(`classify → draft → decide_approval → execute → record`) whose state is
checkpointed in Postgres after every step. Agent calls don't run inline — they
are **enqueued as tasks** and picked up by separate worker processes, so the
system is genuinely distributed and survives crashes.

Five processes cooperate (all share one Postgres):

| Process | Module | Role |
|---|---|---|
| `api` | `ticketflow.api` | FastAPI HTTP contract: create / inspect / approve tickets |
| `runner` | `ticketflow.runner` | Leases runnable workflows, advances the graph, dispatches tasks at-least-once (made safe by idempotency keys), fires durable timers |
| `agent_worker` | `ticketflow.agent_worker` | Drains the primary agent queue with a token-bucket rate limit + bounded concurrency |
| `fallback_worker` | `ticketflow.fallback_worker` | Unthrottled drain of the fallback queue (used when a task misses its schedule-to-start budget) |
| `side_effect_worker` | `ticketflow.side_effect_worker` | Runs `send_reply` / `execute_refund` / `record_result`, with at-most-once refunds via the ledger |

A ticket moves through these states:

```
received → classifying → drafting → ┬─────────────────────────→ resolved
                                     └─ awaiting_approval ─┬───→ resolved / rejected
                                                           └─(24h timeout)→ escalated
```

A refund action or a low-confidence (`< 0.75`) draft stops at
`awaiting_approval` until a human decides (or 24h elapses and it escalates).

## Run It

Prerequisites:

- [uv](https://docs.astral.sh/uv/)
- Docker (Compose runs Postgres and all worker processes)

Install dependencies:

```bash
make install
```

### Option A — run the whole stack in Docker (simplest)

This is the quickest way to get a working, end-to-end TicketFlow:

```bash
make up      # builds and starts postgres + api + runner + all three workers
make logs    # follow logs (Ctrl-C to stop following)
```

The API is now on `http://localhost:8000`. Jump to
[Drive a ticket end-to-end](#drive-a-ticket-end-to-end). When you're done:

```bash
make down          # stop the stack
make stack-reset   # stop and wipe the Postgres volume
```

If port 8000 is taken, start with `API_PORT=8010 make up` and pass
`API_URL=http://localhost:8010` to the demo commands below.

### Option B — run processes locally (for development)

Run Postgres in Docker and each application process in its own shell so you can
edit and restart them individually:

```bash
make server          # Postgres only (foreground)

# each of these in a separate shell:
make api             # FastAPI on http://localhost:8000
make runner          # workflow driver
make agent_worker    # primary agent queue consumer
make fallback-worker # fallback queue consumer
make side-effect-worker
```

You need at least `api`, `runner`, and `agent_worker` for a ticket to reach
`resolved`; add `side-effect-worker` for refunds/replies and `fallback-worker`
to exercise schedule-to-start fallback.

> `make doctor` pings the API and prints the resolved config. Its
> `orchestration` line reports whether the durable graph compiled during API
> startup; use `make status ID=…` to observe real workflow progress.

### Drive a ticket end-to-end

With the stack up (Option A or B), the `make` targets wrap the HTTP API:

```bash
# Create a ticket (this example asks for a refund → routes to approval).
make ticket
# → {"ticket_id":"<id>"}

# Inspect its current state (re-run to watch it advance).
make status ID=<id>
# → received → classifying → drafting → awaiting_approval

# Approve (or reject) the held ticket.
make approve ID=<id>     # → resolved
make reject  ID=<id>     # → rejected
```

Equivalent raw calls (the contract is plain JSON):

```bash
curl -s -X POST http://localhost:8000/tickets \
  -H 'Content-Type: application/json' \
  -d '{"customer_email":"jo@example.com","subject":"refund please","body":"I was double charged."}'

curl -s http://localhost:8000/tickets/<id>
curl -s 'http://localhost:8000/tickets?status=awaiting_approval'   # list by status

curl -s -X POST http://localhost:8000/tickets/<id>/approval \
  -H 'Content-Type: application/json' \
  -d '{"approved":true,"approver":"you","note":"ok"}'
```

Submitting a second approval for the same ticket returns `409`. To create many
tickets at once:

```bash
make batch N=100         # POST N tickets concurrently, print a status histogram
make reset               # clear the Postgres read model between runs
```

## Fault-injection demos

These make the rebuilt distributed mechanisms visible by breaking them. The
saturation demo lowers the primary agent queue's rate limit and schedule-to-start
budget, creates a concurrent batch, and requires at least one ticket to route
through the unthrottled fallback queue:

```bash
make demo-saturation-fallback
```

Expected output includes a `model_paths` histogram with at least one fallback
path, for example `fallback/fallback` or `primary/fallback`. The defaults are
fast for local runs; override them when needed:

```bash
SATURATION_AGENT_MAX_PER_SECOND=0.25 \
SATURATION_SCHEDULE_TO_START_S=1 \
SATURATION_COUNT=8 \
SATURATION_TIMEOUT=90 \
make demo-saturation-fallback
```

The other three DDIA demos (worker-kill redelivery, duplicate-refund
at-most-once, approval edges) live in the integration test suite — see `make
integration` below and `plan.md` Milestone 8.

## Tracing

OpenTelemetry tracing is off by default. Enable it with
`TICKETFLOW_TRACE_EXPORTER`:

- `none` (default): tracing disabled
- `console`: spans printed to stdout
- `otlp`: spans exported over OTLP HTTP to `TICKETFLOW_OTLP_ENDPOINT`
  (default `http://localhost:4318/v1/traces`)

For the Docker stack, enable the exporter and the `tracing` profile (adds Jaeger
at `http://localhost:16686`):

```bash
TICKETFLOW_TRACE_EXPORTER=otlp docker compose --profile tracing up --build
```

## Tests

```bash
make check        # format-check + lint + pyright + unit tests
make test         # unit tests only (no infrastructure needed)
make integration  # tests against real Postgres
make smoke        # end-to-end tests against the Docker stack
make coverage
```

`make test` excludes the `smoke` and `integration` markers, so it runs with no
Docker or Postgres. Those markers cover the highest-value behavior (worker
redelivery, at-most-once refunds, real checkpointer semantics) and are run
explicitly:

- `make integration` runs the Postgres integration tests. By default the pytest
  fixture starts a session-scoped `postgres:17-alpine` Testcontainers database
  and creates an isolated schema per test. To reuse an existing local or Compose
  database instead, set `TEST_DATABASE_URL`:

  ```bash
  TEST_DATABASE_URL=postgresql://ticketflow:ticketflow@localhost:5432/ticketflow make integration
  ```

- `make smoke` starts the Docker stack, waits for the API, runs the smoke tests,
  and tears the stack down. Use `API_PORT=8010 make smoke` if another local
  service already owns port 8000.

`make install` also installs a pre-push hook that runs `make check` before a
branch is pushed.
