# TicketFlow

TicketFlow is migrating toward a LangGraph support-ticket workflow backed by
Postgres. The current branch implements Milestone 1: dependency and
infrastructure scaffolding plus the durable Postgres-backed task queue. Nothing
orchestrates tickets yet.

Migration plan: `plan.md`

## Current Milestone

Milestone 1 provides:

- Postgres in Docker Compose.
- LangGraph, LangGraph Postgres checkpoint, and psycopg dependencies.
- `DATABASE_URL` configuration.
- A small psycopg connection-pool and bootstrap helper.
- A durable `task_queue` table with idempotent enqueue, leased dequeue,
  completion, retry/fail handling, and expired-lease reclaim helpers.
- Import-clean API and worker placeholders.

The ticket API contract is still present, but orchestration endpoints return
`503` with `LangGraph/Postgres orchestration is not wired yet.` until later
milestones add the graph, runner, and workers.

## Run It

Prerequisites:

- [uv](https://docs.astral.sh/uv/)
- Docker, or a local Postgres instance reachable through `DATABASE_URL`

Install dependencies:

```bash
make install
```

Start Postgres:

```bash
make server
```

Run the API:

```bash
make api
```

Run the workflow runner and agent worker (each in its own shell):

```bash
make runner
make agent_worker
```

Check readiness:

```bash
make doctor
```

In Milestone 1, `/health` reports the API process is alive and `/ready` reports
the stack as degraded because orchestration is intentionally unavailable.

## Docker

Run the Milestone 1 stack:

```bash
make up
make logs
make down
make stack-reset
```

The Compose stack includes Postgres, the API, the workflow `runner`, and the
`agent_worker`, `fallback-worker`, and `side-effect-worker` processes.

## Fault Injection Demos

Milestone 8 demos make the rebuilt distributed mechanisms visible. The
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

## Tracing

OpenTelemetry tracing is off by default. Enable it with
`TICKETFLOW_TRACE_EXPORTER`:

- `none` (default): tracing disabled
- `console`: spans printed to stdout
- `otlp`: spans exported over OTLP HTTP to `TICKETFLOW_OTLP_ENDPOINT`
  (default `http://localhost:4318/v1/traces`)

For the Docker stack, enable the exporter and tracing profile:

```bash
TICKETFLOW_TRACE_EXPORTER=otlp docker compose --profile tracing up --build
```

## Tests

```bash
make check
make test
make integration
make coverage
make smoke
```

`make integration` runs the Postgres integration tests. By default the pytest
fixture starts a session-scoped `postgres:17-alpine` Testcontainers database and
creates an isolated schema per test. To reuse an existing local or Compose
database instead, set `TEST_DATABASE_URL`:

```bash
TEST_DATABASE_URL=postgresql://ticketflow:ticketflow@localhost:5432/ticketflow make integration
```

`make smoke` starts the Docker stack, waits for the API, runs the smoke tests,
and tears the stack down. Use `API_PORT=8010 make smoke` if another local
service already owns port 8000.

`make check` runs Ruff formatting check, Ruff linting, Pyright, and the normal
test suite. `make install` also installs a pre-push hook that runs `make check`
before a branch is pushed.
