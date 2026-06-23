# TicketFlow: Temporal → LangGraph + hand-built Postgres task queue

A working checklist for migrating the orchestration backend incrementally. Each task is small
and self-contained — pick one up at a time. Check it off when its **Done** condition holds.

> **Goal beyond the migration:** Temporal hands you durable workflows *and* distributed task
> queues as one black box. We split it open and rebuild both halves by hand on Postgres, to see
> how each maps to *Designing Data-Intensive Applications* (DDIA). Every task is tagged with the
> DDIA concept it exercises.

## Locked decisions

- **Faithful distributed** — separate worker processes poll a hand-built, Postgres-backed queue.
- **Postgres** is the single store (queue + LangGraph checkpointer + read model).
- **Full replacement** of Temporal; **keep the external HTTP API contract identical**.
- **Never touch** `src/ticketflow/agent/base.py`, `src/ticketflow/agent/mock.py`,
  `src/ticketflow/models.py`. The `Agent` protocol boundary stays.

## Conceptual translation (reference)

| Temporal primitive | Hand-built replacement | DDIA |
|---|---|---|
| Durable workflow / history replay | LangGraph `StateGraph` + `PostgresSaver`, `thread_id = ticket id` | Ch 5/9 log-based replication |
| `await activity` / `await approval` | LangGraph `interrupt()` + resume with `Command(resume=…)` | durable execution |
| Activity task queues | `task_queue` table drained `FOR UPDATE SKIP LOCKED` by separate workers | Ch 11 DB-as-queue |
| At-least-once + crash recovery | Lease (`lease_owner` + `lease_expires_at`) + janitor reclaim | Ch 8 leases; Ch 11 at-least-once |
| `RetryPolicy(1s, ×2, max 5)` | `attempts` + `available_at = now()+backoff`; `failed` after max | Ch 8 retries |
| Non-retryable error | `permanent` flag / `max_attempts=1` | — |
| `schedule_to_start` → fallback | 30s timer; if still `pending`, re-dispatch to `-fallback` | Ch 11 backpressure routing |
| Rate limit + concurrency (10/s, 20) | Token bucket + lease-batch in primary worker | Ch 11 backpressure |
| Idempotent enqueue | `idempotency_key` UNIQUE (`{ticket_id}:{step}`) | Ch 11/12 dedup |
| `execute_refund` at-most-once | Refund ledger keyed by `ticket_id`, `ON CONFLICT DO NOTHING` | Ch 12 exactly-once effects |
| Enqueue + state in one step | Outbox: task row written in the checkpoint transaction | Ch 11 outbox |
| Durable timers (24h, 30s) | `workflow_run.wakeup_at`, runner resumes when due | durable timers |
| Visibility search attribute | `workflow_run.status` secondary index, queried by API | Ch 3 secondary indexes |
| Server re-schedules workflow tasks | Runner pool leases `workflow_run` rows, advances the graph | Ch 8 work distribution |

---

## Milestone 0 — Infra & scaffolding
*Get Postgres and dependencies in place; nothing orchestrates yet.*

- [x] **0.1 Compose: Postgres in, Temporal out.** Replace `temporal` + `temporal-init` services
  with a `postgres` service (volume, healthcheck). Keep API/worker service stubs (commands updated
  later). _Done:_ `docker compose up postgres` is healthy.
- [x] **0.2 Dependencies.** Remove `temporalio`; add `langgraph`, `langgraph-checkpoint-postgres`,
  `psycopg[binary,pool]`. _Done:_ `make install` resolves; `import langgraph` works.
- [x] **0.3 Config.** `config.py`: add `DATABASE_URL`; keep queue names + rate-limit knobs
  (`AGENT_MAX_PER_SECOND`, `AGENT_MAX_CONCURRENT`, `AGENT_SCHEDULE_TO_START_S`); drop `TEMPORAL_*`.
  _Done:_ `test_config.py` updated and green.
- [x] **0.4 DB module.** New `db.py`: psycopg connection pool + a `bootstrap()`/migration runner
  that creates tables. _Done:_ `bootstrap()` is idempotent (safe to run twice).

## Milestone 1 — The durable task queue (the core artifact)
*DDIA: Ch 11 (DB-as-queue, at-least-once, outbox), Ch 8 (leases, retries).*

- [x] **1.1 Schema.** `task_queue(id, queue_name, task_type, workflow_id, payload jsonb,
  idempotency_key UNIQUE, status, attempts, max_attempts, available_at, enqueued_at, lease_owner,
  lease_expires_at, result jsonb, error, permanent)`. _Done:_ created by `bootstrap()`.
- [x] **1.2 `enqueue()`** with idempotency key (`ON CONFLICT (idempotency_key) DO NOTHING`).
  _Done:_ enqueuing the same key twice yields one row.
- [x] **1.3 `dequeue(queue_name, worker_id)`** via the `FOR UPDATE SKIP LOCKED` + lease update
  (see SQL below). _Done:_ returns a leased task or `None`.
- [x] **1.4 `complete()` / `fail()`.** complete → `done` + `result`. fail → if `attempts < max`
  and not `permanent`: `pending` with `available_at = now() + 1s·2^attempt`; else `failed` + error.
  _Done:_ backoff schedule observable in a test.
- [x] **1.5 `reclaim_expired()`** janitor: `status='leased' AND lease_expires_at < now()` → `pending`.
  _Done:_ a dropped lease becomes redeliverable.
- [x] **1.6 Queue unit tests.** Concurrent `dequeue` delivers each task once (SKIP LOCKED); lease
  expiry + reclaim; backoff schedule; enqueue idempotency. _Done:_ all green.

```sql
-- 1.3 dequeue
UPDATE task_queue SET status='leased', lease_owner=$1,
  lease_expires_at = now() + interval '30 seconds', attempts = attempts + 1
WHERE id = (
  SELECT id FROM task_queue
  WHERE queue_name=$2 AND status='pending' AND available_at <= now()
  ORDER BY available_at FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING *;
```

## Milestone 2 — Read model on Postgres
*DDIA: Ch 12 (idempotent / exactly-once effects).*

- [x] **2.1 Refund ledger.** Port `refunds(ticket_id PK, amount)` + `refund_attempts`; keep
  `record_refund` returning `True` only on first refund (`INSERT … ON CONFLICT DO NOTHING`).
- [x] **2.2 Results store.** Port `ticket_results(ticket_id PK, data jsonb)` (`save_result` upsert,
  `load_result`). _Done:_ terminal ticket results persist through Postgres JSONB and tests cover
  upsert/load/clear behavior.
- [x] **2.3 Tests.** Retarget `test_readmodel.py` to Postgres; assert at-most-once refund semantics.

## Milestone 3 — LangGraph workflow graph
*DDIA: durable execution as a logged state machine.*

- [x] **3.1 Graph skeleton (inline).** `graph.py`: `TicketState` + `StateGraph`
  (`classify → draft → decide_approval → execute → record`) with `PostgresSaver`. Nodes call the
  agent **inline** for now (no queue) to prove the graph runs and checkpoints. _Done:_ a ticket
  reaches `resolved`; state survives a fresh process via the checkpointer.
- [x] **3.2 Dispatch-and-interrupt.** Convert `classify`/`draft` to **enqueue a task + `interrupt()`**;
  resume with `Command(resume=<result>)`. _Done:_ graph suspends at dispatch, resumes on result.
- [x] **3.3 Approval gate.** `decide_approval` rule (`action==REFUND` OR `confidence<0.75`);
  `await_approval` sets `wakeup_at=now()+24h` and `interrupt()`s; resume by decision or timer.
- [x] **3.4 Schedule-to-start fallback.** Primary dispatch sets `wakeup_at=now()+30s`; if the task
  is still `pending` when the timer fires, re-dispatch to `ticketflow-agent-fallback`.
- [x] **3.5 Terminal nodes.** `execute` (refund via ledger + `send_reply`) and `record`; preserve
  `REJECTION_REPLY` / `ESCALATION_REPLY` text and `CONFIDENCE_THRESHOLD=0.75`.

## Milestone 4 — Runner (the workflow driver)
*DDIA: Ch 8 work distribution; outbox commit.*

- [x] **4.1 `workflow_run` table** `(ticket_id PK, status, wakeup_at, lease_owner,
  lease_expires_at, …)` + a claim/lease helper.
- [x] **4.2 Runner loop.** `runner.py`: lease a runnable run (result/signal ready OR `wakeup_at`
  due), resume its graph, persist **checkpoint + state + outbox tasks in one transaction**, release.
- [x] **4.3 Timers.** Resume runs whose `wakeup_at <= now()` with a "timeout" input (drives 24h
  approval expiry and 30s fallback).
- [ ] **4.4 Signals.** `pending_signal(workflow_id, kind, payload, consumed)`; deliver approval
  decisions into the resumed graph; mark consumed.
- [x] **4.5 Janitor wired in.** Periodically call `reclaim_expired()` for tasks and runs.

## Milestone 5 — Workers
*DDIA: Ch 11 backpressure & rate limiting.*

- [ ] **5.1 Primary agent worker.** `agent_worker.py` consumes `ticketflow-agent` with a token
  bucket (`AGENT_MAX_PER_SECOND`) + bounded concurrency (`AGENT_MAX_CONCURRENT`), calls
  `TicketActivities` → `MockAgent`, writes result, wakes the run.
- [x] **5.2 Fallback worker.** Unthrottled consumer of `ticketflow-agent-fallback`.
- [x] **5.3 Side-effect worker.** Default-queue consumer for `send_reply` / `execute_refund` /
  `record_result` with retries.
- [x] **5.4 Permanent failures.** Map `AgentPermanentError` → `permanent=True` task result
  (non-retryable; workflow escalates).

## Milestone 6 — API rewire (contract unchanged)

- [x] **6.1 `POST /tickets`** inserts a `workflow_run` (and initial outbox); returns same response.
- [x] **6.2 `GET /tickets/{id}`** reads graph state from checkpoint with read-model fallback.
- [x] **6.3 `POST /tickets/{id}/approval`** writes a signal; returns **409** if not awaiting approval.
- [x] **6.4 `GET /tickets?status=`** queries the `workflow_run.status` secondary index.

## Milestone 7 — Tests & tooling

- [x] **7.1 Postgres test fixture** (testcontainers or `pytest-postgresql`, isolated schema/test).
- [ ] **7.2 Injectable clock** so 24h approval + 30s schedule-to-start fire deterministically.
- [x] **7.3 Drive-until-quiescent helper** (in-process runner+worker steps), analogous to today's
  `make_worker`.
- [ ] **7.4 Retarget workflow tests:** happy path, fallback-on-timeout, transient retries succeed,
  exhausted retries → escalate, permanent error → no retry, refund idempotency.
- [ ] **7.5 Retarget API tests:** create, list-by-status, status, approval, duplicate approval 409,
  late approval timeout → escalate.
- [ ] **7.6 Makefile + docs:** rename `worker`/`llm-worker` targets to `runner`/`agent_worker`;
  update README run instructions. (Keep `test_mock_agent.py` & `test_models.py` unchanged.)

## Milestone 8 — DDIA fault-injection demos (the payoff)
*Make the rebuilt mechanisms visible by breaking them.*

- [ ] **8.1 Worker-kill redelivery.** Kill an agent worker mid-lease → janitor reclaims → task
  redelivered → workflow still completes. (at-least-once + crash recovery)
- [ ] **8.2 Saturation → fallback.** Drop `AGENT_MAX_PER_SECOND` low → tasks exceed 30s
  schedule-to-start → auto-routed to fallback queue. (backpressure)
- [ ] **8.3 Duplicate refund.** Force duplicate delivery of `execute_refund` → ledger keeps it
  at-most-once. (idempotent effect)
- [ ] **8.4 Approval edges.** Submit approval twice → second is 409; let approval lapse 24h (fast
  clock) → escalation. (durable timer + idempotent signal)

---

## Dependency notes

- **M1 is the keystone** — M3.2+, M4, M5 all build on the queue. Do M0 → M1 first.
- M2 can be done any time after M0.
- M3.1 (inline) lets you validate LangGraph + checkpointer before the queue wiring of M3.2.
- M8 demos require M4 + M5 running end-to-end.

## Open item to confirm at implementation time

Exact LangGraph API surface (`interrupt` / `Command` / `PostgresSaver` / per-node `RetryPolicy`)
against the installed version — the design above is conceptual, not API-final.

## Verification (overall)

- `make test` green, including the new `taskqueue` unit tests.
- E2E: bring up Postgres + `runner` + `agent_worker` + `api`; `POST /tickets`; watch
  `received → classifying → drafting → resolved`; a refund/low-confidence ticket stops at
  `awaiting_approval` until `POST /approval`.
- The four M8 demos behave as described.
