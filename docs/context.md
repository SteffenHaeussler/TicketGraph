# Ticketflow Decision Log

How the project's decisions were made, in the order they happened. Each entry
records the decision, why it won, what lost, where it lives in the code, and what
it taught. This file replaces the old `plan.md` task list: everything planned
there is implemented; the reasoning is preserved here.

Companion docs: `docs/superpowers/specs/2026-06-10-ticketflow-design.md` (the
original design brainstorm), `docs/distributed-programming-arc.md` (the
DDIA-flavored narrative), `docs/agent-activity-ops.md` (LLM worker operations).

---

## One workflow per ticket, workflow ID = ticket ID

- **Decision:** Each ticket runs as its own `TicketWorkflow` with workflow ID
  `ticket-<id>` (PR #10).
- **Why:** Small failure domain (one stuck ticket cannot block others) and the
  ticket ID becomes the natural idempotency and lookup key — duplicate
  `POST /tickets` starts surface as `WorkflowAlreadyStartedError` → HTTP 409.
- **Alternatives considered:** Parent/child saga and a dynamic agent loop were
  explicitly deferred in the design spec as future iterations.
- **Where:** `src/ticketflow/api.py` (`create_ticket`), `src/ticketflow/workflows.py`.
- **Taught:** Idempotency is cheapest when the identifier is chosen at the edge
  and reused everywhere downstream.

## Mock agent behind an `Agent` protocol, seeded randomness

- **Decision:** A `MockAgent` with seedable RNG implements an `Agent` protocol;
  failure rate, refund rate, latency, and confidence are constructor knobs
  (PRs #4, #6, #7).
- **Why:** The project is about distributed behavior, not prompt engineering. A
  seeded mock makes retries, approvals, and fallbacks reproducible in tests while
  keeping the swap-in point for a real LLM obvious.
- **Alternatives considered:** Calling a real LLM API from day one — rejected:
  nondeterministic tests, cost, and no control over failure injection.
- **Where:** `src/ticketflow/agent/base.py`, `src/ticketflow/agent/mock.py`.
- **Taught:** Injecting failures deliberately (10% `AgentOverloadedError`) is what
  makes retry policies observable instead of theoretical.

## Approval as a workflow update with validator, not a signal

- **Decision:** `submit_approval` is a workflow **update**; a validator rejects it
  unless the ticket is `AWAITING_APPROVAL` and undecided (PR #21, originally a
  signal).
- **Why:** Updates give the caller a synchronous result (the resulting status) or
  a rejection that maps cleanly to HTTP 409. Signals are fire-and-forget: the
  approver could not know whether their decision landed, was duplicate, or late.
- **Where:** `src/ticketflow/workflows.py` (`submit_approval`,
  `validate_submit_approval`), `src/ticketflow/api.py` (409 mapping).
- **Taught:** Validation must run against current workflow state *before* the
  event is accepted into history; that is what a validator is for.

## Close the approval-timeout race by setting terminal status first

- **Decision:** `_finish` sets the terminal status *before* running the final
  activities (refund, reply, record) so the update validator rejects approvals
  that arrive while the escalation/resolution is still finishing (PR #35).
- **Why:** Between "timeout fired" and "workflow closed" there is a window where
  a late approval would otherwise be accepted and then silently lost.
- **Where:** `src/ticketflow/workflows.py` (`_finish`, comment at the top).
- **Taught:** Races in workflow code are ordering bugs in one logical thread —
  fixable by sequencing state transitions, not by locks.

## Split task queues: workflow worker vs LLM worker

- **Decision:** `TicketWorkflow` and fast side effects stay on the `ticketflow`
  queue (`worker.py`); `classify_ticket`/`draft_reply` run on `ticketflow-agent`,
  served by a dedicated `llm_worker.py` process (PRs #40, #42).
- **Why:** Real LLM backends are the scarce, rate-limited dependency. A separate
  queue lets the agent tier saturate or die while tickets keep progressing
  through replies, refunds, and record-keeping.
- **Alternatives considered:** A message broker between worker and agent —
  rejected: Temporal's task queue *is* the queue; the realism comes from capacity
  tuning, not extra infrastructure.
- **Where:** `src/ticketflow/worker.py`, `src/ticketflow/llm_worker.py`,
  `src/ticketflow/config.py`.
- **Taught:** The two production rate-limit knobs differ in scope:
  `max_task_queue_activities_per_second` is server-side — the *vendor's* budget,
  enforced across all workers, so scaling workers never exceeds it.
  `max_concurrent_activities` is worker-side — the *host's* capacity, tuned per
  process.

## Fallback model via `schedule_to_start_timeout`

- **Decision:** Agent activities run on the primary queue with a 30s
  schedule-to-start budget; when the task waits too long to start, the workflow
  catches the `SCHEDULE_TO_START` timeout and reruns the activity on an
  unthrottled fallback queue served by a faster, lower-confidence mock (PR #40,
  tuned in #43).
- **Why:** Real LLM stacks fall back to a cheaper model when the primary cannot
  respond in time; the Temporal-shaped definition of "in time" is queue wait.
  Capping fallback confidence at 0.6 makes the cost visible: fallback tickets
  land in the human approval inbox.
- **Alternatives considered:** Provider registries / config-driven fallback
  chains — deliberately not built; two agents, no abstraction.
- **Where:** `src/ticketflow/workflows.py` (`_execute_agent_activity`,
  `_execute_fallback_agent_activity`), `src/ticketflow/agent/mock.py`
  (`MockAgent.fallback`).
- **Taught:** Degraded service beats an outage, and the degradation should be
  measurable downstream (`model_path` in results, batch histograms).

## Manual retry loop for agent activities

- **Decision:** `_execute_agent_activity` uses `maximum_attempts=1` on the
  activity plus its own retry loop with `workflow.sleep` backoff, instead of
  letting Temporal's retry policy run the attempts.
- **Why:** Server-side retries always go back to the *same* queue. To reroute to
  the fallback queue, the workflow must observe the `SCHEDULE_TO_START` timeout
  itself — which means owning the retry loop for the primary path too.
- **Trade-off (accepted):** Retry state moves into workflow code and history
  (each attempt is a separate activity plus a timer). This is the leaked
  abstraction the blog post should be honest about.
- **Where:** `src/ticketflow/workflows.py:216-246`.

## Two read paths: search attribute + SQLite read model

- **Decision:** Live state is exposed two ways: a `TicketStatus` search attribute
  upserted on every transition (visibility queries, approval inbox) and a SQLite
  read model written once at terminal state (PRs #10, #26).
- **Why:** They answer different questions. "Which tickets await approval right
  now?" is a visibility-store query across live workflows. "What happened to
  ticket X?" must outlive Temporal's retention and survive worker downtime, so
  `GET /tickets/{id}` falls back from the live query to SQLite.
- **Where:** `src/ticketflow/workflows.py` (`_set_status`),
  `src/ticketflow/readmodel.py`, `src/ticketflow/api.py` (`get_ticket` fallback
  chain, `list_tickets`).
- **Taught:** DDIA's derived-data theme in miniature: keep the system of record
  (workflow history) optimized for execution and audit, and derive a small view
  optimized for reads.

## Payload schema evolution rule (DDIA ch. 4)

- **Decision:** Models that cross the Temporal wire only ever gain *defaulted*
  fields (e.g. `model: str = "primary"`); never required additions, removals, or
  renames (PR #39, provoked deliberately first).
- **Why:** Payloads outlive code: replay decodes history recorded under old
  schemas with today's models. A required-field addition put a real workflow into
  a `WorkflowTaskFailed` loop — still "Running", but unable to accept its
  approval.
- **Where:** `src/ticketflow/models.py`; regression tests in
  `tests/test_models.py` (`...for_old_payloads`).
- **Taught:** Required additions break backward compatibility with old histories;
  removals/renames break forward compatibility with old senders; defaults buy
  both.

## Idempotency ledger for refunds (DDIA ch. 7–8)

- **Decision:** `execute_refund` records every delivery in `refund_attempts` but
  the refund itself at most once in `refunds`, keyed by ticket ID (PR #41,
  demonstrated with a crash injected between side effect and ack).
- **Why:** Activities are at-least-once. "Exactly-once" is a sum: at-least-once
  delivery plus idempotent effects. Keeping the attempt log separate from the
  effect makes the duplicate delivery *observable* instead of silently absorbed.
- **Where:** `src/ticketflow/activities.py` (`execute_refund`),
  `src/ticketflow/readmodel.py` (`record_refund`).
- **Taught:** The idempotency key must live in the same transactional store as
  the effect — and a plain `INSERT` + counter would have double-counted.

## Operability: readiness, doctor, preflight

- **Decision:** `/ready` checks Temporal health plus pollers on all three task
  queues; `scripts/doctor.py` turns that into actionable CLI output; the batch
  script preflights the namespace and search attribute (PRs #25, #31, #32, #38).
- **Why:** The stack is four processes; the most common local failure is "one of
  them isn't running", and every probe should name the `make` target that fixes
  it.
- **Where:** `src/ticketflow/api.py` (`/ready`), `scripts/doctor.py`,
  `scripts/batch.py` (`check_temporal_setup`).

## Deployment smoke test as an opt-out pytest marker

- **Decision:** End-to-end tests against the docker stack live in
  `tests/test_smoke_stack.py` behind a `smoke` marker that pytest deselects by
  default (`addopts = "-m 'not smoke'"`); `make smoke` starts the stack, waits
  for the API, runs the tests, and tears the stack down.
- **Why:** `make test` must stay runnable without Docker (CI, quick local
  loops), but "the compose stack actually processes a ticket" is exactly the
  failure mode unit tests can't catch. `make smoke` uses `scripts/wait_for_api.py`
  for a clear readiness wait before pytest runs.
- **Where:** `tests/test_smoke_stack.py`, `scripts/wait_for_api.py`,
  `pyproject.toml` (`[tool.pytest.ini_options]`), `Makefile`
  (`smoke`/`test-docker`).

---

## Open follow-ups

### Periodic activity heartbeats

**Why:** `classify_ticket`/`draft_reply` heartbeat only before and after the
agent call (`src/ticketflow/activities.py:24-31`). With
`heartbeat_timeout=30s`, a real LLM call taking longer than 30s would be killed
mid-flight; the pattern only works because the mock's latency caps at 3s.

**Steps:**
- [ ] Wrap the agent call in a background heartbeat loop (e.g. an asyncio task
      heartbeating every ~10s) and cancel it when the call returns.
- [ ] Keep tests instant (latency defaults to 0).

**Verify:**
- [ ] `make test`.
- [ ] Run with `MOCK_AGENT_LATENCY_MAX_S=45`: the activity survives its 30s
      heartbeat timeout instead of failing.

### Fallback escape hatch

**Why:** `_execute_fallback_agent_activity`
(`src/ticketflow/workflows.py:264`) sets no `schedule_to_start_timeout` and the
workflow has no run timeout. If the fallback worker is also down, the activity
task parks in the queue forever and the ticket hangs instead of escalating —
the last line of defense is the only path without one.

**Steps:**
- [ ] Add a `schedule_to_start_timeout` to the fallback activity options.
- [ ] Catch the `SCHEDULE_TO_START` timeout from the fallback path and escalate
      the ticket (reuse the existing escalation flow).
- [ ] Workflow test: no worker on either agent queue → ticket reaches
      `ESCALATED` instead of hanging (time-skipping makes this instant).

**Verify:**
- [ ] `make test`.
- [ ] Full stack with both LLM workers stopped: a new ticket escalates after the
      timeouts instead of staying `CLASSIFYING` forever.

---

## Deliberately out of scope

- Auth on the approval endpoint (anyone can approve refunds today).
- Real notification on ESCALATED — it is currently just a status + reply.
- Payment-provider idempotency for refunds (the code comment in
  `activities.py` marks the spot; the local ledger shows the shape).
- Production Temporal deployment (the compose stack runs `start-dev`; fine for
  learning).
