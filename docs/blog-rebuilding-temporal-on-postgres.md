# Rebuilding Temporal's durable execution by hand on Postgres

> **Writing scaffold — not a draft.** Each section below is a launchpad: the key points to
> make and the exact code to read first. Read the source, form your own understanding, then
> write the prose and pull your own snippets. Line numbers are pointers, not quotes — confirm
> them as you go (code moves).

**Audience:** backend / distributed-systems engineers.
**Angle:** "I already wrote about the Temporal version. This is the same system rebuilt by hand
on Postgres — here is every primitive and the SQL behind it."
**The four primitives to land:** durable execution · retry · idempotency · fallback.

---

## 1. Why rebuild Temporal by hand

Key points to make (in your words):
- Temporal hands you two things at once: durable workflows *and* a distributed task queue. That
  bundling is exactly what hides the primitives.
- Rebuilding both on plain Postgres makes each primitive legible — you can point at the row.
- This is a learning exercise; the goal is understanding, not replacing Temporal.

Code/notes to study:
- `plan.md` lines 6, 13 — the stated goal ("durable workflows *and* distributed task queues",
  "Faithful distributed — separate worker processes poll a hand-built, Postgres-backed queue").

Guiding question:
- What did Temporal do for you automatically that you now have to *name* and implement? List them
  before reading on — sections 3–6 are the answers.

---

## 2. The concept map (Temporal → hand-built)

Build this table yourself from `plan.md` lines 24–34. Fill the right column from the code you read
in later sections. Stub:

| Temporal primitive | Hand-built equivalent | Where it lives |
| --- | --- | --- |
| `await activity` / `await approval` | LangGraph `interrupt()` + resume with `Command(resume=…)` | `graph.py` / `runner.py` |
| Activity task queue | `task_queue` table drained `FOR UPDATE SKIP LOCKED` | `db.py`, `taskqueue.py` |
| Durable timer | `workflow_run.wakeup_at` + runner resumes when due | `db.py`, `runner.py` |
| Idempotent enqueue | `idempotency_key` UNIQUE + `ON CONFLICT DO NOTHING` | `taskqueue.py` |
| `schedule_to_start_timeout` | `wakeup_at` timeout → redispatch to fallback queue | `graph.py` |
| Worker lease / heartbeat | `lease_owner` + `lease_expires_at` + janitor reclaim | `db.py`, `runner.py` |

Guiding question:
- For each row: what breaks if that primitive is missing? (That's your "why it exists" sentence.)

---

## 3. Durable execution: the workflow is a checkpointed state machine

Key points to make:
- The workflow is a LangGraph `StateGraph`. State is a `TicketState` TypedDict — that dict *is* the
  durable execution state (everything needed to resume lives in it).
- Pattern is **dispatch → await**: a dispatch node enqueues a task and records a `wakeup_at`; an
  await node calls `interrupt()` and suspends. No thread sleeps.
- The `workflow_run` table is the durable run record + lease. The runner loop is the engine:
  claim a run → is its result/signal/timer ready? → resume the graph → save (which releases the
  lease) **atomically**.
- Crash safety = leases. A runner that dies holds a lease that expires; a janitor reclaims it.
  Leases are measured in Postgres server time, not the worker's clock (so clock skew can't
  mis-expire a lease).

Code to study:
- `src/ticketflow/graph.py:91` — `TicketState` (what is durable).
- `src/ticketflow/graph.py:248` `dispatch_classify`, `:258` `await_classify`, `:221` the
  `interrupt()` envelope — the dispatch/await pair.
- `src/ticketflow/db.py:638` — `workflow_run` table DDL (`status`, `wakeup_at`, `lease_owner`,
  `lease_expires_at`, status CHECK constraint).
- `src/ticketflow/runner.py:219` `step()` and `:282` `run_forever()` — the loop.
- `src/ticketflow/db.py:229` `claim_run`, `:307` `save_run` — lease acquire / atomic release.
- `src/ticketflow/db.py:280` `reclaim_expired_runs` + `runner.py:266` `reclaim_expired_leases` —
  the janitor. (Postgres-time leases: see commit `bf21c13`.)
- `src/ticketflow/db.py:399` `wake_run` — how a finished task pulls `wakeup_at` to now so the
  runner picks the run up immediately.

Guiding questions:
- Where exactly is the "checkpoint"? (Hint: LangGraph's saver vs the `workflow_run` projection —
  which is source of truth, which is for visibility/leasing?)
- Why must status update + lease release happen in one transaction (`save_run`)? What race opens
  if they don't?

---

## 4. Retry & backoff as table state (the task queue)

Key points to make:
- The queue is one table. Retry, backoff, and attempt-counting are **columns and SQL**, not worker
  code or framework magic.
- `dequeue` leases a row with `FOR UPDATE SKIP LOCKED` (concurrent workers don't block each other),
  bumps `attempts`, sets a lease.
- Backoff is computed in SQL: `now() + interval '1 second' * power(2, attempts)`.
- Two failure classes: transient (`AgentOverloadedError`) → goes back to `pending` and retries;
  permanent (`AgentPermanentError`) → straight to `failed`, no retry.
- Exhausted retries → task `failed` → runner resumes the graph with `{"kind": "task_failed"}` →
  the graph escalates the ticket.
- Crash safety: a leased task whose lease expires is reclaimed to `pending` **with `attempts`
  preserved** — a crashed worker's attempt still counts toward `max_attempts`.

Code to study:
- `src/ticketflow/db.py:565` — `task_queue` DDL (`attempts`, `max_attempts`, `available_at`,
  `lease_owner`, `lease_expires_at`, `permanent`, status CHECK).
- `src/ticketflow/db.py:178` `dequeue` — the `FOR UPDATE SKIP LOCKED` lease.
- `src/ticketflow/taskqueue.py:162` `fail` (backoff CASE at `:181`), `:138` `complete`,
  `:198` `reclaim_expired`.
- `src/ticketflow/agent_worker.py:79` `process_one_task`, `:103` the `AgentPermanentError` branch,
  `:74` the `fail(..., permanent=...)` call.
- `src/ticketflow/graph.py:69` `_is_task_failed`, `:258`–`:274` escalation on task failure.
- `docs/context.md:103` — "Manual retry loop for agent activities" (rationale).

Guiding questions:
- Why compute backoff in SQL instead of in the worker? (Think: what happens to in-flight backoff
  state if the worker restarts.)
- `attempts` is incremented in `dequeue`, before the work runs. Why there and not on failure?

---

## 5. Idempotency: two layers

Key points to make:
- **Layer 1 — enqueue dedup.** Every task carries an `idempotency_key` (`{ticket_id}:{step}`,
  e.g. `:classify`, `:draft`, `:finalize`). It's UNIQUE; enqueue is `ON CONFLICT DO NOTHING`.
  Duplicate dispatches silently no-op. This makes *at-least-once* dispatch safe.
- **Layer 2 — at-most-once effect (the refund ledger).** Delivery is at-least-once, but a refund
  must happen once. Two tables: `refund_attempts` (one row per delivery — the observable log) and
  `refunds` (PK `ticket_id`, `ON CONFLICT DO NOTHING` — the actual effect, recorded once).
- The function returns "was this the first refund?" so a retry after a crash-between-effect-and-ack
  is a safe no-op, and you can still see every attempt.
- Same idea for the terminal result: an idempotent upsert keyed by `ticket_id`.

Code to study:
- `src/ticketflow/taskqueue.py:11` `enqueue` — the `ON CONFLICT (idempotency_key) DO NOTHING`.
- Key construction: `src/ticketflow/graph.py:162` (`enqueue_agent_task`, `{workflow_id}:{task_type}`)
  and `:394` (the `finalize_ticket` enqueue).
- `src/ticketflow/ledger.py:6` `record_refund` — attempt log (`:14`) + idempotent effect (`:19`–`21`).
- `src/ticketflow/activities.py:39` `execute_refund` (calls into the ledger), `:62` `record_result`.
- `docs/context.md:147` — "Idempotency ledger for refunds (DDIA ch. 7–8)".

Guiding questions:
- Why split `refund_attempts` from `refunds` instead of one table? What does each answer that the
  other can't?
- Where is the exactly-once boundary actually enforced — in the queue, the worker, or the DB
  constraint? (This is the punchline of the section.)

---

## 6. Fallback: schedule-to-start as queue routing

Key points to make:
- Two queues: primary `ticketflow-agent` is **rate-limited** (token bucket); fallback
  `ticketflow-agent-fallback` is **unthrottled**.
- Temporal's `schedule_to_start_timeout` becomes a durable timer: dispatch arms
  `wakeup_at = now() + AGENT_SCHEDULE_TO_START_S` (30s). If the task hasn't *started* by then, the
  primary queue is saturated.
- On timeout the runner resumes the graph with `{"kind": "timeout"}`. The await node redispatches:
  `cancel_pending` marks the original task permanently failed, then it re-enqueues on the fallback
  queue under `{key}:fallback`.
- The fallback agent is **deliberately worse** — lower confidence `(0.0, 0.6)` — so degraded
  answers fall below the approval threshold and surface in the human approval inbox. Degradation
  is *observable*, not silent.
- Contrast with a circuit breaker: this isn't "stop calling" — it's "route to a slower, dumber,
  always-available tier and flag it."

Code to study:
- `src/ticketflow/config.py:13`–`19` — queue names, `AGENT_MAX_PER_SECOND`,
  `AGENT_MAX_CONCURRENT`, `AGENT_SCHEDULE_TO_START_S`.
- `src/ticketflow/agent_worker.py:25` `TokenBucket` (the rate limit); `fallback_worker.py` (same
  loop, `max_per_second=None`).
- `src/ticketflow/graph.py:173` (arming `wakeup_at`), `:211` `await_agent_task`, `:230` the
  `_is_timeout` branch, `:175` `redispatch_agent_task`.
- `src/ticketflow/taskqueue.py:105` `cancel_pending`.
- `src/ticketflow/agent/mock.py:69` `MockAgent.fallback` (confidence `(0.0, 0.6)` at `:76`).
- Approval gate: `src/ticketflow/graph.py:322`–`326` (`needs_approval`, `CONFIDENCE_THRESHOLD`).
- `docs/context.md:84` — "Fallback model via `schedule_to_start_timeout`".

Guiding questions:
- Why give the fallback task a *different* idempotency key (`{key}:fallback`) instead of reusing
  the original? What would collide otherwise?
- Schedule-to-start measures queue *wait*, not execution time. Why is wait the right signal for
  "fall back"?

---

## 7. The split-queue bulkhead (short section)

Key points to make:
- Finalize / side-effects (`send_reply`, `execute_refund`, `record_result`) run **unthrottled on
  their own queue**, while the LLM tier is throttled.
- Reason: the LLM is the scarce, failure-prone dependency. Isolating it means the agent tier can
  saturate or die while tickets already past the LLM still finalize. Bulkhead / backpressure.
- The side-effect worker is unthrottled on *rate* but still bounded on *concurrency* — and it never
  touches the LLM, so it can't eat the agent's rate budget.

Code to study:
- `src/ticketflow/side_effect_worker.py` (the `finalize_ticket` consumer; `max_per_second=None`).
- `docs/context.md:65` — "Split task queues: workflow worker vs LLM worker" (the rationale, incl.
  the two rate-limit knobs: server-side vendor budget vs worker-side host capacity).

Guiding question:
- Which resource does each queue's limit actually protect? (Name the scarce thing per queue —
  that's why they're separate.)

---

## 8. What this taught about durable execution

Key points to land (closing):
- Durable orchestration = a logged state machine + leases + idempotency keys. No magic.
- Every Temporal "feature" maps to a row, a constraint, or a janitor query.
- Plain Postgres is enough to get exactly-once *effects* on top of at-least-once *delivery* — the
  trick is always pushing the dedup down to a DB constraint.
- What you'd still miss vs real Temporal (be honest): history/replay determinism, versioning,
  visibility tooling, scale. (See `docs/context.md` open follow-ups around line 187.)

Guiding question:
- If you had to give up exactly one of {leases, idempotency keys, durable timers}, which failure
  would hurt most — and what would you see in production when it broke?

---

### Source index (verify line numbers as you write)

- Workflow graph: `src/ticketflow/graph.py`
- Runner loop: `src/ticketflow/runner.py`
- Tables + leasing: `src/ticketflow/db.py`
- Queue ops: `src/ticketflow/taskqueue.py`
- Refund idempotency: `src/ticketflow/ledger.py`, `src/ticketflow/activities.py`
- Fallback + workers: `src/ticketflow/agent_worker.py`, `fallback_worker.py`,
  `side_effect_worker.py`, `config.py`, `agent/mock.py`
- Rationale / decision log: `docs/context.md`, `plan.md`
