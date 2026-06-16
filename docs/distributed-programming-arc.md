# Ticketflow Distributed Programming Learning Arc

Ticketflow is a small service for learning how distributed programs behave when
work takes time, fails intermittently, needs human input, and must still produce
an auditable result. It models a support-ticket resolver: an HTTP request starts
a ticket, a worker classifies it, a mock agent drafts a reply, risky or
low-confidence answers wait for approval, and the workflow eventually resolves,
rejects, or escalates the ticket.

If you are reading *Designing Data-Intensive Applications*, treat this project
as a concrete lab for reliability, derived data, asynchronous processing,
idempotent side effects, and observability.

## The Problem This Service Solves

A support ticket is not a good fit for a single request/response handler. The
system may need to call an unreliable agent backend, retry transient failures,
wait up to 24 hours for a human approval, execute a refund, send a reply, and
answer status checks while all of that is happening.

Ticketflow uses Temporal because that shape of work needs durable orchestration.
The important state is not just in a database row; it is the sequence of
decisions and side effects that move one ticket from `received` to a terminal
status.

## One Mental Model

The service has three runtime roles:

- FastAPI in `src/ticketflow/api.py` is the edge. It accepts HTTP requests,
  starts workflows, queries live status, submits approval updates, and falls
  back to the read model when live workflow queries are unavailable.
- Temporal Server is the durable coordinator. It stores workflow history,
  schedules workflow and activity tasks, owns timers, tracks workflow IDs, and
  exposes visibility queries.
- The worker in `src/ticketflow/worker.py` runs application code. It polls the
  `ticketflow` task queue and hosts both `TicketWorkflow` and
  `TicketActivities`.

That split is the core distributed-programming lesson: the API process can
return quickly, the worker can restart, and Temporal still knows what each
ticket is waiting for.

## Request Lifecycle

`POST /tickets` creates a `Ticket` model and calls
`Client.start_workflow(TicketWorkflow.run, ...)` with a workflow ID like
`ticket-<id>`. The HTTP handler does not classify, draft, refund, or send mail.
It only starts durable work and returns the ticket ID.

Inside `TicketWorkflow.run`, the workflow records status transitions and calls
activities:

1. `classify_ticket` asks the agent for a support category.
2. `draft_reply` asks the agent for reply text, confidence, and a proposed
   action.
3. If the draft proposes a refund or confidence is below `0.75`, the workflow
   waits for `submit_approval` for up to 24 hours.
4. Approved tickets execute a refund when needed and send the drafted reply.
   Rejected tickets send a fallback reply. Unanswered or failed tickets
   escalate.
5. `record_result` writes the terminal outcome to the SQLite read model.

`GET /tickets/{ticket_id}` first asks the workflow for live state through the
`status` query. `POST /tickets/{ticket_id}/approval` sends a workflow update,
not a best-effort message, so callers get a synchronous result or a conflict.

## Why Temporal Instead Of Plain Background Jobs

A basic job queue can run work later, but Ticketflow needs more than "run this
function eventually." It needs to remember where a ticket is between steps,
survive process restarts, expose current state, reject duplicate approvals, and
wait on a durable timer without keeping a Python process asleep for 24 hours.

Temporal gives the workflow an event history. The Python workflow code looks
like normal control flow, but Temporal can replay it from history after a worker
restart. That is why `TicketWorkflow` keeps state in fields such as `_status`,
`_classification`, `_draft`, and `_decision`: those fields are reconstructed
from the recorded workflow events.

The workflow is deliberately one workflow per ticket. That keeps the failure
domain small and makes the ticket ID the natural idempotency and lookup key.

## Activities Isolate Side Effects

Workflow code must be deterministic because Temporal may replay it. Calls to
the outside world belong in activities, implemented in
`src/ticketflow/activities.py`.

Ticketflow uses activities for agent calls, replies, refunds, and read-model
writes. That boundary matters:

- Agent calls can fail transiently and be retried with a backoff policy.
- Permanent agent errors are converted to non-retryable `ApplicationError`s.
- Refund execution is idempotent by ticket ID: attempts are logged in a
  `refund_attempts` table, while the refund itself is recorded at most once in
  a `refunds` table keyed by ticket ID — the same shape as an external payment
  provider idempotency key.
- SQLite writes happen after the workflow reaches a terminal result, making the
  database a derived view of completed workflow state.

This is a DDIA-style separation between the system of record for process state
and a derived representation optimized for later reads.

## Failures, Retries, Timeouts, Updates, And Queries

The retry policy in `src/ticketflow/workflows.py` gives agent activities up to
five attempts with exponential backoff. That models a common distributed
failure: a dependency is temporarily overloaded, and retrying is reasonable.
When retries are exhausted, the workflow does not disappear into a failed job
list; it returns an escalated ticket result.

Timeouts appear at two levels. Activities have start-to-close and heartbeat
timeouts so a stuck external call does not block forever. Approval uses
`workflow.wait_condition(..., timeout=APPROVAL_TIMEOUT)`, which creates a
durable wait. No worker thread needs to stay blocked for a day.

Approvals use a workflow update:

- The validator rejects approvals unless the ticket is currently
  `awaiting_approval`.
- Duplicate or late approvals become HTTP `409` responses.
- Accepted updates are recorded in workflow history and return the final status
  to the caller.

Queries are different. The `status` query reads the current workflow state
without changing it. That is ideal for live status pages, but it still needs a
worker capable of replaying the workflow history. Ticketflow handles that
limitation with the read model.

## Visibility And The Read Model

Ticketflow exposes two read paths because they answer different questions.

For active workflows, `TicketWorkflow._set_status` upserts a `TicketStatus`
search attribute. The API can then use `list_workflows` to build an approval
inbox, such as "all workflows where `TicketStatus = awaiting_approval`." This
is Temporal's visibility store, not a per-workflow query loop.

For completed workflows, `record_result` persists a compact `TicketResult` in
SQLite through `src/ticketflow/readmodel.py`. The API uses that result if a live
workflow query fails because history has been deleted after retention, the
worker is down, or the query times out.

That split mirrors a recurring DDIA theme: systems often maintain derived data
because the write path and read path have different needs. Temporal history is
excellent for durable execution and audit. The read model is better for a small
"what happened to this ticket?" lookup after the workflow is no longer live.

## Payload Schema Evolution

Pydantic models such as `Classification` and `DraftReply` cross the Temporal
wire as activity inputs and results. Those payloads are stored in workflow
history for as long as history is retained, and replay decodes old payloads
with the model code running today.

That makes schema evolution a compatibility contract. Adding a required field
to `Classification` breaks replay of histories whose recorded
`classify_ticket` result does not contain that field. The visible symptom is a
workflow task failure loop: the workflow remains running, but updates and
queries cannot make progress because replay cannot reconstruct state.

The safe rule is to add optional or defaulted fields and keep old names
readable. Required-field additions break backward compatibility with old
histories; removals or renames break forward compatibility with producers or
callers that still send the old shape. Defaults buy both sides enough
compatibility for old history and new code to coexist.

## At-Least-Once Delivery And Idempotent Side Effects

Temporal activities are at-least-once: when a worker finishes a side effect
but dies before acking the completion, the server never learns the attempt
succeeded and schedules a retry — the side effect runs again. This was
demonstrated by raising after the side effect on attempt 1:

- `record_result` wrote the ticket result to SQLite, failed, and ran again on
  attempt 2. History showed `ActivityTaskStarted` with `attempt: 2` and
  `lastFailure: "crash after side effect"`, yet `ticket_results` held exactly
  one row, because `INSERT OR REPLACE` keyed by ticket ID absorbs the
  duplicate.
- `execute_refund` recorded the refund, failed, and logged
  "already executed; attempt 2 is a no-op" on the retry. `refund_attempts`
  held two rows (the honest delivery count), `refunds` held one (the effect).

"Exactly-once" is therefore not a delivery guarantee but a sum: at-least-once
delivery plus idempotent effects. The counter-example makes the danger
concrete: had `record_result` been a plain `INSERT` plus a counter increment,
the retry would have double-counted, because retries are routine and every
non-idempotent side effect silently corrupts data the first time a worker
dies in the gap between effect and ack.

## DDIA Connections

Ticketflow is intentionally small, but it touches several data-intensive system
ideas:

- Reliability: workflow history lets a ticket continue after API or worker
  process failure.
- Fault tolerance: transient agent failures retry; exhausted retries escalate
  into a business outcome.
- Idempotency: workflow IDs prevent duplicate starts, and refund side effects
  are keyed by ticket ID.
- Derived data: the SQLite read model stores terminal results derived from
  workflow completion.
- Asynchronous processing: HTTP starts and observes work instead of doing all
  work inside one request.
- Human-in-the-loop coordination: workflow updates model a human decision as a
  durable event with validation and a result.
- Observability: Temporal Web UI shows workflow history, retries, timers, and
  updates; OpenTelemetry spans connect the API, workflow, and activities.

## Suggested Reading Path

1. Start with `README.md` to run the service and watch one ticket move through
   Temporal Web UI.
2. Read `src/ticketflow/models.py` to learn the domain states and data crossing
   API, workflow, and activity boundaries.
3. Read `src/ticketflow/api.py` to see the HTTP edge start, query, list, and
   update workflows.
4. Read `src/ticketflow/workflows.py` slowly. This is the durable state machine
   and the best file for understanding distributed control flow.
5. Read `src/ticketflow/activities.py` to see how side effects are moved out of
   deterministic workflow code.
6. Read `src/ticketflow/readmodel.py` to understand why completed workflow
   results are copied into SQLite.
7. Read `tests/test_workflow.py` for the most important behavior: retries,
   approval, duplicate update rejection, timeout escalation, search attributes,
   and read-model persistence.

The main lesson is that Ticketflow does not try to hide distribution behind a
plain function call. It makes the long-running process explicit, stores its
history durably, isolates side effects, and gives every important transition a
place in the model.
