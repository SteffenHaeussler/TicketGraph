# Ticketflow design (2026-06-10)

A Temporal.io learning project: a mocked AI agent resolves support tickets inside
a durable workflow, with conditional human-in-the-loop approval.

## Decisions (from brainstorming)

- Language: Python (`temporalio` SDK), packaged with `uv`.
- Agent is mocked with realistic randomness behind an `Agent` protocol so a real
  LLM (e.g. Claude) can be swapped in later.
- Domain: support-ticket resolver — classify → draft reply → risky actions
  (refunds) or low-confidence drafts need human approval.
- Interaction via FastAPI (start ticket, query status, approve/reject) so a web
  UI can be added later.
- Architecture: single workflow per ticket (Option A). Parent/child saga
  (Option B) and a dynamic agent loop (Option C) are future iterations.

## Flow

1. `classify_ticket` activity (retried on transient mock failures)
2. `draft_reply` activity → proposed action: reply-only or refund(amount)
3. If refund proposed OR confidence < 0.75: wait up to 24h for an approval
   signal; on timeout escalate to a human and stop.
4. Approved → execute refund (if any) + send reply. Rejected → send fallback.
5. `status` query exposes current step/draft at any time.

## Temporal concepts exercised

Activities + retry policies, signals, queries, `workflow.wait_condition`,
timers/timeouts, workflow IDs & idempotency, workers, time-skipping tests,
Temporal Web UI.
