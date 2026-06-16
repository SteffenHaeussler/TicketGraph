# Agent Activity Operations

LLM-backed activities run on separate task queues from workflow progress and
lightweight side effects. This started as a sketch; it is now implemented.

How it is built:

- `TicketWorkflow` and the fast side effects (`send_reply`, `execute_refund`,
  `record_result`) stay on the `ticketflow` task queue, hosted by
  `src/ticketflow/worker.py`. Tickets keep progressing even when the agent
  queues are saturated.
- `classify_ticket` and `draft_reply` run on the `ticketflow-agent` queue,
  hosted by the dedicated `src/ticketflow/llm_worker.py` process.
- The primary agent queue is throttled with two different knobs
  (`src/ticketflow/config.py`):
  - `AGENT_MAX_PER_SECOND` → `max_task_queue_activities_per_second`, a
    *server-side* limit that models the LLM provider's request budget. It is
    enforced across all workers polling the queue, so scaling out workers never
    exceeds the vendor limit.
  - `AGENT_MAX_CONCURRENT` → `max_concurrent_activities`, a *worker-side* limit
    that models the host's capacity and protects each process.
- If an agent task waits longer than `AGENT_SCHEDULE_TO_START_S` (default 30s)
  in the primary queue, `TicketWorkflow._execute_agent_activity`
  (`src/ticketflow/workflows.py`) catches the `SCHEDULE_TO_START` timeout and
  reruns the activity on the unthrottled `ticketflow-agent-fallback` queue,
  served by a faster, lower-confidence mock (`MockAgent.fallback()`). Fallback
  results cap confidence at 0.6, so they visibly land in the approval inbox.
- Agent activities keep longer start-to-close (2m) and heartbeat (30s) timeouts
  than side-effect activities (30s start-to-close). Transient provider pressure
  raises `AgentOverloadedError` and is retried with backoff; permanent failures
  raise `AgentPermanentError`, which activities convert to a non-retryable
  Temporal application error and the workflow turns into an `ESCALATED` ticket.

Known gaps are tracked in `docs/context.md` under "Open follow-ups": heartbeats
fire only at the start and end of each agent call (a real >30s LLM call would
trip the heartbeat timeout), and the fallback path has no schedule-to-start
budget of its own (if the fallback worker is down too, tickets hang instead of
escalating).
