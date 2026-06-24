# Agent Activity Operations

LLM-backed activities run on separate task queues from workflow progress and
lightweight side effects. This started as a sketch; it is now implemented.

How it is built:

- The LangGraph ticket workflow is advanced by `src/ticketflow/runner.py`.
  Terminal side effects (`send_reply`, `execute_refund`, `record_result`) are
  dispatched through the default `ticketflow` queue and drained by
  `src/ticketflow/side_effect_worker.py`.
- `classify` and `draft` agent work runs on the `ticketflow-agent` queue,
  hosted by the dedicated `src/ticketflow/agent_worker.py` process.
- The primary agent queue is throttled with two different knobs
  (`src/ticketflow/config.py`):
  - `AGENT_MAX_PER_SECOND` controls the primary worker's token bucket and spaces
    out task leases to model the LLM provider's request budget.
  - `AGENT_MAX_CONCURRENT` bounds in-process worker tasks and models host
    capacity.
- If an agent task waits longer than `AGENT_SCHEDULE_TO_START_S` (default 30s)
  in the primary queue, the runner resumes the graph with a timeout envelope.
  The graph cancels the still-pending primary task and re-dispatches the same
  work to the unthrottled `ticketflow-agent-fallback` queue, served by a faster,
  lower-confidence mock (`MockAgent.fallback()`).
- Transient provider pressure raises `AgentOverloadedError` and is retried by
  the Postgres queue with exponential backoff. Permanent failures raise
  `AgentPermanentError`, are recorded as non-retryable task failures, and the
  workflow turns into an `ESCALATED` ticket.

Run `make demo-saturation-fallback` to see backpressure routing: the target
starts the Docker stack with a low `AGENT_MAX_PER_SECOND`, short
`AGENT_SCHEDULE_TO_START_S`, and a batch assertion that at least one settled
ticket reports a fallback model path.
