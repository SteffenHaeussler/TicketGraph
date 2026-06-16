"""LangGraph workflow placeholder for the orchestration migration."""

from datetime import timedelta

CONFIDENCE_THRESHOLD = 0.75
APPROVAL_TIMEOUT = timedelta(hours=24)
ACTIVITY_TIMEOUT = timedelta(seconds=30)
AGENT_ACTIVITY_TIMEOUT = timedelta(minutes=2)
AGENT_HEARTBEAT_TIMEOUT = timedelta(seconds=30)

REJECTION_REPLY = (
    "Thanks for your patience. After review we cannot fulfil this request "
    "automatically; a human agent will follow up shortly."
)
ESCALATION_REPLY = (
    "We need a bit more time with your request and have escalated your "
    "ticket to a human agent."
)


class OrchestrationUnavailableError(RuntimeError):
    """Raised when code attempts to use orchestration before it is rebuilt."""
