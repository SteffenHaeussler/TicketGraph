"""Shared workflow constants for the LangGraph orchestration."""

from datetime import timedelta

CONFIDENCE_THRESHOLD = 0.75
APPROVAL_TIMEOUT = timedelta(hours=24)

REJECTION_REPLY = (
    "Thanks for your patience. After review we cannot fulfil this request "
    "automatically; a human agent will follow up shortly."
)
ESCALATION_REPLY = (
    "We need a bit more time with your request and have escalated your "
    "ticket to a human agent."
)
