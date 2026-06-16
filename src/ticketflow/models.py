"""Data models shared by API, workflow, and activities."""

from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class TicketCategory(StrEnum):
    """High-level support categories assigned by the agent."""

    BILLING = "billing"
    TECHNICAL = "technical"
    ACCOUNT = "account"
    GENERAL = "general"


class ActionType(StrEnum):
    """Side-effect class proposed by a drafted reply."""

    REPLY_ONLY = "reply_only"
    REFUND = "refund"


class TicketStatus(StrEnum):
    """Durable workflow states visible through the API and search attributes."""

    RECEIVED = "received"
    CLASSIFYING = "classifying"
    DRAFTING = "drafting"
    AWAITING_APPROVAL = "awaiting_approval"
    RESOLVED = "resolved"
    REJECTED = "rejected"
    ESCALATED = "escalated"


class Ticket(BaseModel):
    """Customer request accepted by the API and processed by the workflow."""

    id: str
    customer_email: str
    subject: str
    body: str


class Classification(BaseModel):
    """Agent category assignment with bounded confidence."""

    category: TicketCategory
    confidence: float = Field(ge=0.0, le=1.0)
    model: str = "primary"


class ProposedAction(BaseModel):
    """Action the system should take when sending a reply."""

    type: ActionType
    refund_amount: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def require_refund_amount_for_refunds(self) -> "ProposedAction":
        """Require refund actions to carry an amount at model boundaries."""
        if self.type == ActionType.REFUND and self.refund_amount is None:
            raise ValueError("refund_amount is required for refund actions")
        return self


class DraftReply(BaseModel):
    """Agent-authored response candidate and any requested side effect."""

    reply_text: str
    action: ProposedAction
    confidence: float = Field(ge=0.0, le=1.0)
    model: str = "primary"


class ApprovalDecision(BaseModel):
    """Human decision captured by the workflow update."""

    approved: bool
    approver: str
    note: str | None = None


class TicketResult(BaseModel):
    """Terminal ticket outcome persisted to the read model."""

    ticket_id: str
    status: TicketStatus
    reply_text: str
    refund_executed: bool = False
    model_path: str = "primary/primary"


class TicketStatusInfo(BaseModel):
    """Current or archived status returned by the ticket status API."""

    ticket_id: str
    status: TicketStatus
    classification: Classification | None = None
    draft: DraftReply | None = None
    decision: ApprovalDecision | None = None
    result: TicketResult | None = None
