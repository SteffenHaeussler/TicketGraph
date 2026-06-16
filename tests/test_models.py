import pytest
from pydantic import ValidationError

from ticketflow.models import (
    ActionType,
    ApprovalDecision,
    Classification,
    DraftReply,
    ProposedAction,
    TicketCategory,
    TicketResult,
    TicketStatus,
)


def test_ticket_result_defaults_to_no_refund():
    result = TicketResult(
        ticket_id="t1", status=TicketStatus.RESOLVED, reply_text="done"
    )
    assert result.refund_executed is False


def test_reply_only_action_has_no_refund_amount():
    action = ProposedAction(type=ActionType.REPLY_ONLY)
    assert action.refund_amount is None


def test_classification_confidence_must_be_between_zero_and_one():
    with pytest.raises(ValidationError):
        Classification(category=TicketCategory.BILLING, confidence=-0.1)

    with pytest.raises(ValidationError):
        Classification(category=TicketCategory.BILLING, confidence=1.1)


def test_agent_outputs_default_to_primary_model_for_old_payloads():
    classification = Classification(category=TicketCategory.BILLING, confidence=0.9)
    draft = DraftReply(
        reply_text="Try restarting the app.",
        action=ProposedAction(type=ActionType.REPLY_ONLY),
        confidence=0.9,
    )

    assert classification.model == "primary"
    assert draft.model == "primary"


def test_draft_reply_confidence_must_not_exceed_one():
    with pytest.raises(ValidationError):
        DraftReply(
            reply_text="No problem.",
            action=ProposedAction(type=ActionType.REPLY_ONLY),
            confidence=1.1,
        )


def test_refund_action_requires_positive_refund_amount():
    with pytest.raises(ValidationError):
        ProposedAction(type=ActionType.REFUND)

    with pytest.raises(ValidationError):
        ProposedAction(type=ActionType.REFUND, refund_amount=0)

    action = ProposedAction(type=ActionType.REFUND, refund_amount=1.0)
    assert action.refund_amount == 1.0


def test_approval_decision_requires_approver():
    with pytest.raises(ValidationError):
        ApprovalDecision.model_validate({"approved": True})

    decision = ApprovalDecision(approved=True, approver="sam@example.com")
    assert decision.approver == "sam@example.com"


def test_ticket_result_defaults_to_primary_model_path_for_old_payloads():
    result = TicketResult(
        ticket_id="t-1",
        status=TicketStatus.RESOLVED,
        reply_text="done",
        refund_executed=False,
    )

    assert result.model_path == "primary/primary"
