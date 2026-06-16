"""Durable per-ticket workflow with conditional human approval."""

import asyncio
from datetime import timedelta
from typing import cast

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    TimeoutError,
    TimeoutType,
)

with workflow.unsafe.imports_passed_through():
    from ticketflow import config
    from ticketflow.activities import TicketActivities
    from ticketflow.models import (
        ActionType,
        ApprovalDecision,
        Classification,
        DraftReply,
        Ticket,
        TicketResult,
        TicketStatus,
        TicketStatusInfo,
    )

CONFIDENCE_THRESHOLD = 0.75
APPROVAL_TIMEOUT = timedelta(hours=24)
ACTIVITY_TIMEOUT = timedelta(seconds=30)
AGENT_ACTIVITY_TIMEOUT = timedelta(minutes=2)
AGENT_HEARTBEAT_TIMEOUT = timedelta(seconds=30)
AGENT_SCHEDULE_TO_START_S = config.AGENT_SCHEDULE_TO_START_S
AGENT_TASK_QUEUE = config.AGENT_TASK_QUEUE
FALLBACK_TASK_QUEUE = config.FALLBACK_TASK_QUEUE
RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_attempts=5,
)
SINGLE_ATTEMPT_RETRY_POLICY = RetryPolicy(maximum_attempts=1)

REJECTION_REPLY = (
    "Thanks for your patience. After review we cannot fulfil this request "
    "automatically; a human agent will follow up shortly."
)
ESCALATION_REPLY = (
    "We need a bit more time with your request and have escalated your "
    "ticket to a human agent."
)
TICKET_STATUS_ATTR = SearchAttributeKey.for_keyword("TicketStatus")


@workflow.defn
class TicketWorkflow:
    """Durable workflow that resolves one support ticket."""

    def __init__(self) -> None:
        """Initialize replay-safe workflow state."""
        self._ticket: Ticket | None = None
        self._status = TicketStatus.RECEIVED
        self._classification: Classification | None = None
        self._draft: DraftReply | None = None
        self._decision: ApprovalDecision | None = None

    @workflow.run
    async def run(self, ticket: Ticket) -> TicketResult:
        """Drive classification, drafting, approval, and terminal side effects."""
        self._ticket = ticket
        self._set_status(TicketStatus.RECEIVED)

        self._set_status(TicketStatus.CLASSIFYING)
        try:
            self._classification = await self._execute_agent_activity(
                TicketActivities.classify_ticket,
                ticket,
            )
        except ActivityError:
            return await self._finish(
                reply_text=ESCALATION_REPLY,
                refund=False,
                status=TicketStatus.ESCALATED,
            )

        self._set_status(TicketStatus.DRAFTING)
        try:
            self._draft = await self._execute_agent_activity(
                TicketActivities.draft_reply,
                args=[ticket, self._classification],
            )
        except ActivityError:
            return await self._finish(
                reply_text=ESCALATION_REPLY,
                refund=False,
                status=TicketStatus.ESCALATED,
            )

        draft = self._draft
        if draft is None:
            raise ApplicationError("draft missing", non_retryable=True)
        needs_approval = (
            draft.action.type == ActionType.REFUND
            or draft.confidence < CONFIDENCE_THRESHOLD
        )
        if not needs_approval:
            return await self._finish(
                reply_text=draft.reply_text,
                refund=False,
                status=TicketStatus.RESOLVED,
            )

        self._set_status(TicketStatus.AWAITING_APPROVAL)
        try:
            await workflow.wait_condition(
                lambda: self._decision is not None, timeout=APPROVAL_TIMEOUT
            )
        except asyncio.TimeoutError:
            return await self._finish(
                reply_text=ESCALATION_REPLY,
                refund=False,
                status=TicketStatus.ESCALATED,
            )

        decision = self._decision
        if decision is None:
            raise ApplicationError("approval decision missing", non_retryable=True)

        if not decision.approved:
            return await self._finish(
                reply_text=REJECTION_REPLY,
                refund=False,
                status=TicketStatus.REJECTED,
            )

        return await self._finish(
            reply_text=draft.reply_text,
            refund=draft.action.type == ActionType.REFUND,
            status=TicketStatus.RESOLVED,
        )

    @workflow.update
    async def submit_approval(self, decision: ApprovalDecision) -> TicketStatus:
        """Accept a human decision and return the resulting status."""
        self._decision = decision
        await workflow.wait_condition(
            lambda: self._status != TicketStatus.AWAITING_APPROVAL
        )
        return self._status

    @submit_approval.validator
    def validate_submit_approval(self, decision: ApprovalDecision) -> None:
        """Reject approval updates unless the workflow is awaiting one."""
        _ = decision
        if self._status != TicketStatus.AWAITING_APPROVAL or self._decision is not None:
            raise ApplicationError(
                "ticket is not awaiting approval", non_retryable=True
            )

    @workflow.query
    def status(self) -> TicketStatusInfo:
        """Return the current in-workflow ticket state."""
        return TicketStatusInfo(
            ticket_id=self._ticket.id if self._ticket else "",
            status=self._status,
            classification=self._classification,
            draft=self._draft,
            decision=self._decision,
        )

    async def _finish(
        self, *, reply_text: str, refund: bool, status: TicketStatus
    ) -> TicketResult:
        if self._ticket is None:
            raise ApplicationError("workflow has no ticket", non_retryable=True)
        # Set the terminal status before the final activities so the approval
        # validator rejects updates that arrive while they are still running.
        self._set_status(status)
        if refund:
            draft = cast(DraftReply, self._draft)
            await workflow.execute_activity_method(
                TicketActivities.execute_refund,
                args=[self._ticket.id, draft.action.refund_amount],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
        await workflow.execute_activity_method(
            TicketActivities.send_reply,
            args=[self._ticket, reply_text],
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )
        result = TicketResult(
            ticket_id=self._ticket.id,
            status=status,
            reply_text=reply_text,
            refund_executed=refund,
            model_path=self._model_path(),
        )
        await workflow.execute_activity_method(
            TicketActivities.record_result,
            result,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )
        return result

    def _model_path(self) -> str:
        classification_model = (
            self._classification.model if self._classification else "primary"
        )
        draft_model = self._draft.model if self._draft else "primary"
        return f"{classification_model}/{draft_model}"

    async def _execute_agent_activity(self, activity_method, *args, **kwargs):
        primary_options = {
            **kwargs,
            "task_queue": AGENT_TASK_QUEUE,
            "schedule_to_start_timeout": timedelta(seconds=AGENT_SCHEDULE_TO_START_S),
            "start_to_close_timeout": AGENT_ACTIVITY_TIMEOUT,
            "heartbeat_timeout": AGENT_HEARTBEAT_TIMEOUT,
            "retry_policy": SINGLE_ATTEMPT_RETRY_POLICY,
        }
        delay = RETRY_POLICY.initial_interval
        for attempt in range(1, RETRY_POLICY.maximum_attempts + 1):
            try:
                return await workflow.execute_activity_method(
                    activity_method,
                    *args,
                    **primary_options,
                )
            except ActivityError as exc:
                if _is_schedule_to_start_timeout(exc):
                    return await _execute_fallback_agent_activity(
                        activity_method, *args, **kwargs
                    )
                if _is_non_retryable_application_error(exc):
                    raise
                if attempt == RETRY_POLICY.maximum_attempts:
                    raise
                await workflow.sleep(delay)
                delay *= RETRY_POLICY.backoff_coefficient
        raise ApplicationError(
            "agent activity retry loop exhausted", non_retryable=True
        )

    def _set_status(self, status: TicketStatus) -> None:
        self._status = status
        workflow.upsert_search_attributes([TICKET_STATUS_ATTR.value_set(status.value)])


def _is_schedule_to_start_timeout(exc: ActivityError) -> bool:
    return (
        isinstance(exc.cause, TimeoutError)
        and exc.cause.type == TimeoutType.SCHEDULE_TO_START
    )


def _is_non_retryable_application_error(exc: ActivityError) -> bool:
    return isinstance(exc.cause, ApplicationError) and exc.cause.non_retryable


async def _execute_fallback_agent_activity(activity_method, *args, **kwargs):
    fallback_options = {
        **kwargs,
        "task_queue": FALLBACK_TASK_QUEUE,
        "start_to_close_timeout": AGENT_ACTIVITY_TIMEOUT,
        "heartbeat_timeout": AGENT_HEARTBEAT_TIMEOUT,
        "retry_policy": RETRY_POLICY,
    }
    return await workflow.execute_activity_method(
        activity_method,
        *args,
        **fallback_options,
    )
