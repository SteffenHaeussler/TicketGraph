from types import SimpleNamespace

from temporalio.api.workflowservice.v1 import request_response_pb2
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from scripts import reset
from ticketflow import readmodel
from ticketflow.models import TicketResult, TicketStatus


class FakeHandle:
    def __init__(self, client: "FakeClient", workflow_id: str):
        self._client = client
        self.workflow_id = workflow_id
        self.terminate_reasons: list[str] = []

    async def terminate(self, reason: str) -> None:
        if self._client.fail_terminate:
            raise RPCError("already closed", RPCStatusCode.NOT_FOUND, b"")
        self.terminate_reasons.append(reason)
        self._client.workflows[
            self.workflow_id
        ].status = WorkflowExecutionStatus.TERMINATED


class FakeWorkflowService:
    def __init__(self):
        self.deleted_ids: list[str] = []

    async def delete_workflow_execution(
        self, request: request_response_pb2.DeleteWorkflowExecutionRequest
    ) -> None:
        self.deleted_ids.append(request.workflow_execution.workflow_id)


class FakeWorkflowIterator:
    def __init__(self, workflows):
        self._workflows = list(workflows)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._workflows:
            raise StopAsyncIteration
        return self._workflows.pop(0)


class FakeClient:
    namespace = "default"

    def __init__(self, statuses: dict[str, WorkflowExecutionStatus]):
        self.workflows = {
            workflow_id: SimpleNamespace(id=workflow_id, status=status)
            for workflow_id, status in statuses.items()
        }
        self.handles = {
            workflow_id: FakeHandle(self, workflow_id) for workflow_id in statuses
        }
        self.workflow_service = FakeWorkflowService()
        self.queries: list[str] = []
        self.fail_terminate = False

    def list_workflows(self, query: str):
        self.queries.append(query)
        workflows = self.workflows.values()
        if 'ExecutionStatus = "Running"' in query:
            workflows = [
                workflow
                for workflow in workflows
                if workflow.status == WorkflowExecutionStatus.RUNNING
            ]
        return FakeWorkflowIterator(workflows)

    def get_workflow_handle(self, workflow_id: str):
        return self.handles[workflow_id]


def make_client(fail_terminate: bool = False) -> FakeClient:
    client = FakeClient(
        {
            "ticket-running": WorkflowExecutionStatus.RUNNING,
            "ticket-done": WorkflowExecutionStatus.COMPLETED,
        }
    )
    client.fail_terminate = fail_terminate
    return client


async def test_reset_terminates_running_and_deletes_all_ticket_workflows():
    client = make_client()

    terminated, deleted = await reset.reset_workflows(client)

    assert terminated == 1
    assert deleted == 2
    assert client.handles["ticket-running"].terminate_reasons == ["ticketflow reset"]
    assert client.handles["ticket-done"].terminate_reasons == []
    assert sorted(client.workflow_service.deleted_ids) == [
        "ticket-done",
        "ticket-running",
    ]


async def test_reset_waits_for_terminations_to_close_before_deleting():
    client = make_client()

    await reset.reset_workflows(client)

    # Deleting right after terminating races the server's close processing
    # and parks the deletion in a slow retry loop, so the running check must
    # come between the terminate pass and the delete pass.
    running_query_index = client.queries.index(reset.RUNNING_QUERY, 1)
    delete_pass_index = len(client.queries) - 1
    assert client.queries[0] == reset.RUNNING_QUERY
    assert running_query_index < delete_pass_index
    assert client.queries[delete_pass_index] == reset.WORKFLOW_QUERY


async def test_reset_survives_terminate_race_with_closing_workflow():
    client = make_client(fail_terminate=True)

    terminated, deleted = await reset.reset_workflows(client, wait_timeout=0)

    assert terminated == 0
    assert deleted == 2


async def test_run_reset_clears_read_model_and_reports_counts(tmp_path):
    db = str(tmp_path / "read.db")
    readmodel.save_result(
        TicketResult(
            ticket_id="old",
            status=TicketStatus.RESOLVED,
            reply_text="archived",
        ),
        db,
    )
    client = make_client()

    summary = await reset.run_reset(client, db_path=db)

    assert summary == {"terminated": 1, "deleted": 2, "read_model_rows_cleared": 1}
    assert readmodel.load_result("old", db) is None
