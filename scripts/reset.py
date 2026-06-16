"""Wipe all ticketflow state: ticket workflows and the SQLite read model."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator
from typing import Protocol, cast

from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.workflowservice.v1 import request_response_pb2
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.service import RPCError

from ticketflow import config, readmodel

WORKFLOW_QUERY = 'WorkflowType = "TicketWorkflow"'
RUNNING_QUERY = WORKFLOW_QUERY + ' AND ExecutionStatus = "Running"'
TERMINATE_REASON = "ticketflow reset"


class _WorkflowSummary(Protocol):
    id: str


class _WorkflowHandle(Protocol):
    async def terminate(self, reason: str) -> None: ...


class _WorkflowService(Protocol):
    async def delete_workflow_execution(
        self, request: request_response_pb2.DeleteWorkflowExecutionRequest
    ) -> None: ...


class _ResetClient(Protocol):
    @property
    def namespace(self) -> str: ...

    @property
    def workflow_service(self) -> _WorkflowService: ...

    def list_workflows(self, query: str) -> AsyncIterator[_WorkflowSummary]: ...

    def get_workflow_handle(self, workflow_id: str) -> _WorkflowHandle: ...


async def _terminate_running(client: _ResetClient) -> int:
    terminated = 0
    async for summary in client.list_workflows(RUNNING_QUERY):
        try:
            await client.get_workflow_handle(summary.id).terminate(
                reason=TERMINATE_REASON
            )
            terminated += 1
        except RPCError:
            pass
    return terminated


async def _wait_until_none_running(client: _ResetClient, timeout: float) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        async for _ in client.list_workflows(RUNNING_QUERY):
            break
        else:
            return
        await asyncio.sleep(0.5)


async def _delete_all(client: _ResetClient) -> int:
    deleted = 0
    async for summary in client.list_workflows(WORKFLOW_QUERY):
        try:
            # The Python SDK has no high-level delete; use the raw service.
            await client.workflow_service.delete_workflow_execution(
                request_response_pb2.DeleteWorkflowExecutionRequest(
                    namespace=client.namespace,
                    workflow_execution=WorkflowExecution(workflow_id=summary.id),
                )
            )
            deleted += 1
        except RPCError:
            pass
    return deleted


async def reset_workflows(
    client: _ResetClient, wait_timeout: float = 15.0
) -> tuple[int, int]:
    """Terminate running ticket workflows, then delete all of them.

    Deleting immediately after terminating races the server's close
    processing and parks the deletion in a slow retry loop, so we wait for
    visibility to show no running workflows before the delete pass.
    Individual failures (e.g. a workflow closing between listing and
    terminating) are skipped so a reset never aborts halfway.
    """
    terminated = await _terminate_running(client)
    if terminated:
        await _wait_until_none_running(client, wait_timeout)
    deleted = await _delete_all(client)
    return terminated, deleted


async def run_reset(client: _ResetClient, db_path: str | None = None) -> dict[str, int]:
    """Reset ticket workflows and clear the read model."""
    terminated, deleted = await reset_workflows(client)
    cleared = readmodel.clear(db_path)
    return {
        "terminated": terminated,
        "deleted": deleted,
        "read_model_rows_cleared": cleared,
    }


def parse_args() -> argparse.Namespace:
    """Parse reset command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Terminate and delete all ticket workflows and clear "
        "the SQLite read model."
    )
    parser.add_argument("--address", default=config.TEMPORAL_ADDRESS)
    parser.add_argument("--namespace", default=config.TEMPORAL_NAMESPACE)
    parser.add_argument("--db-path", default=config.DB_PATH)
    return parser.parse_args()


async def amain(args: argparse.Namespace) -> dict[str, int]:
    """Connect to Temporal and run the reset command."""
    client = await Client.connect(
        args.address,
        namespace=args.namespace,
        data_converter=pydantic_data_converter,
    )
    return await run_reset(cast(_ResetClient, client), db_path=args.db_path)


def main() -> int:
    """Run the reset command."""
    args = parse_args()
    try:
        summary = asyncio.run(amain(args))
    except RuntimeError as exc:
        print(f"reset failed: {exc}")
        return 1

    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
