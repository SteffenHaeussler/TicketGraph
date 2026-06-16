import httpx

from scripts import doctor


async def test_check_stack_reports_api_down():
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        result = await doctor.check_stack(client)

    assert result.exit_code == 1
    assert result.lines == [
        "api: unavailable (run `make api`)",
        "database: unknown",
        "orchestration: unknown",
    ]


async def test_check_stack_reports_milestone_zero_degraded_state():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "healthy"})
        return httpx.Response(
            200,
            json={
                "status": "degraded",
                "database": {"status": "not_checked"},
                "orchestration": {
                    "status": "not_implemented",
                    "message": "LangGraph/Postgres orchestration is not wired yet.",
                },
                "config": {
                    "database_url": "postgresql://localhost/ticketflow",
                    "task_queue": "ticketflow",
                    "agent_task_queue": "ticketflow-agent",
                    "fallback_task_queue": "ticketflow-agent-fallback",
                },
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        result = await doctor.check_stack(client)

    assert result.exit_code == 1
    assert result.lines == [
        "api: healthy",
        "database: not_checked (postgresql://localhost/ticketflow)",
        "orchestration: not_implemented",
        "orchestration: LangGraph/Postgres orchestration is not wired yet.",
    ]


def test_lines_to_print_suppresses_success_lines_when_quiet():
    result = doctor.CheckResult(exit_code=0, lines=["api: healthy"])

    assert doctor.lines_to_print(result, quiet=True) == []


def test_lines_to_print_keeps_failure_lines_when_quiet():
    result = doctor.CheckResult(
        exit_code=1, lines=["api: unavailable (run `make api`)"]
    )

    assert doctor.lines_to_print(result, quiet=True) == [
        "api: unavailable (run `make api`)"
    ]
