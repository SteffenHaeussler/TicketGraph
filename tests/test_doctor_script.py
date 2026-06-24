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


def _ready_handler(ready_body: dict[str, object]):
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "healthy"})
        return httpx.Response(200, json=ready_body)

    return handler


async def test_check_stack_reports_healthy_when_stack_is_ready():
    transport = httpx.MockTransport(
        _ready_handler(
            {
                "status": "healthy",
                "database": {"status": "connected"},
                "orchestration": {"status": "ready"},
                "config": {
                    "database_url": "postgresql://localhost/ticketflow",
                    "task_queue": "ticketflow",
                    "agent_task_queue": "ticketflow-agent",
                    "fallback_task_queue": "ticketflow-agent-fallback",
                },
            }
        )
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        result = await doctor.check_stack(client)

    assert result.exit_code == 0
    assert result.lines == [
        "api: healthy",
        "database: connected (postgresql://localhost/ticketflow)",
        "orchestration: ready",
    ]


async def test_check_stack_reports_degraded_when_database_unreachable():
    transport = httpx.MockTransport(
        _ready_handler(
            {
                "status": "degraded",
                "database": {"status": "unavailable"},
                "orchestration": {"status": "ready"},
                "config": {
                    "database_url": "postgresql://localhost/ticketflow",
                    "task_queue": "ticketflow",
                    "agent_task_queue": "ticketflow-agent",
                    "fallback_task_queue": "ticketflow-agent-fallback",
                },
            }
        )
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        result = await doctor.check_stack(client)

    assert result.exit_code == 1
    assert result.lines == [
        "api: healthy",
        "database: unavailable (postgresql://localhost/ticketflow)",
        "orchestration: ready",
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
