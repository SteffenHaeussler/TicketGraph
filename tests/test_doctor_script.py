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
        "temporal: unknown",
        "worker: unknown",
    ]


async def test_check_stack_fails_when_worker_pollers_are_missing():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "healthy"})
        return httpx.Response(
            200,
            json={
                "status": "degraded",
                "temporal": {"status": "healthy"},
                "worker": {
                    "status": "degraded",
                    "task_queue": "ticketflow",
                    "workflow_pollers": 0,
                    "activity_pollers": 0,
                    "message": "No worker pollers found. Run `make worker`.",
                },
                "llm_worker": {
                    "status": "healthy",
                    "primary_task_queue": "ticketflow-agent",
                    "fallback_task_queue": "ticketflow-agent-fallback",
                    "primary_activity_pollers": 1,
                    "fallback_activity_pollers": 1,
                },
                "config": {
                    "address": "localhost:7233",
                    "namespace": "default",
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
        "temporal: healthy (localhost:7233, namespace default)",
        "worker: degraded (ticketflow; workflow pollers=0, activity pollers=0)",
        (
            "llm-worker: healthy (primary=ticketflow-agent pollers=1, "
            "fallback=ticketflow-agent-fallback pollers=1)"
        ),
        "worker: no pollers found; run `make worker`",
    ]


async def test_check_stack_fails_when_llm_worker_pollers_are_missing():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "healthy"})
        return httpx.Response(
            200,
            json={
                "status": "degraded",
                "temporal": {"status": "healthy"},
                "worker": {
                    "status": "healthy",
                    "task_queue": "ticketflow",
                    "workflow_pollers": 1,
                    "activity_pollers": 1,
                },
                "llm_worker": {
                    "status": "degraded",
                    "primary_task_queue": "ticketflow-agent",
                    "fallback_task_queue": "ticketflow-agent-fallback",
                    "primary_activity_pollers": 0,
                    "fallback_activity_pollers": 0,
                    "message": ("No LLM worker pollers found. Run `make llm-worker`."),
                },
                "config": {
                    "address": "localhost:7233",
                    "namespace": "default",
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
        "temporal: healthy (localhost:7233, namespace default)",
        "worker: healthy (ticketflow; workflow pollers=1, activity pollers=1)",
        (
            "llm-worker: degraded (primary=ticketflow-agent pollers=0, "
            "fallback=ticketflow-agent-fallback pollers=0)"
        ),
        "llm-worker: no pollers found; run `make llm-worker`",
    ]


async def test_check_stack_fails_when_temporal_is_unavailable():
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json={"status": "healthy"})
        return httpx.Response(
            503,
            json={
                "status": "unavailable",
                "temporal": {"status": "unavailable"},
                "worker": {"status": "unknown"},
                "config": {
                    "address": "localhost:7233",
                    "namespace": "default",
                    "task_queue": "ticketflow",
                },
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        result = await doctor.check_stack(client)

    assert result.exit_code == 1
    assert result.lines == [
        "api: healthy",
        "temporal: unavailable (localhost:7233, namespace default)",
        "worker: unknown",
        "llm-worker: unknown",
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
