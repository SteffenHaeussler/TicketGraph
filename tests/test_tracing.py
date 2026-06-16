import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from ticketflow.tracing import (
    instrument_fastapi_app,
    setup_tracing,
    setup_tracing_components,
)


def test_invalid_exporter_raises_value_error():
    with pytest.raises(ValueError, match="Unsupported trace exporter"):
        setup_tracing(service_name="ticketflow-test", exporter="bogus")


def test_none_exporter_disables_tracing():
    assert setup_tracing(service_name="ticketflow-test", exporter="none") is None


def test_console_exporter_returns_tracer_provider():
    provider = setup_tracing(service_name="ticketflow-test", exporter="console")
    assert isinstance(provider, TracerProvider)


def test_injected_span_exporter_receives_spans_with_service_name():
    span_exporter = InMemorySpanExporter()
    provider = setup_tracing(
        service_name="ticketflow-test", span_exporter=span_exporter
    )
    assert isinstance(provider, TracerProvider)

    with provider.get_tracer("ticketflow").start_as_current_span("test-span"):
        pass

    spans = span_exporter.get_finished_spans()
    assert [span.name for span in spans] == ["test-span"]
    assert spans[0].resource.attributes["service.name"] == "ticketflow-test"


async def test_fastapi_instrumentation_uses_injected_provider():
    setup_tracing(service_name="global-provider", span_exporter=InMemorySpanExporter())
    span_exporter = InMemorySpanExporter()
    tracing = setup_tracing_components(
        service_name="ticketflow-api-test", span_exporter=span_exporter
    )
    assert tracing is not None

    app = FastAPI()

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    instrument_fastapi_app(app, tracing)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/ping")

    assert response.status_code == 200
    spans = span_exporter.get_finished_spans()
    assert any(span.name == "GET /ping" for span in spans)
    assert {span.resource.attributes["service.name"] for span in spans} == {
        "ticketflow-api-test"
    }
