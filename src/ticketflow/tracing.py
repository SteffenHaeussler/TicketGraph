"""Application tracing configuration via OpenTelemetry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanExporter,
)

from ticketflow import config

if TYPE_CHECKING:
    from fastapi import FastAPI

SUPPORTED_EXPORTERS = {"none", "console", "otlp"}


@dataclass(frozen=True)
class TracingSetup:
    """OpenTelemetry objects that need to share one provider."""

    provider: TracerProvider


def setup_tracing_components(
    service_name: str,
    exporter: str | None = None,
    endpoint: str | None = None,
    span_exporter: SpanExporter | None = None,
) -> TracingSetup | None:
    """Configure OpenTelemetry and return tracing components, if enabled."""
    resolved_exporter = (exporter or config.TRACE_EXPORTER).lower()
    if resolved_exporter not in SUPPORTED_EXPORTERS:
        raise ValueError(f"Unsupported trace exporter: {resolved_exporter}")
    if span_exporter is None and resolved_exporter == "none":
        return None

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    if span_exporter is not None:
        provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    elif resolved_exporter == "console":
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    else:
        provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=endpoint or config.OTLP_ENDPOINT)
            )
        )

    if not isinstance(trace.get_tracer_provider(), TracerProvider):
        trace.set_tracer_provider(provider)

    return TracingSetup(provider=provider)


def setup_tracing(
    service_name: str,
    exporter: str | None = None,
    endpoint: str | None = None,
    span_exporter: SpanExporter | None = None,
) -> TracerProvider | None:
    """Configure OpenTelemetry and return the provider, if enabled."""
    tracing = setup_tracing_components(
        service_name=service_name,
        exporter=exporter,
        endpoint=endpoint,
        span_exporter=span_exporter,
    )
    return tracing.provider if tracing else None


def instrument_fastapi_app(app: FastAPI, tracing: TracingSetup) -> None:
    """Instrument FastAPI with the configured provider."""
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracing.provider)
