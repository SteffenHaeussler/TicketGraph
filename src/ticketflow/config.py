"""Connection settings shared by worker and API."""

import os

from dotenv import load_dotenv

load_dotenv(".env")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://ticketflow:ticketflow@localhost:5432/ticketflow"
)
TASK_QUEUE = os.environ.get("TICKETFLOW_TASK_QUEUE", "ticketflow")
AGENT_TASK_QUEUE = os.environ.get("TICKETFLOW_AGENT_TASK_QUEUE", "ticketflow-agent")
FALLBACK_TASK_QUEUE = os.environ.get(
    "TICKETFLOW_FALLBACK_TASK_QUEUE", "ticketflow-agent-fallback"
)
AGENT_MAX_PER_SECOND = float(os.environ.get("AGENT_MAX_PER_SECOND", "10.0"))
AGENT_MAX_CONCURRENT = int(os.environ.get("AGENT_MAX_CONCURRENT", "20"))
DB_POOL_MAX_SIZE = max(
    int(os.environ.get("TICKETFLOW_DB_POOL_MAX_SIZE", "10")), AGENT_MAX_CONCURRENT
)
AGENT_SCHEDULE_TO_START_S = float(os.environ.get("AGENT_SCHEDULE_TO_START_S", "30"))
MOCK_AGENT_LATENCY_MAX_S = float(os.environ.get("MOCK_AGENT_LATENCY_MAX_S", "0"))
JANITOR_INTERVAL_S = float(os.environ.get("TICKETFLOW_JANITOR_INTERVAL_S", "5.0"))
RETENTION_INTERVAL_S = float(
    os.environ.get("TICKETFLOW_RETENTION_INTERVAL_S", "3600.0")
)
RETENTION_MAX_AGE_S = float(
    os.environ.get("TICKETFLOW_RETENTION_MAX_AGE_S", "604800.0")
)
LOG_FORMAT = os.environ.get("TICKETFLOW_LOG_FORMAT", "text")
LOG_LEVEL = os.environ.get("TICKETFLOW_LOG_LEVEL", "INFO")
TRACE_EXPORTER = os.environ.get("TICKETFLOW_TRACE_EXPORTER", "none")
OTLP_ENDPOINT = os.environ.get(
    "TICKETFLOW_OTLP_ENDPOINT", "http://localhost:4318/v1/traces"
)
LOG_FIELDS = [
    field.strip()
    for field in os.environ.get(
        "TICKETFLOW_LOG_FIELDS", "time,level,logger,message,ticket_id"
    ).split(",")
    if field.strip()
]
