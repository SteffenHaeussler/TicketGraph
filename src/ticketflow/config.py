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
AGENT_SCHEDULE_TO_START_S = float(os.environ.get("AGENT_SCHEDULE_TO_START_S", "30"))
MOCK_AGENT_LATENCY_MAX_S = float(os.environ.get("MOCK_AGENT_LATENCY_MAX_S", "0"))
DB_PATH = os.environ.get("TICKETFLOW_DB_PATH", "ticketflow.db")
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
