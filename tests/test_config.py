import importlib

from ticketflow import config


def test_config_reads_postgres_and_queue_settings_from_environment(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@db.example/tickets")
    monkeypatch.setenv("TICKETFLOW_TASK_QUEUE", "tickets-prod")
    monkeypatch.setenv("TICKETFLOW_AGENT_TASK_QUEUE", "agents-prod")
    monkeypatch.setenv("TICKETFLOW_FALLBACK_TASK_QUEUE", "agents-fallback-prod")
    monkeypatch.setenv("AGENT_MAX_PER_SECOND", "1.5")
    monkeypatch.setenv("AGENT_MAX_CONCURRENT", "7")
    monkeypatch.setenv("AGENT_SCHEDULE_TO_START_S", "4.5")
    monkeypatch.setenv("MOCK_AGENT_LATENCY_MAX_S", "3.25")
    monkeypatch.setenv("TICKETFLOW_JANITOR_INTERVAL_S", "2.5")
    monkeypatch.setenv("TICKETFLOW_LOG_FORMAT", "json")
    monkeypatch.setenv("TICKETFLOW_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("TICKETFLOW_LOG_FIELDS", "level,message,task_queue")
    monkeypatch.setenv("TICKETFLOW_TRACE_EXPORTER", "otlp")
    monkeypatch.setenv("TICKETFLOW_OTLP_ENDPOINT", "http://otel.example:4318/v1/traces")

    reloaded = importlib.reload(config)

    assert reloaded.DATABASE_URL == "postgresql://user:pass@db.example/tickets"
    assert reloaded.TASK_QUEUE == "tickets-prod"
    assert reloaded.AGENT_TASK_QUEUE == "agents-prod"
    assert reloaded.FALLBACK_TASK_QUEUE == "agents-fallback-prod"
    assert reloaded.AGENT_MAX_PER_SECOND == 1.5
    assert reloaded.AGENT_MAX_CONCURRENT == 7
    assert reloaded.AGENT_SCHEDULE_TO_START_S == 4.5
    assert reloaded.MOCK_AGENT_LATENCY_MAX_S == 3.25
    assert reloaded.JANITOR_INTERVAL_S == 2.5
    assert reloaded.LOG_FORMAT == "json"
    assert reloaded.LOG_LEVEL == "DEBUG"
    assert reloaded.LOG_FIELDS == ["level", "message", "task_queue"]
    assert reloaded.TRACE_EXPORTER == "otlp"
    assert reloaded.OTLP_ENDPOINT == "http://otel.example:4318/v1/traces"

    monkeypatch.delenv("DATABASE_URL")
    monkeypatch.delenv("TICKETFLOW_TASK_QUEUE")
    monkeypatch.delenv("TICKETFLOW_AGENT_TASK_QUEUE")
    monkeypatch.delenv("TICKETFLOW_FALLBACK_TASK_QUEUE")
    monkeypatch.delenv("AGENT_MAX_PER_SECOND")
    monkeypatch.delenv("AGENT_MAX_CONCURRENT")
    monkeypatch.delenv("AGENT_SCHEDULE_TO_START_S")
    monkeypatch.delenv("MOCK_AGENT_LATENCY_MAX_S")
    monkeypatch.delenv("TICKETFLOW_JANITOR_INTERVAL_S")
    monkeypatch.delenv("TICKETFLOW_LOG_FORMAT")
    monkeypatch.delenv("TICKETFLOW_LOG_LEVEL")
    monkeypatch.delenv("TICKETFLOW_LOG_FIELDS")
    monkeypatch.delenv("TICKETFLOW_TRACE_EXPORTER")
    monkeypatch.delenv("TICKETFLOW_OTLP_ENDPOINT")
    importlib.reload(config)


def test_config_trace_settings_default_to_disabled():
    assert config.TRACE_EXPORTER == "none"
    assert config.OTLP_ENDPOINT == "http://localhost:4318/v1/traces"


def test_config_database_url_defaults_to_local_postgres():
    assert (
        config.DATABASE_URL
        == "postgresql://ticketflow:ticketflow@localhost:5432/ticketflow"
    )


def test_config_agent_settings_default_to_local_demo_values():
    assert config.AGENT_TASK_QUEUE == "ticketflow-agent"
    assert config.FALLBACK_TASK_QUEUE == "ticketflow-agent-fallback"
    assert config.AGENT_MAX_PER_SECOND == 10.0
    assert config.AGENT_MAX_CONCURRENT == 20
    assert config.AGENT_SCHEDULE_TO_START_S == 30.0
    assert config.MOCK_AGENT_LATENCY_MAX_S == 0.0


def test_config_janitor_interval_defaults_to_five_seconds():
    assert config.JANITOR_INTERVAL_S == 5.0


def test_config_reads_postgres_settings_from_dotenv(tmp_path, monkeypatch):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "\n".join(
            [
                "DATABASE_URL=postgresql://dotenv.example/ticketflow",
                "TICKETFLOW_TASK_QUEUE=tickets-dotenv",
                "TICKETFLOW_LOG_FORMAT=json",
                "TICKETFLOW_LOG_LEVEL=WARNING",
                "TICKETFLOW_LOG_FIELDS=time,level,message",
            ]
        )
    )
    monkeypatch.chdir(tmp_path)

    reloaded = importlib.reload(config)

    assert reloaded.DATABASE_URL == "postgresql://dotenv.example/ticketflow"
    assert reloaded.TASK_QUEUE == "tickets-dotenv"
    assert reloaded.LOG_FORMAT == "json"
    assert reloaded.LOG_LEVEL == "WARNING"
    assert reloaded.LOG_FIELDS == ["time", "level", "message"]

    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("TICKETFLOW_TASK_QUEUE", raising=False)
    monkeypatch.delenv("TICKETFLOW_LOG_FORMAT", raising=False)
    monkeypatch.delenv("TICKETFLOW_LOG_LEVEL", raising=False)
    monkeypatch.delenv("TICKETFLOW_LOG_FIELDS", raising=False)
    importlib.reload(config)
