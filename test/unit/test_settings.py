"""Unit tests for 12-factor WorkerSettings."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from internal.config.settings import WorkerSettings


def test_settings_defaults_and_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKER_PROCESS_COUNT", "3")
    monkeypatch.setenv("TIMEWHEEL_EXPIRY_SECONDS", "1.5")
    monkeypatch.setenv("KAFKA_TOPIC_PARTITIONS", "32")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "super-secret-kafka-pass")
    monkeypatch.setenv("REDIS_PASSWORD", "super-secret-redis-pass")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "super-secret-ch-pass")

    settings = WorkerSettings()

    assert settings.worker_process_count == 3
    assert settings.timewheel_expiry_seconds == 1.5
    assert settings.kafka_topic_partitions == 32
    assert settings.batch_max_rows == 50_000
    assert settings.batch_flush_interval_seconds == 2.0

    rendered = repr(settings)
    assert "super-secret-kafka-pass" not in rendered
    assert "super-secret-redis-pass" not in rendered
    assert "super-secret-ch-pass" not in rendered


def test_settings_sasl_requires_credentials() -> None:
    with pytest.raises(ValidationError, match="kafka_sasl_mechanism"):
        WorkerSettings(kafka_security_protocol="SASL_SSL")

    settings = WorkerSettings(
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_sasl_username="worker-svc",
        kafka_sasl_password="my-sasl-password",
    )
    assert settings.kafka_sasl_mechanism == "SCRAM-SHA-512"
    assert "my-sasl-password" not in repr(settings)


def test_settings_fetch_memory_and_timeout_bounds() -> None:
    with pytest.raises(ValidationError, match="kafka_fetch_max_bytes"):
        WorkerSettings(
            kafka_fetch_max_bytes=65_536,
            kafka_max_partition_fetch_bytes=131_072,
        )

    with pytest.raises(ValidationError, match="kafka_session_timeout_ms"):
        WorkerSettings(
            kafka_session_timeout_ms=6_000,
            kafka_heartbeat_interval_ms=6_000,
        )

    with pytest.raises(ValidationError, match="worker_heartbeat_timeout_seconds"):
        WorkerSettings(
            worker_heartbeat_interval_seconds=5.0,
            worker_heartbeat_timeout_seconds=5.0,
        )
