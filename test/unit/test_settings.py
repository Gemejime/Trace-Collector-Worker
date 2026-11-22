"""Unit tests for 12-factor WorkerSettings."""

from __future__ import annotations

from pathlib import Path

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
    assert settings.timewheel_tick_ms == 50
    assert settings.timewheel_max_open_traces == 25_000
    assert settings.redis_socket_timeout_seconds == 0.5
    assert settings.redis_pipeline_batch_size == 256
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

    with pytest.raises(ValidationError, match="timewheel_expiry_seconds"):
        WorkerSettings(
            timewheel_expiry_seconds=0.01,
            timewheel_tick_ms=50,
        )


def test_settings_loads_dotenv_file_and_respects_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("WORKER_PROCESS_COUNT", raising=False)
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    monkeypatch.delenv("TIMEWHEEL_EXPIRY_SECONDS", raising=False)

    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "WORKER_PROCESS_COUNT=2\nKAFKA_TOPIC=custom-traces\nTIMEWHEEL_EXPIRY_SECONDS=2.5\n",
        encoding="utf-8",
    )

    first = WorkerSettings()
    assert first.worker_process_count == 2
    assert first.kafka_topic == "custom-traces"
    assert first.timewheel_expiry_seconds == 2.5

    dotenv_path.write_text(
        "WORKER_PROCESS_COUNT=6\nKAFKA_TOPIC=updated-traces\nTIMEWHEEL_EXPIRY_SECONDS=3.0\n",
        encoding="utf-8",
    )

    second = WorkerSettings()
    assert second.worker_process_count == 6
    assert second.kafka_topic == "updated-traces"
    assert second.timewheel_expiry_seconds == 3.0


def test_dotenv_example_covers_all_worker_settings_and_validates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    example_text = (repo_root / ".env.example").read_text(encoding="utf-8")

    declared_keys: set[str] = set()
    for raw_line in example_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            candidate = line.lstrip("#").strip()
            if "=" in candidate:
                key = candidate.split("=", 1)[0].strip()
                if key.isupper() and "_" in key:
                    declared_keys.add(key.lower())
        elif "=" in line:
            key = line.split("=", 1)[0].strip()
            declared_keys.add(key.lower())

    missing_fields = set(WorkerSettings.model_fields.keys()) - declared_keys
    assert not missing_fields, f"Missing fields in .env.example: {sorted(missing_fields)}"

    monkeypatch.chdir(tmp_path)
    (tmp_path / ".env").write_text(example_text, encoding="utf-8")
    loaded = WorkerSettings()
    assert loaded.kafka_topic == "rag-diagnostic-traces"
