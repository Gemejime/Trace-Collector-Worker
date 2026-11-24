"""Unit tests for scripts/sync_env_to_deployments.py."""

from __future__ import annotations

from pathlib import Path

import yaml

from scripts.sync_env_to_deployments import is_sensitive_key, run_sync


def test_is_sensitive_key_filters_credentials() -> None:
    assert is_sensitive_key("KAFKA_SASL_PASSWORD") is True
    assert is_sensitive_key("API_SECRET") is True
    assert is_sensitive_key("AUTH_TOKEN") is True
    assert is_sensitive_key("SSL_KEYFILE") is True
    assert is_sensitive_key("KAFKA_TOPIC") is False
    assert is_sensitive_key("WORKER_PROCESS_COUNT") is False


def test_sync_and_check_only_preserve_blocks_and_secret_name(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.prod"
    env_file.write_text(
        "\n".join(
            [
                "WORKER_PROCESS_COUNT=4",
                "KAFKA_TOPIC=rag-diagnostic-traces",
                "KAFKA_SASL_PASSWORD=super-secret",
                "REDIS_TOKEN=tok-123",
                "API_SECRET=sec-456",
                "SIGNING_KEY=key-789",
                "PROMETHEUS_MULTIPROC_DIR=",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    values_file = tmp_path / "values.yaml"
    values_file.write_text(
        "\n".join(
            [
                "# Keep top-level comment",
                "replicaCount: 4",
                "image:",
                "  repository: ghcr.io/example/worker",
                "env:",
                '  secretName: "prod-secrets"',
                '  WORKER_PROCESS_COUNT: "2"',
                "keda:",
                "  enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    # Initial check-only detects drift
    assert run_sync(env_file, values_file, check_only=True) == 1

    # Sync updates values.yaml
    assert run_sync(env_file, values_file, check_only=False) == 0

    # Subsequent check-only passes cleanly
    assert run_sync(env_file, values_file, check_only=True) == 0

    updated_text = values_file.read_text(encoding="utf-8")
    assert "# Keep top-level comment" in updated_text
    assert "super-secret" not in updated_text
    assert "tok-123" not in updated_text
    assert "sec-456" not in updated_text
    assert "key-789" not in updated_text

    parsed = yaml.safe_load(updated_text)
    assert parsed["replicaCount"] == 4
    assert parsed["image"]["repository"] == "ghcr.io/example/worker"
    assert parsed["keda"]["enabled"] is True
    assert parsed["env"] == {
        "secretName": "prod-secrets",
        "WORKER_PROCESS_COUNT": "4",
        "KAFKA_TOPIC": "rag-diagnostic-traces",
        "PROMETHEUS_MULTIPROC_DIR": "",
    }
