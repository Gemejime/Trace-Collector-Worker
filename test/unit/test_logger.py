"""Unit tests for structured JSON logging and strict credential/PII redaction."""

from __future__ import annotations

import io
import json

from pydantic import SecretStr

from internal.telemetry.logger import REDACTED, configure_logging, get_logger


def test_logger_emits_json_with_mandatory_correlation_fields() -> None:
    buf = io.StringIO()
    configure_logging(level="INFO", stream=buf)
    logger = get_logger("test_component", trace_id="tr-123", span_id="sp-456", tenant_id="t-1")

    logger.info("span_processed", partition=7, stage="rrf_blend", duration_ms=1.25)

    line = buf.getvalue().strip()
    record = json.loads(line)
    assert record["event"] == "span_processed"
    assert record["level"] == "info"
    assert record["trace_id"] == "tr-123"
    assert record["span_id"] == "sp-456"
    assert record["tenant_id"] == "t-1"
    assert record["partition"] == 7
    assert record["stage"] == "rrf_blend"
    assert record["duration_ms"] == 1.25
    assert isinstance(record["worker_pid"], int)
    assert "timestamp" in record


def test_logger_default_correlation_fields_outside_span_context() -> None:
    buf = io.StringIO()
    configure_logging(level="INFO", stream=buf)
    logger = get_logger("supervisor")

    logger.info("supervisor_started")

    record = json.loads(buf.getvalue().strip())
    for key in (
        "trace_id",
        "span_id",
        "tenant_id",
        "worker_pid",
        "partition",
        "stage",
        "duration_ms",
    ):
        assert key in record


def test_logger_redacts_credentials_dsns_and_pii() -> None:
    buf = io.StringIO()
    configure_logging(level="INFO", stream=buf)
    logger = get_logger("security_test")

    logger.warning(
        "connection_failed",
        password="secret-password-123",
        sasl_password=SecretStr("sasl-secret-xyz"),
        chunk_text="Confidential patient medical history",
        email="alice@example.com",
        dsn="clickhouse://writer:ch-secret@clickhouse.internal:9000/rag_traces",
        message="Failed auth Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig on rediss://:redispass@redis:6379",
        nested={"api_key": "key-999999", "safe_doc_id": 42},
    )

    output = buf.getvalue()
    for leaked in (
        "secret-password-123",
        "sasl-secret-xyz",
        "Confidential patient medical history",
        "alice@example.com",
        "ch-secret",
        "eyJhbGciOiJIUzI1NiJ9",
        "redispass",
        "key-999999",
    ):
        assert leaked not in output

    record = json.loads(output.strip())
    assert record["password"] == REDACTED
    assert record["sasl_password"] == REDACTED
    assert record["chunk_text"] == REDACTED
    assert record["email"] == REDACTED
    assert record["dsn"] == REDACTED
    assert record["nested"]["api_key"] == REDACTED
    assert record["nested"]["safe_doc_id"] == 42
    assert f"Bearer {REDACTED}" in record["message"]
