"""Structured JSON logging with strict credential, DSN, and PII/PHI redaction."""

from __future__ import annotations

import logging
import os
import re
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any

import structlog
from pydantic import SecretStr

REDACTED = "[REDACTED]"

_SENSITIVE_KEY_TOKENS: frozenset[str] = frozenset(
    {
        "password",
        "passwd",
        "passphrase",
        "secret",
        "token",
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "credential",
        "credentials",
        "dsn",
        "connection_string",
        "chunk_text",
        "raw_text",
        "query_text",
        "prompt",
        "pii",
        "phi",
        "email",
        "ssn",
        "phone",
        "sasl_password",
        "ssl_key_password",
    }
)

_DSN_CREDENTIAL_RE = re.compile(
    r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^/\s:@]*):([^/\s@]+)@",
)
_KV_SECRET_RE = re.compile(
    r"(?i)\b(password|passwd|passphrase|secret|api_key|token|authorization)\s*([=:])\s*([^\s,;'\"]+|'[^']*'|\"[^\"]*\")"
)
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._\-+/=]+")

_DEFAULT_CORRELATION_FIELDS: dict[str, Any] = {
    "trace_id": "",
    "span_id": "",
    "tenant_id": "",
    "partition": -1,
    "stage": "",
    "duration_ms": None,
}


def _is_sensitive_key(key: str) -> bool:
    normalized = key.strip().lower()
    if normalized in _SENSITIVE_KEY_TOKENS:
        return True
    return any(tok in normalized for tok in ("password", "secret", "token", "passphrase", "dsn"))


def _sanitize_string(value: str) -> str:
    sanitized = _DSN_CREDENTIAL_RE.sub(rf"\1{REDACTED}:{REDACTED}@", value)
    sanitized = _BEARER_RE.sub(f"Bearer {REDACTED}", sanitized)
    sanitized = _KV_SECRET_RE.sub(rf"\1\2{REDACTED}", sanitized)
    return sanitized


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return REDACTED
    if isinstance(value, str):
        return _sanitize_string(value)
    if isinstance(value, Mapping):
        return {
            str(k): (REDACTED if _is_sensitive_key(str(k)) else _sanitize_value(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_value(item) for item in value)
    return value


def inject_correlation_defaults(
    _logger: Any,
    _method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """Ensure mandatory correlation keys exist on every emitted log record."""
    for key, default_val in _DEFAULT_CORRELATION_FIELDS.items():
        event_dict.setdefault(key, default_val)
    event_dict.setdefault("worker_pid", os.getpid())
    return event_dict


def redact_sensitive_fields(
    _logger: Any,
    _method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """Redact credentials, DSNs, raw chunk text, and PII/PHI before JSON serialization."""
    for key in list(event_dict.keys()):
        if _is_sensitive_key(key):
            event_dict[key] = REDACTED
        else:
            event_dict[key] = _sanitize_value(event_dict[key])
    return event_dict


def configure_logging(
    level: str = "INFO",
    stream: Any = None,
) -> None:
    """Configure structlog to emit single-line redacted JSON logs."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    target_stream = stream if stream is not None else sys.stdout

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
            inject_correlation_defaults,
            redact_sensitive_fields,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.WriteLoggerFactory(file=target_stream),
        cache_logger_on_first_use=False,
    )


def get_logger(
    name: str | None = None, **initial_bindings: Any
) -> structlog.typing.FilteringBoundLogger:
    """Return a structured JSON logger optionally bound with initial context."""
    logger: structlog.typing.FilteringBoundLogger = structlog.get_logger(name)
    if initial_bindings:
        return logger.bind(**initial_bindings)
    return logger
