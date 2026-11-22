"""12-factor environment configuration for rag-trace-collector-worker."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """Validated runtime settings for supervisor and isolated worker processes."""

    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    # Multi-process supervisor & lifecycle
    worker_process_count: int = Field(default=4, ge=1, le=32)
    graceful_drain_timeout_seconds: float = Field(default=20.0, gt=0.0, le=300.0)
    worker_heartbeat_interval_seconds: float = Field(default=2.0, gt=0.0, le=60.0)
    worker_heartbeat_timeout_seconds: float = Field(default=10.0, gt=0.0, le=300.0)
    worker_respawn_initial_backoff_seconds: float = Field(default=0.5, ge=0.1, le=30.0)
    worker_respawn_max_backoff_seconds: float = Field(default=30.0, ge=1.0, le=300.0)
    worker_max_restarts_in_window: int = Field(default=10, ge=1, le=100)
    worker_restart_window_seconds: float = Field(default=60.0, ge=1.0, le=3600.0)

    # Kafka ingress (rag-diagnostic-traces, 32 partitions, partition key hash(trace_id))
    kafka_bootstrap_servers: str = Field(default="localhost:9092", min_length=1)
    kafka_topic: str = Field(default="rag-diagnostic-traces", min_length=1)
    kafka_topic_partitions: int = Field(default=32, ge=1, le=1024)
    kafka_group_id: str = Field(default="rag-trace-collector-worker", min_length=1)
    kafka_auto_offset_reset: Literal["earliest", "latest"] = Field(default="earliest")
    kafka_security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = Field(
        default="PLAINTEXT"
    )
    kafka_sasl_mechanism: Literal["SCRAM-SHA-256", "SCRAM-SHA-512"] | None = Field(default=None)
    kafka_sasl_username: str | None = Field(default=None)
    kafka_sasl_password: SecretStr | None = Field(default=None)
    kafka_ssl_cafile: str | None = Field(default=None)
    kafka_ssl_certfile: str | None = Field(default=None)
    kafka_ssl_keyfile: str | None = Field(default=None)
    kafka_ssl_key_password: SecretStr | None = Field(default=None)
    kafka_max_poll_interval_ms: int = Field(default=300_000, ge=10_000, le=1_800_000)
    kafka_session_timeout_ms: int = Field(default=30_000, ge=6_000, le=300_000)
    kafka_heartbeat_interval_ms: int = Field(default=3_000, ge=500, le=60_000)
    kafka_max_payload_bytes: int = Field(default=1_048_576, ge=1024, le=16_777_216)
    kafka_max_poll_records: int = Field(default=500, ge=1, le=50_000)
    kafka_fetch_max_bytes: int = Field(default=16_777_216, ge=65_536, le=134_217_728)
    kafka_max_partition_fetch_bytes: int = Field(default=1_048_576, ge=65_536, le=33_554_432)
    kafka_max_in_flight_offsets: int = Field(default=10_000, ge=1, le=500_000)
    kafka_offset_commit_interval_seconds: float = Field(default=1.0, gt=0.0, le=60.0)

    # Redis 7.2 Cluster provenance cache
    redis_cluster_nodes: str = Field(default="localhost:6379", min_length=1)
    redis_password: SecretStr | None = Field(default=None)
    redis_tls_enabled: bool = Field(default=False)
    redis_ssl_cafile: str | None = Field(default=None)
    redis_lru_capacity: int = Field(default=50_000, ge=100, le=1_000_000)
    redis_socket_timeout_seconds: float = Field(default=0.5, gt=0.0, le=30.0)
    redis_pipeline_batch_size: int = Field(default=256, ge=1, le=10_000)

    # ClickHouse 24.3+ native TCP sink
    clickhouse_host: str = Field(default="localhost", min_length=1)
    clickhouse_port: int = Field(default=9000, ge=1, le=65535)
    clickhouse_database: str = Field(default="rag_traces", min_length=1)
    clickhouse_user: str = Field(default="default", min_length=1)
    clickhouse_password: SecretStr | None = Field(default=None)
    clickhouse_tls_enabled: bool = Field(default=False)
    clickhouse_ssl_cafile: str | None = Field(default=None)

    # Time-wheel, micro-batching & local disk WAL
    timewheel_expiry_seconds: float = Field(default=1.5, gt=0.0, le=60.0)
    timewheel_tick_ms: int = Field(default=50, ge=10, le=1_000)
    timewheel_max_open_traces: int = Field(default=25_000, ge=100, le=500_000)
    batch_max_rows: int = Field(default=50_000, ge=1, le=500_000)
    batch_flush_interval_seconds: float = Field(default=2.0, gt=0.0, le=60.0)
    disk_wal_dir: str = Field(default="/var/spool/trace-wal", min_length=1)

    # Telemetry & logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO")
    telemetry_host: str = Field(default="127.0.0.1", min_length=1)
    telemetry_port: int = Field(default=8080, ge=1, le=65535)
    prometheus_multiproc_dir: str = Field(default="")

    @model_validator(mode="after")
    def _validate_cross_field_invariants(self) -> WorkerSettings:
        if self.kafka_security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
            if self.kafka_sasl_mechanism is None:
                raise ValueError(
                    "kafka_sasl_mechanism is required when kafka_security_protocol uses SASL"
                )
            if not self.kafka_sasl_username:
                raise ValueError(
                    "kafka_sasl_username is required when kafka_security_protocol uses SASL"
                )
            if self.kafka_sasl_password is None:
                raise ValueError(
                    "kafka_sasl_password is required when kafka_security_protocol uses SASL"
                )

        if self.kafka_fetch_max_bytes < self.kafka_max_partition_fetch_bytes:
            raise ValueError("kafka_fetch_max_bytes must be >= kafka_max_partition_fetch_bytes")

        if self.kafka_session_timeout_ms <= self.kafka_heartbeat_interval_ms:
            raise ValueError("kafka_session_timeout_ms must be > kafka_heartbeat_interval_ms")

        if self.worker_respawn_max_backoff_seconds < self.worker_respawn_initial_backoff_seconds:
            raise ValueError(
                "worker_respawn_max_backoff_seconds must be >= "
                "worker_respawn_initial_backoff_seconds"
            )

        if self.worker_heartbeat_timeout_seconds <= self.worker_heartbeat_interval_seconds:
            raise ValueError(
                "worker_heartbeat_timeout_seconds must be > worker_heartbeat_interval_seconds"
            )

        if self.timewheel_expiry_seconds * 1000.0 < self.timewheel_tick_ms:
            raise ValueError("timewheel_expiry_seconds * 1000 must be >= timewheel_tick_ms")

        return self
