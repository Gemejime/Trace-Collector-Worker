"""Prometheus multiprocess metrics registry for rag-trace-collector-worker."""

from __future__ import annotations

import os
from pathlib import Path

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    multiprocess,
)

_FLUSH_LATENCY_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)


def configure_prometheus_multiproc(multiproc_dir: str) -> str | None:
    """Configure ``PROMETHEUS_MULTIPROC_DIR`` before metric registration when non-empty."""
    cleaned = multiproc_dir.strip()
    if not cleaned:
        return None
    Path(cleaned).mkdir(parents=True, exist_ok=True)
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = cleaned
    return cleaned


def mark_worker_process_dead(pid: int, multiproc_dir: str = "") -> None:
    """Remove dead worker PID shards from Prometheus multiprocess storage."""
    target_dir = multiproc_dir.strip() or os.environ.get("PROMETHEUS_MULTIPROC_DIR", "").strip()
    if not target_dir:
        return
    multiprocess.mark_process_dead(pid, path=target_dir)  # type: ignore[no-untyped-call]


def generate_metrics_exposition(
    multiproc_dir: str = "",
    registry: CollectorRegistry | None = None,
) -> bytes:
    """Render Prometheus text exposition bytes for multiprocess or single-process mode."""
    target_dir = multiproc_dir.strip() or os.environ.get("PROMETHEUS_MULTIPROC_DIR", "").strip()
    if target_dir:
        mp_registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(mp_registry, path=target_dir)  # type: ignore[no-untyped-call]
        return generate_latest(mp_registry)
    return generate_latest(registry or REGISTRY)


class MetricsRegistry:
    """Contract Prometheus metrics defined in ``.ai/doc-api.md``."""

    def __init__(
        self,
        worker_id: str,
        multiproc_dir: str = "",
        registry: CollectorRegistry | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.multiproc_dir = configure_prometheus_multiproc(multiproc_dir)
        if registry is None:
            registry = REGISTRY
        self.registry = registry

        self.spans_consumed_total = Counter(
            "trace_worker_spans_consumed_total",
            "Raw spans consumed per Kafka partition",
            labelnames=("partition", "worker_id"),
            registry=registry,
        )
        self.dags_assembled_total = Counter(
            "trace_worker_dags_assembled_total",
            "Completed multi-stage execution DAGs",
            labelnames=("worker_id",),
            registry=registry,
        )
        self.dag_evictions_total = Counter(
            "trace_worker_dag_evictions_total",
            "Time-wheel evictions by completion status",
            labelnames=("status",),
            registry=registry,
        )
        self.provenance_resolution_ratio = Gauge(
            "trace_worker_provenance_resolution_ratio",
            "Ratio of chunk hashes successfully mapped to lineage metadata",
            labelnames=("worker_id",),
            multiprocess_mode="liveall",
            registry=registry,
        )
        self.lru_hit_ratio = Gauge(
            "trace_worker_lru_hit_ratio",
            "Local 50k LRU cache hit ratio",
            labelnames=("worker_id",),
            multiprocess_mode="liveall",
            registry=registry,
        )
        self.clickhouse_flush_duration_seconds = Histogram(
            "trace_worker_clickhouse_flush_duration_seconds",
            "Micro-batch native TCP insert latencies into ClickHouse",
            labelnames=("worker_id",),
            buckets=_FLUSH_LATENCY_BUCKETS,
            registry=registry,
        )
        self.jemalloc_resident_bytes = Gauge(
            "trace_worker_jemalloc_resident_bytes",
            "Resident native heap allocated by jemalloc (mallctl)",
            labelnames=("worker_id",),
            multiprocess_mode="liveall",
            registry=registry,
        )
        self.kafka_consumer_lag = Gauge(
            "trace_worker_kafka_consumer_lag",
            "Kafka consumer group lag per partition",
            labelnames=("partition",),
            multiprocess_mode="liveall",
            registry=registry,
        )
        self.spillover_active = Gauge(
            "trace_worker_spillover_active",
            "1 when spooling micro-batches to local disk WAL",
            labelnames=("worker_id",),
            multiprocess_mode="liveall",
            registry=registry,
        )
        self.clickhouse_errors_total = Counter(
            "trace_worker_clickhouse_errors_total",
            "ClickHouse native TCP write/connection errors",
            labelnames=("error_type",),
            registry=registry,
        )

    def inc_spans_consumed(self, partition: int, count: int = 1) -> None:
        if count > 0:
            self.spans_consumed_total.labels(
                partition=str(partition),
                worker_id=self.worker_id,
            ).inc(count)

    def set_kafka_consumer_lag(self, partition: int, lag: int) -> None:
        self.kafka_consumer_lag.labels(partition=str(partition)).set(max(0, lag))

    def inc_dags_assembled(self, count: int = 1) -> None:
        if count > 0:
            self.dags_assembled_total.labels(worker_id=self.worker_id).inc(count)

    def inc_dag_evictions(self, status: str, count: int = 1) -> None:
        if count > 0:
            self.dag_evictions_total.labels(status=status).inc(count)

    def set_provenance_resolution_ratio(self, ratio: float) -> None:
        clamped = min(1.0, max(0.0, ratio))
        self.provenance_resolution_ratio.labels(worker_id=self.worker_id).set(clamped)

    def set_lru_hit_ratio(self, ratio: float) -> None:
        clamped = min(1.0, max(0.0, ratio))
        self.lru_hit_ratio.labels(worker_id=self.worker_id).set(clamped)

    def observe_clickhouse_flush_duration(self, seconds: float) -> None:
        self.clickhouse_flush_duration_seconds.labels(worker_id=self.worker_id).observe(
            max(0.0, seconds)
        )

    def inc_clickhouse_errors(self, error_type: str, count: int = 1) -> None:
        if count > 0:
            self.clickhouse_errors_total.labels(error_type=error_type).inc(count)

    def set_spillover_active(self, active: bool) -> None:
        self.spillover_active.labels(worker_id=self.worker_id).set(1.0 if active else 0.0)

    def set_jemalloc_resident_bytes(self, resident_bytes: int) -> None:
        self.jemalloc_resident_bytes.labels(worker_id=self.worker_id).set(max(0, resident_bytes))
