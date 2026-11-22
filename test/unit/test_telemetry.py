"""Unit tests for Prometheus metrics, jemalloc scraper, and HTTP probe server."""

from __future__ import annotations

import ctypes
from pathlib import Path
from typing import Any

from aiohttp.test_utils import TestClient, TestServer
from prometheus_client import CollectorRegistry

from internal.telemetry.health import TelemetryState, WorkerReadinessReport, build_telemetry_app
from internal.telemetry.jemalloc_stats import JemallocStatsScraper
from internal.telemetry.metrics import MetricsRegistry


def test_metrics_registry_exposes_all_ten_contract_metrics() -> None:
    reg = CollectorRegistry()
    metrics = MetricsRegistry(worker_id="worker-0", registry=reg)

    metrics.inc_spans_consumed(partition=3, count=5)
    metrics.set_kafka_consumer_lag(partition=3, lag=120)
    metrics.inc_dags_assembled(2)
    metrics.inc_dag_evictions("partial", 1)
    metrics.set_provenance_resolution_ratio(0.999)
    metrics.set_lru_hit_ratio(0.92)
    metrics.observe_clickhouse_flush_duration(0.15)
    metrics.inc_clickhouse_errors("TimeoutError", 1)
    metrics.set_spillover_active(True)
    metrics.set_jemalloc_resident_bytes(42_000_000)

    sample_names = {metric.name for metric in reg.collect()}
    expected = {
        "trace_worker_spans_consumed",
        "trace_worker_dags_assembled",
        "trace_worker_dag_evictions",
        "trace_worker_provenance_resolution_ratio",
        "trace_worker_lru_hit_ratio",
        "trace_worker_clickhouse_flush_duration_seconds",
        "trace_worker_jemalloc_resident_bytes",
        "trace_worker_kafka_consumer_lag",
        "trace_worker_spillover_active",
        "trace_worker_clickhouse_errors",
    }
    assert expected.issubset(sample_names)


def test_jemalloc_scraper_fallback_and_mocked_mallctl() -> None:
    fallback = JemallocStatsScraper(mallctl_fn=None)
    if not fallback.available:
        stats = fallback.scrape()
        assert stats.resident_bytes == 0
        assert stats.available is False

    values = {
        b"stats.allocated": 100_000,
        b"stats.resident": 150_000,
        b"stats.mapped": 200_000,
    }

    def fake_mallctl(
        name: bytes,
        oldp: Any,
        _oldlenp: Any,
        _newp: Any,
        _newlen: int,
    ) -> int:
        if name == b"epoch":
            return 0
        if name in values:
            ctypes.cast(oldp, ctypes.POINTER(ctypes.c_size_t))[0] = values[name]
            return 0
        return 1

    scraper = JemallocStatsScraper(mallctl_fn=fake_mallctl)
    stats = scraper.scrape()
    assert stats.available is True
    assert stats.allocated_bytes == 100_000
    assert stats.resident_bytes == 150_000
    assert stats.mapped_bytes == 200_000


async def test_telemetry_http_endpoints(tmp_path: Path) -> None:
    reg = CollectorRegistry()
    metrics = MetricsRegistry(worker_id="worker-0", registry=reg)
    metrics.inc_spans_consumed(partition=0, count=1)

    state = TelemetryState(expected_workers=1)
    openapi_file = tmp_path / "openapi.yaml"
    openapi_file.write_text("openapi: 3.1.0\n", encoding="utf-8")

    active_count = 1
    app = build_telemetry_app(
        state=state,
        active_workers_fn=lambda: active_count,
        registry=reg,
        openapi_path=openapi_file,
    )

    async with TestClient(TestServer(app)) as client:
        resp_health = await client.get("/healthz")
        assert resp_health.status == 200
        assert await resp_health.json() == {"status": "alive", "active_workers": 1}

        resp_not_ready = await client.get("/ready")
        assert resp_not_ready.status == 503

        state.update_worker(
            WorkerReadinessReport(
                worker_index=0,
                pid=1234,
                kafka="ok",
                redis="ok",
                clickhouse="ok",
                disk_wal="ok",
                draining=False,
            )
        )
        resp_ready = await client.get("/ready")
        assert resp_ready.status == 200
        assert (await resp_ready.json())["status"] == "ready"

        state.update_worker(
            WorkerReadinessReport(
                worker_index=0,
                pid=1234,
                kafka="ok",
                redis="ok",
                clickhouse="error",
                disk_wal="ok",
                draining=False,
            )
        )
        resp_ch_err = await client.get("/ready")
        assert resp_ch_err.status == 503
        assert (await resp_ch_err.json())["checks"]["clickhouse"] == "error"

        state.set_draining(True)
        resp_drain = await client.get("/ready")
        assert resp_drain.status == 503
        assert (await resp_drain.json())["status"] == "draining"

        resp_metrics = await client.get("/metrics")
        assert resp_metrics.status == 200
        assert "trace_worker_spans_consumed_total" in await resp_metrics.text()

        resp_openapi = await client.get("/openapi.yaml")
        assert resp_openapi.status == 200
        assert "openapi: 3.1.0" in await resp_openapi.text()

        resp_docs = await client.get("/docs")
        assert resp_docs.status == 200
        assert "SwaggerUIBundle" in await resp_docs.text()
