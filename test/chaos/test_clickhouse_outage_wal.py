"""Chaos tests for ClickHouse outage WAL spooling, drain, and disk WAL exhaustion."""

from __future__ import annotations

import asyncio
from cmd.worker.process_runner import TracePipeline
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aiohttp.test_utils import TestClient, TestServer
from aiokafka import TopicPartition
from prometheus_client import CollectorRegistry

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from internal.config.settings import WorkerSettings
from internal.consumer.kafka_consumer import KafkaSpanConsumer
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.lru_cache import ChunkProvenance, ProvenanceLRUCache
from internal.provenance.redis_client import ProvenanceResolver
from internal.storage.clickhouse.batch_buffer import MicroBatchBuffer
from internal.storage.spillover.disk_wal import DiskWal
from internal.telemetry.health import TelemetryState, WorkerReadinessReport, build_telemetry_app
from internal.telemetry.metrics import MetricsRegistry
from test.fixtures.trace_builders import make_candidate, make_span_bytes


@dataclass
class _FakeKafkaMessage:
    offset: int
    value: bytes | None


class _ChaosKafkaConsumer:
    def __init__(self, tp: TopicPartition, batches: list[dict[TopicPartition, list[Any]]]) -> None:
        self._tp = tp
        self._batches = list(batches)
        self._paused: set[TopicPartition] = set()
        self.committed: list[dict[TopicPartition, Any]] = []
        self.getmany_calls = 0
        self.pause_calls = 0
        self.resume_calls = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def subscribe(self, _topics: list[str], listener: Any = None) -> None:
        _ = listener

    def partitions_for_topic(self, _topic: str) -> set[int]:
        return {self._tp.partition}

    def assignment(self) -> set[TopicPartition]:
        return {self._tp}

    def paused(self) -> set[TopicPartition]:
        return set(self._paused)

    def pause(self, *tps: TopicPartition) -> None:
        self.pause_calls += 1
        self._paused.update(tps)

    def resume(self, *tps: TopicPartition) -> None:
        self.resume_calls += 1
        self._paused.difference_update(tps)

    def highwater(self, _tp: TopicPartition) -> int | None:
        return None

    async def getmany(
        self, timeout_ms: int = 250, max_records: int = 500
    ) -> dict[TopicPartition, list[Any]]:
        _ = timeout_ms, max_records
        self.getmany_calls += 1
        if self._paused or not self._batches:
            return {}
        return self._batches.pop(0)

    async def commit(self, offsets: dict[TopicPartition, Any]) -> None:
        self.committed.append(dict(offsets))


class _ChaosClickHouseClient:
    def __init__(self, should_fail: bool = True) -> None:
        self.should_fail = should_fail
        self.batches: list[list[str]] = []

    async def insert_graphs(self, graphs: Sequence[ProvenanceGraph]) -> int:
        if self.should_fail:
            raise ConnectionError("simulated 60s clickhouse outage")
        self.batches.append([g.trace_id for g in graphs])
        return len(graphs)


def _metric_value(reg: CollectorRegistry, metric_name: str) -> float:
    for metric in reg.collect():
        if metric.name == metric_name:
            for sample in metric.samples:
                if sample.name == metric_name:
                    return float(sample.value)
    return 0.0


async def test_clickhouse_outage_wal_spool_heartbeat_and_exact_batch_drain(
    tmp_path: Path,
) -> None:
    """Simulate a ClickHouse outage, verify WAL spooling + paused poll heartbeats + exact drain."""
    tp = TopicPartition("rag-diagnostic-traces", 0)
    settings = WorkerSettings(
        batch_max_rows=2,
        batch_flush_interval_seconds=0.5,
        kafka_offset_commit_interval_seconds=0.01,
        disk_wal_dir=str(tmp_path),
        clickhouse_insert_deduplicate=True,
        clickhouse_async_insert=False,
    )
    assert settings.clickhouse_insert_deduplicate is True
    assert settings.clickhouse_async_insert is False

    reg = CollectorRegistry()
    metrics = MetricsRegistry(worker_id="worker-0", registry=reg)

    batch_records = [
        _FakeKafkaMessage(
            offset=idx,
            value=make_span_bytes(
                trace_id=f"outage-trace-{idx}",
                span_id=f"span-{idx}",
                tenant_id="tenant-chaos",
                is_terminal=True,
                candidates=[make_candidate(doc_id=1, chunk_id="c1")],
            ),
        )
        for idx in range(4)
    ]
    fake_kafka = _ChaosKafkaConsumer(
        tp=tp,
        batches=[
            {tp: batch_records[:2]},
            {tp: batch_records[2:]},
        ],
    )

    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    tw = BucketedTimeWheel(tick_ms=50, expiry_ms=500, start_ms=0)
    sm = DagStateMachine(timewheel=tw, metrics=metrics)
    lru = ProvenanceLRUCache(capacity=100)
    lru.put(1, "c1", ChunkProvenance(1, "ph-1", 0, (0.1, 0.1, 0.9, 0.9)))
    resolver = ProvenanceResolver(settings=settings, lru_cache=lru, redis_client=object())
    ch_client = _ChaosClickHouseClient(should_fail=True)
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=10 * 1024 * 1024, metrics=metrics)

    pipeline = TracePipeline(state_machine=sm, provenance_resolver=resolver, offset_manager=offsets)
    buffer = MicroBatchBuffer(
        settings=settings,
        client=ch_client,
        offset_manager=offsets,
        disk_wal=wal,
        on_traces_durable=pipeline.mark_traces_durable,
    )
    pipeline.set_emit_sink(buffer)

    consumer = KafkaSpanConsumer(
        settings=settings,
        offset_manager=offsets,
        span_handler=pipeline.handle_span,
        consumer=fake_kafka,
        backpressure_fn=buffer.is_backpressure_active,
        metrics=metrics,
    )
    await consumer.start()

    # Poll batch 1 -> ClickHouse fails -> spools to WAL.
    assert await consumer.poll_once() == 2
    await pipeline.flush_pending_emissions()
    assert wal.pending_segments == 1
    assert _metric_value(reg, "trace_worker_spillover_active") == 1.0
    assert fake_kafka.committed == []

    # Poll batch 2 -> backpressure pauses Kafka at end of poll; batch 2 also spools to WAL.
    assert await consumer.poll_once() == 2
    await pipeline.flush_pending_emissions()
    assert wal.pending_segments == 2
    assert fake_kafka.pause_calls == 1
    assert fake_kafka.committed == []

    # While paused during outage, poll_once() still calls getmany() to keep group heartbeats alive.
    prev_getmany = fake_kafka.getmany_calls
    for _ in range(3):
        assert await consumer.poll_once() == 0
    assert fake_kafka.getmany_calls == prev_getmany + 3
    assert fake_kafka.committed == []

    # ClickHouse recovers -> WAL drains preserving exact batch boundaries -> commit & resume.
    ch_client.should_fail = False
    drained = await buffer.drain_wal()
    assert drained == 4
    assert ch_client.batches == [
        ["outage-trace-0", "outage-trace-1"],
        ["outage-trace-2", "outage-trace-3"],
    ]
    assert _metric_value(reg, "trace_worker_spillover_active") == 0.0

    await asyncio.sleep(0.02)
    # Next poll commits durable offsets [0..3] -> offset 4, and resumes Kafka consumption.
    assert await consumer.poll_once() == 0
    assert fake_kafka.resume_calls == 1
    assert fake_kafka.committed[-1][tp].offset == 4


async def test_disk_wal_exhaustion_flips_readiness_to_503_and_pauses_kafka(
    tmp_path: Path,
) -> None:
    """Fill DiskWal to max_bytes and verify /ready returns 503 and backpressure activates."""
    tp = TopicPartition("rag-diagnostic-traces", 0)
    settings = WorkerSettings(
        batch_max_rows=1,
        disk_wal_dir=str(tmp_path),
        disk_wal_max_bytes=65_536,
    )
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=160)
    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    ch_client = _ChaosClickHouseClient(should_fail=True)
    buffer = MicroBatchBuffer(
        settings=settings,
        client=ch_client,
        offset_manager=offsets,
        disk_wal=wal,
    )

    g1 = ProvenanceGraph(
        trace_id="wal-fill-1",
        tenant_id="tenant-chaos",
        event_date="2026-11-22",
        is_partial=False,
        total_duration_ms=1.0,
    )
    g2 = ProvenanceGraph(
        trace_id="wal-fill-2-exceeds-quota-padding",
        tenant_id="tenant-chaos",
        event_date="2026-11-22",
        is_partial=False,
        total_duration_ms=1.0,
    )

    await buffer.emit(g1, ((tp, 0),))
    await buffer.emit(g2, ((tp, 1),))

    # First batch spooled to disk; second overflowed in-memory without exceeding disk quota.
    assert wal.total_bytes <= wal.max_bytes
    assert buffer.is_backpressure_active() is True
    assert buffer.pending_batches >= 1

    # Simulate exact quota exhaustion -> is_full() -> /ready flips to 503.
    wal._current_bytes = wal.max_bytes
    assert wal.is_full() is True
    assert wal.is_healthy() is False

    state = TelemetryState(expected_workers=1)
    state.update_worker(
        WorkerReadinessReport(
            worker_index=0,
            pid=1001,
            kafka="ok",
            redis="ok",
            clickhouse="error",
            disk_wal="ok" if wal.is_healthy() else "error",
        )
    )
    app = build_telemetry_app(state=state, active_workers_fn=lambda: 1)
    async with TestClient(TestServer(app)) as client:
        resp = await client.get("/ready")
        assert resp.status == 503
        body = await resp.json()
        assert body["status"] == "not_ready"
        assert body["checks"]["disk_wal"] == "error"
