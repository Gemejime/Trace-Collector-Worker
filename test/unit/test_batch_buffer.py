"""Unit tests for MicroBatchBuffer two-tier flushing, offset durability, and WAL spillover."""

from __future__ import annotations

import time
from cmd.worker.process_runner import TracePipeline
from collections.abc import Sequence
from pathlib import Path

from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from api.proto.v1.trace_record_pb2 import CandidateTrace, TraceSpanRecord
from internal.config.settings import WorkerSettings
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.lru_cache import ChunkProvenance, ProvenanceLRUCache
from internal.provenance.redis_client import ProvenanceResolver
from internal.storage.clickhouse.batch_buffer import MicroBatchBuffer
from internal.storage.spillover.disk_wal import DiskWal


class _FakeClickHouseClient:
    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.batches: list[list[ProvenanceGraph]] = []

    async def insert_graphs(self, graphs: Sequence[ProvenanceGraph]) -> int:
        if self.should_fail:
            raise ConnectionError("clickhouse unavailable")
        self.batches.append(list(graphs))
        return len(graphs)


def _make_graph(trace_id: str) -> ProvenanceGraph:
    return ProvenanceGraph(
        trace_id=trace_id,
        tenant_id="tenant-a",
        event_date="2026-11-22",
        is_partial=False,
        total_duration_ms=10.0,
    )


async def test_batch_buffer_flushes_at_max_rows_and_marks_durable(tmp_path: Path) -> None:
    settings = WorkerSettings(
        batch_max_rows=3,
        batch_flush_interval_seconds=60.0,
        disk_wal_dir=str(tmp_path),
    )
    tp = TopicPartition("rag-diagnostic-traces", 0)
    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    for off in range(3):
        offsets.track_fetched(tp, off)

    client = _FakeClickHouseClient()
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)
    buf = MicroBatchBuffer(settings=settings, client=client, offset_manager=offsets, disk_wal=wal)

    await buf.emit(_make_graph("t0"), ((tp, 0),))
    await buf.emit(_make_graph("t1"), ((tp, 1),))
    assert offsets.committable_offsets() == {}
    assert len(client.batches) == 0

    await buf.emit(_make_graph("t2"), ((tp, 2),))
    assert len(client.batches) == 1
    assert len(client.batches[0]) == 3
    assert offsets.committable_offsets()[tp].offset == 3


async def test_batch_buffer_flushes_on_interval_and_respects_revocation(tmp_path: Path) -> None:
    settings = WorkerSettings(
        batch_max_rows=100,
        batch_flush_interval_seconds=0.05,
        disk_wal_dir=str(tmp_path),
    )
    tp0 = TopicPartition("rag-diagnostic-traces", 0)
    tp1 = TopicPartition("rag-diagnostic-traces", 1)
    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp0, tp1])
    offsets.track_fetched(tp0, 0)
    offsets.track_fetched(tp1, 0)

    client = _FakeClickHouseClient()
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)
    buf = MicroBatchBuffer(settings=settings, client=client, offset_manager=offsets, disk_wal=wal)

    await buf.emit(_make_graph("t0"), ((tp0, 0),))
    await buf.emit(_make_graph("t1"), ((tp1, 0),))
    buf.on_partitions_revoked([tp1])
    offsets.on_partitions_revoked([tp1])

    buf._last_flush_ts = time.monotonic() - 1.0
    written = await buf.flush_if_due()
    assert written == 1
    assert client.batches[0][0].trace_id == "t0"
    assert offsets.committable_offsets()[tp0].offset == 1
    assert tp1 not in offsets.committable_offsets()


async def test_batch_buffer_spools_to_wal_on_failure_and_drains_on_recovery(tmp_path: Path) -> None:
    settings = WorkerSettings(
        batch_max_rows=2,
        batch_flush_interval_seconds=10.0,
        disk_wal_dir=str(tmp_path),
    )
    tp = TopicPartition("rag-diagnostic-traces", 0)
    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    offsets.track_fetched(tp, 0)
    offsets.track_fetched(tp, 1)

    client = _FakeClickHouseClient(should_fail=True)
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)
    buf = MicroBatchBuffer(settings=settings, client=client, offset_manager=offsets, disk_wal=wal)

    await buf.emit(_make_graph("t0"), ((tp, 0),))
    await buf.emit(_make_graph("t1"), ((tp, 1),))

    assert wal.spillover_active is True
    assert wal.pending_segments == 1
    assert buf.is_backpressure_active() is True
    assert offsets.committable_offsets() == {}

    client.should_fail = False
    drained = await buf.drain_wal()
    assert drained == 2
    assert wal.spillover_active is False
    assert buf.is_backpressure_active() is False
    assert offsets.committable_offsets()[tp].offset == 2


async def test_late_duplicate_offset_waits_for_clickhouse_flush_ack(tmp_path: Path) -> None:
    settings = WorkerSettings(
        batch_max_rows=10,
        batch_flush_interval_seconds=10.0,
        disk_wal_dir=str(tmp_path),
    )
    tp = TopicPartition("rag-diagnostic-traces", 0)
    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    offsets.track_fetched(tp, 0)
    offsets.track_fetched(tp, 1)

    tw = BucketedTimeWheel(tick_ms=50, expiry_ms=500, start_ms=0)
    sm = DagStateMachine(timewheel=tw)
    lru = ProvenanceLRUCache(capacity=100)
    lru.put(1, "c1", ChunkProvenance(1, "h1", 0, (0.1, 0.1, 0.5, 0.5)))
    resolver = ProvenanceResolver(settings=settings, lru_cache=lru, redis_client=object())
    client = _FakeClickHouseClient()
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)

    pipeline = TracePipeline(state_machine=sm, provenance_resolver=resolver, offset_manager=offsets)
    buf = MicroBatchBuffer(
        settings=settings,
        client=client,
        offset_manager=offsets,
        disk_wal=wal,
        on_traces_durable=pipeline.mark_traces_durable,
    )
    pipeline.set_emit_sink(buf)

    s0 = TraceSpanRecord(
        trace_id="trace-dup",
        span_id="s0",
        tenant_id="tenant-a",
        is_terminal=True,
        candidates=[CandidateTrace(doc_id=1, chunk_id="c1", dense_rank=1)],
    )
    s1_late = TraceSpanRecord(
        trace_id="trace-dup",
        span_id="s1",
        tenant_id="tenant-a",
        is_terminal=True,
    )

    await pipeline.handle_span(tp, 0, s0)
    await pipeline.handle_span(tp, 1, s1_late)
    await pipeline.flush_pending_emissions()

    assert buf.buffered_rows == 1
    assert offsets.committable_offsets() == {}

    await buf.flush()
    assert offsets.committable_offsets()[tp].offset == 2
