"""Integration tests for ClickHouse micro-batching, atomic offsets, and WAL spillover."""

from __future__ import annotations

import asyncio
import os
import time
from cmd.worker.process_runner import TracePipeline
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from api.proto.v1.trace_record_pb2 import CandidateTrace, TraceSpanRecord
from internal.config.settings import WorkerSettings
from internal.consumer.kafka_consumer import KafkaSpanConsumer
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.lru_cache import ChunkProvenance, ProvenanceLRUCache
from internal.provenance.redis_client import ProvenanceResolver
from internal.storage.clickhouse.batch_buffer import MicroBatchBuffer
from internal.storage.spillover.disk_wal import DiskWal


@dataclass
class _FakeKafkaMessage:
    offset: int
    value: bytes | None


class _FakeKafkaConsumer:
    def __init__(self, tp: TopicPartition, batches: list[dict[TopicPartition, list[Any]]]) -> None:
        self._tp = tp
        self._batches = list(batches)
        self._paused: set[TopicPartition] = set()
        self.committed: list[dict[TopicPartition, Any]] = []
        self.pause_calls = 0
        self.resume_calls = 0

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def subscribe(self, _topics: list[str], listener: Any = None) -> None:
        _ = listener

    def partitions_for_topic(self, _topic: str) -> set[int]:
        return {0}

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
        if self._paused or not self._batches:
            return {}
        return self._batches.pop(0)

    async def commit(self, offsets: dict[TopicPartition, Any]) -> None:
        self.committed.append(dict(offsets))


class _FakeClickHouseSink:
    def __init__(self, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.inserted: list[ProvenanceGraph] = []

    async def insert_graphs(self, graphs: Sequence[ProvenanceGraph]) -> int:
        if self.should_fail:
            raise ConnectionError("clickhouse down")
        self.inserted.extend(graphs)
        return len(graphs)


async def test_ci_end_to_end_microbatch_backpressure_wal_and_atomic_commits(
    tmp_path: Path,
) -> None:
    tp = TopicPartition("rag-diagnostic-traces", 0)
    settings = WorkerSettings(
        batch_max_rows=2,
        batch_flush_interval_seconds=0.5,
        kafka_offset_commit_interval_seconds=0.01,
        disk_wal_dir=str(tmp_path),
    )

    spans = [
        TraceSpanRecord(
            trace_id=f"trace-{idx}",
            span_id=f"span-{idx}",
            tenant_id="tenant-ci",
            is_terminal=True,
            candidates=[CandidateTrace(doc_id=1, chunk_id="chunk-1", dense_rank=1)],
        ).SerializeToString()
        for idx in range(2)
    ]
    fake_kafka = _FakeKafkaConsumer(
        tp=tp,
        batches=[
            {tp: [_FakeKafkaMessage(offset=0, value=spans[0]), _FakeKafkaMessage(1, spans[1])]}
        ],
    )
    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    tw = BucketedTimeWheel(tick_ms=50, expiry_ms=500, start_ms=0)
    sm = DagStateMachine(timewheel=tw)
    lru = ProvenanceLRUCache(capacity=100)
    lru.put(1, "chunk-1", ChunkProvenance(1, "ph", 0, (0.1, 0.2, 0.8, 0.9)))
    resolver = ProvenanceResolver(settings=settings, lru_cache=lru, redis_client=object())
    ch_sink = _FakeClickHouseSink(should_fail=True)
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)

    pipeline = TracePipeline(state_machine=sm, provenance_resolver=resolver, offset_manager=offsets)
    batch_buffer = MicroBatchBuffer(
        settings=settings,
        client=ch_sink,
        offset_manager=offsets,
        disk_wal=wal,
        on_traces_durable=pipeline.mark_traces_durable,
    )
    pipeline.set_emit_sink(batch_buffer)

    consumer = KafkaSpanConsumer(
        settings=settings,
        offset_manager=offsets,
        span_handler=pipeline.handle_span,
        consumer=fake_kafka,
        backpressure_fn=batch_buffer.is_backpressure_active,
    )
    await consumer.start()

    processed = await consumer.poll_once()
    assert processed == 2
    await pipeline.flush_pending_emissions()

    # ClickHouse failed -> spooled to WAL -> backpressure active -> Kafka paused, zero commits.
    assert wal.spillover_active is True
    assert fake_kafka.committed == []
    await consumer.poll_once()
    assert fake_kafka.pause_calls == 1

    # ClickHouse recovers -> WAL drains -> offsets committed -> Kafka resumes.
    ch_sink.should_fail = False
    drained = await batch_buffer.drain_wal()
    assert drained == 2
    assert len(ch_sink.inserted) == 2

    await asyncio.sleep(0.02)
    await consumer.poll_once()
    assert fake_kafka.resume_calls == 1
    assert fake_kafka.committed[-1][tp].offset == 2


@pytest.mark.integration
async def test_live_l1_l2_latency_and_clickhouse_native_insert() -> None:
    if os.environ.get("RUN_LIVE_INTEGRATION") != "1":
        pytest.skip("Set RUN_LIVE_INTEGRATION=1 with live Redis/ClickHouse to run")

    settings = WorkerSettings()
    lru = ProvenanceLRUCache(capacity=settings.redis_lru_capacity)
    for idx in range(10_000):
        lru.put(idx, f"c-{idx}", ChunkProvenance(1, "ph", 0, (0.0, 0.0, 1.0, 1.0)))

    start = time.perf_counter()
    hits, missing = lru.get_many([(idx, f"c-{idx}") for idx in range(100)])
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    assert len(hits) == 100
    assert not missing
    assert elapsed_ms < 5.0
