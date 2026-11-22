"""Unit tests for KafkaSpanConsumer payload validation, quarantine, and backpressure."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest
from aiokafka import OffsetAndMetadata, TopicPartition

from api.proto.v1.trace_record_pb2 import TraceSpanRecord
from internal.config.settings import WorkerSettings
from internal.consumer.kafka_consumer import KafkaSpanConsumer
from internal.consumer.offset_manager import OffsetManager


@dataclass
class _FakeRecord:
    offset: int
    value: bytes | None


class _FakeAIOKafkaConsumer:
    def __init__(self, batches: list[dict[TopicPartition, list[_FakeRecord]]]) -> None:
        self._batches = list(batches)
        self._assignment: set[TopicPartition] = {TopicPartition("rag-diagnostic-traces", 0)}
        self._paused: set[TopicPartition] = set()
        self.committed: list[dict[TopicPartition, OffsetAndMetadata]] = []
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def subscribe(self, topics: list[str], listener: Any = None) -> None:
        self._topics = topics
        self._listener = listener

    def partitions_for_topic(self, _topic: str) -> set[int]:
        return set(range(32))

    def assignment(self) -> set[TopicPartition]:
        return set(self._assignment)

    def paused(self) -> set[TopicPartition]:
        return set(self._paused)

    def pause(self, *partitions: TopicPartition) -> None:
        self._paused.update(partitions)

    def resume(self, *partitions: TopicPartition) -> None:
        self._paused.difference_update(partitions)

    async def getmany(
        self, timeout_ms: int = 250, max_records: int = 500
    ) -> dict[TopicPartition, list[_FakeRecord]]:
        _ = (timeout_ms, max_records)
        if not self._batches:
            return {}
        return self._batches.pop(0)

    async def commit(self, offsets: dict[TopicPartition, OffsetAndMetadata]) -> None:
        self.committed.append(dict(offsets))


@pytest.mark.asyncio
async def test_consumer_quarantines_corrupt_and_oversized_records() -> None:
    tp = TopicPartition("rag-diagnostic-traces", 0)
    valid_span = TraceSpanRecord(
        trace_id="t-1",
        span_id="s-1",
        tenant_id="tenant-1",
        stage="dense_retrieval",
    ).SerializeToString()
    missing_fields_span = TraceSpanRecord(stage="dense_retrieval").SerializeToString()

    fake_consumer = _FakeAIOKafkaConsumer(
        batches=[
            {
                tp: [
                    _FakeRecord(offset=0, value=b"not-a-valid-protobuf-\xff\xff"),
                    _FakeRecord(offset=1, value=b"x" * 2048),
                    _FakeRecord(offset=2, value=missing_fields_span),
                    _FakeRecord(offset=3, value=valid_span),
                ]
            }
        ]
    )
    settings = WorkerSettings(
        kafka_max_payload_bytes=1024, kafka_offset_commit_interval_seconds=0.01
    )
    offset_mgr = OffsetManager()
    consumer = KafkaSpanConsumer(
        settings=settings, offset_manager=offset_mgr, consumer=fake_consumer
    )  # type: ignore[arg-type]

    await consumer.start()
    processed = await consumer.poll_once()
    await consumer.commit_durable_offsets()
    await consumer.stop()

    assert processed == 4
    assert consumer.quarantined_count == 3
    assert consumer.consumed_count == 1
    assert fake_consumer.committed[-1][tp].offset == 4


@pytest.mark.asyncio
async def test_consumer_backpressure_pauses_and_resumes_partitions() -> None:
    tp = TopicPartition("rag-diagnostic-traces", 0)
    valid_span = TraceSpanRecord(
        trace_id="t-2",
        span_id="s-2",
        tenant_id="tenant-1",
        stage="rrf_blend",
    ).SerializeToString()

    fake_consumer = _FakeAIOKafkaConsumer(
        batches=[
            {
                tp: [
                    _FakeRecord(offset=0, value=valid_span),
                    _FakeRecord(offset=1, value=valid_span),
                ]
            },
            {},
        ]
    )
    settings = WorkerSettings(kafka_max_in_flight_offsets=2)
    offset_mgr = OffsetManager()

    async def hold_in_flight(_tp: TopicPartition, _offset: int, _span: TraceSpanRecord) -> None:
        return None

    consumer = KafkaSpanConsumer(
        settings=settings,
        offset_manager=offset_mgr,
        span_handler=hold_in_flight,
        consumer=fake_consumer,  # type: ignore[arg-type]
    )

    await consumer.start()
    await consumer.poll_once()
    assert tp in fake_consumer.paused()

    # Confirm durable ACKs drop in-flight below low-water (70% of 2 = 1) -> resumes
    offset_mgr.mark_durable(tp, 0)
    offset_mgr.mark_durable(tp, 1)
    await consumer.poll_once()
    assert tp not in fake_consumer.paused()
