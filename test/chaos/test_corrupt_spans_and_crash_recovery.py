"""Chaos tests for corrupt payloads, missing spans, and worker crash/WAL recovery."""

from __future__ import annotations

import functools
import os
import time
from cmd.worker.main import WorkerSupervisor
from cmd.worker.process_runner import WorkerHeartbeat
from pathlib import Path
from typing import Any

from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from internal.config.settings import WorkerSettings
from internal.consumer.kafka_consumer import KafkaSpanConsumer
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine
from internal.graph.timewheel import BucketedTimeWheel
from internal.storage.clickhouse.batch_buffer import MicroBatchBuffer
from internal.storage.spillover.disk_wal import DiskWal
from test.fixtures.trace_builders import build_multistage_dag_spans


class _DummyMsg:
    def __init__(self, offset: int, value: bytes | None) -> None:
        self.offset = offset
        self.value = value


class _SingleBatchKafka:
    def __init__(self, tp: TopicPartition, records: list[_DummyMsg]) -> None:
        self._tp = tp
        self._records = records
        self._polled = False
        self.committed: list[dict[TopicPartition, Any]] = []

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
        return set()

    def highwater(self, _tp: TopicPartition) -> int | None:
        return None

    async def getmany(
        self, timeout_ms: int = 250, max_records: int = 500
    ) -> dict[TopicPartition, list[Any]]:
        _ = timeout_ms, max_records
        if self._polled:
            return {}
        self._polled = True
        return {self._tp: self._records}

    async def commit(self, offsets: dict[TopicPartition, Any]) -> None:
        self.committed.append(dict(offsets))


def _crash_once_worker(
    worker_index: int,
    heartbeat_queue: Any,
    stop_event: Any,
    crash_flag_path: str,
) -> None:
    if worker_index == 0:
        try:
            fd = os.open(crash_flag_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            raise SystemExit(137)
        except FileExistsError:
            pass
    while not stop_event.is_set():
        heartbeat_queue.put_nowait(
            WorkerHeartbeat(
                worker_index=worker_index,
                pid=os.getpid(),
                timestamp=time.monotonic(),
                status="running",
            )
        )
        time.sleep(0.05)


async def test_corrupt_payloads_and_missing_root_spans_emit_partial_dags() -> None:
    """Quarantine corrupt Protobufs and expire fragmented traces as is_partial=True."""
    tp = TopicPartition("rag-diagnostic-traces", 0)
    settings = WorkerSettings(kafka_offset_commit_interval_seconds=60.0)
    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    tw = BucketedTimeWheel(tick_ms=50, expiry_ms=200, start_ms=0)
    sm = DagStateMachine(timewheel=tw)

    # Drop root span (index 0) and terminal span (index 5); keep only intermediate spans.
    dag_spans = build_multistage_dag_spans(trace_id="frag-trace", doc_id=7, chunk_id="c7")
    intermediate_bytes = dag_spans[1].SerializeToString()

    async def _ingest_span(part: TopicPartition, offset: int, span: Any) -> None:
        res = sm.ingest_span(part, offset, span)
        for d_tp, d_off in res.immediate_durable_offsets:
            offsets.mark_durable(d_tp, d_off)

    kafka = _SingleBatchKafka(
        tp=tp,
        records=[
            _DummyMsg(0, b"\xff\xff\xff\x00invalid-protobuf"),
            _DummyMsg(1, None),
            _DummyMsg(2, intermediate_bytes),
        ],
    )
    consumer = KafkaSpanConsumer(
        settings=settings,
        offset_manager=offsets,
        span_handler=_ingest_span,
        consumer=kafka,
    )
    await consumer.start()
    processed = await consumer.poll_once()

    assert processed == 3
    assert consumer.quarantined_count == 2
    assert consumer.consumed_count == 1

    # Before expiry, offset 2 is still open -> cannot commit past offset 1 (next=2).
    assert offsets.committable_offsets()[tp].offset == 2

    # Advance time-wheel past 200ms -> fragmented trace expires with is_partial=True.
    expired = sm.advance_time(300)
    assert len(expired) == 1
    assert expired[0].is_partial is True
    assert expired[0].trace_id == "frag-trace"

    for d_tp, d_off in expired[0].contributing_offsets:
        offsets.mark_durable(d_tp, d_off)
    assert offsets.committable_offsets()[tp].offset == 3


async def test_undrained_wal_recovers_after_worker_restart(tmp_path: Path) -> None:
    """Verify a restarted worker's DiskWal recovers and drains un-acked segments."""
    tp = TopicPartition("rag-diagnostic-traces", 0)
    wal_before = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)
    graph = ProvenanceGraph(
        trace_id="crash-wal-trace",
        tenant_id="tenant-chaos",
        event_date="2026-11-22",
        is_partial=False,
        total_duration_ms=4.2,
    )
    assert wal_before.write_batch([graph], [(tp, 42, 1)]) is True
    assert wal_before.pending_segments == 1

    # Simulate worker restart: new DiskWal + MicroBatchBuffer pointing at same wal_dir.
    wal_after = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)
    assert wal_after.pending_segments == 1
    assert wal_after.spillover_active is True

    recovered: list[str] = []

    class _RecoveredClickHouse:
        async def insert_graphs(self, graphs: list[ProvenanceGraph] | Any) -> int:
            recovered.extend(g.trace_id for g in graphs)
            return len(graphs)

    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    offsets.track_fetched(tp, 42)
    buffer = MicroBatchBuffer(
        settings=WorkerSettings(disk_wal_dir=str(tmp_path)),
        client=_RecoveredClickHouse(),
        offset_manager=offsets,
        disk_wal=wal_after,
    )
    drained = await buffer.drain_wal()
    assert drained == 1
    assert recovered == ["crash-wal-trace"]
    assert wal_after.pending_segments == 0
    assert wal_after.spillover_active is False


def test_supervisor_crash_recovery_cleans_prometheus_shard(tmp_path: Path) -> None:
    """Verify WorkerSupervisor respawns crashed worker and cleans dead PID shard."""
    multiproc_dir = tmp_path / "prom"
    multiproc_dir.mkdir()
    settings = WorkerSettings(
        worker_process_count=2,
        graceful_drain_timeout_seconds=3.0,
        worker_heartbeat_interval_seconds=0.1,
        worker_heartbeat_timeout_seconds=90.0,
        worker_respawn_initial_backoff_seconds=0.1,
        worker_respawn_max_backoff_seconds=1.0,
        prometheus_multiproc_dir=str(multiproc_dir),
    )
    dead_pids: list[int] = []
    crash_flag = str(tmp_path / "crash.flag")
    supervisor = WorkerSupervisor(
        settings=settings,
        worker_target=functools.partial(_crash_once_worker, crash_flag_path=crash_flag),
        on_worker_dead=dead_pids.append,
    )
    supervisor.start()
    try:
        sibling_pid = supervisor.active_worker_pids.get(1)
        assert sibling_pid is not None
        deadline = time.monotonic() + 60.0
        replacement_pid: int | None = None
        while time.monotonic() < deadline:
            assert supervisor.monitor_once() is True
            pid0 = supervisor.active_worker_pids.get(0)
            if (
                dead_pids
                and pid0 is not None
                and pid0 not in dead_pids
                and supervisor._workers[0].last_heartbeat_pid == pid0
            ):
                replacement_pid = pid0
                break
            time.sleep(0.05)
        assert len(dead_pids) == 1
        assert replacement_pid is not None
        assert supervisor.active_worker_pids.get(1) == sibling_pid
    finally:
        supervisor.shutdown()
