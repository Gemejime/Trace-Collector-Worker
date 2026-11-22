"""Per-process uvloop worker runtime for rag-trace-collector-worker."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import queue
import signal
import sys
import time
from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from api.proto.v1.trace_record_pb2 import TraceSpanRecord
from internal.config.settings import WorkerSettings
from internal.consumer.kafka_consumer import KafkaSpanConsumer
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine, PendingDag, build_provenance_graph
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.redis_client import ProvenanceResolver
from internal.storage.clickhouse.batch_buffer import MicroBatchBuffer
from internal.storage.clickhouse.native_client import ClickHouseNativeClient
from internal.storage.spillover.disk_wal import DiskWal
from internal.telemetry.jemalloc_stats import JemallocStatsScraper
from internal.telemetry.logger import configure_logging, get_logger
from internal.telemetry.metrics import MetricsRegistry, configure_prometheus_multiproc


@dataclass(slots=True, frozen=True)
class WorkerHeartbeat:
    """IPC heartbeat emitted from a worker subprocess to the supervisor."""

    worker_index: int
    pid: int
    timestamp: float
    status: str = "alive"
    kafka: str = "ok"
    redis: str = "ok"
    clickhouse: str = "ok"
    disk_wal: str = "ok"
    draining: bool = False


class DagEmitSink(Protocol):
    """Async sink receiving assembled and provenance-enriched DAGs."""

    async def emit(
        self,
        graph: ProvenanceGraph,
        contributing_offsets: tuple[tuple[TopicPartition, int], ...],
    ) -> None: ...


class OffsetDurabilitySink:
    """Phase 2 default sink marking contributing offsets durable via OffsetManager."""

    def __init__(self, offset_manager: OffsetManager) -> None:
        self._offset_manager = offset_manager

    async def emit(
        self,
        graph: ProvenanceGraph,
        contributing_offsets: tuple[tuple[TopicPartition, int], ...],
    ) -> None:
        _ = graph
        for tp, offset in contributing_offsets:
            self._offset_manager.mark_durable(tp, offset)


class TracePipeline:
    """Wires synchronous DAG state-machine ingestion with async provenance enrichment."""

    def __init__(
        self,
        state_machine: DagStateMachine,
        provenance_resolver: ProvenanceResolver,
        offset_manager: OffsetManager,
        emit_sink: DagEmitSink | None = None,
    ) -> None:
        self._state_machine = state_machine
        self._provenance_resolver = provenance_resolver
        self._offset_manager = offset_manager
        self._emit_sink: DagEmitSink = emit_sink or OffsetDurabilitySink(offset_manager)
        self._sink_accepts_emission_id = (
            "emission_id" in inspect.signature(self._emit_sink.emit).parameters
        )
        self._pending_tasks: set[asyncio.Task[None]] = set()
        self._task_metadata: dict[
            asyncio.Task[None], tuple[PendingDag, dict[TopicPartition, int | None], int]
        ] = {}
        self._next_emission_id = 1
        self._pending_emissions: dict[int, list[tuple[TopicPartition, int, int | None]]] = {}
        self._active_emission_by_trace: dict[tuple[str, str], int] = {}
        self._emission_trace_keys: dict[int, tuple[str, str]] = {}
        self._retry_dags: deque[tuple[PendingDag, dict[TopicPartition, int | None], int]] = deque()
        self._logger = get_logger("trace_pipeline", stage="dag_assembly")

    def set_emit_sink(self, emit_sink: DagEmitSink) -> None:
        """Attach or replace the downstream DAG emit sink."""
        self._emit_sink = emit_sink
        self._sink_accepts_emission_id = (
            "emission_id" in inspect.signature(emit_sink.emit).parameters
        )

    def _pop_emission_offsets(
        self, emission_id: int
    ) -> list[tuple[TopicPartition, int, int | None]]:
        trace_key = self._emission_trace_keys.pop(emission_id, None)
        if trace_key is not None and self._active_emission_by_trace.get(trace_key) == emission_id:
            self._active_emission_by_trace.pop(trace_key, None)
        return self._pending_emissions.pop(emission_id, [])

    def mark_traces_durable(
        self, trace_keys: Sequence[tuple[str, str] | tuple[str, str, int | None]]
    ) -> None:
        """Mark late duplicate span offsets durable once ClickHouse ACKs the batch."""
        for entry in trace_keys:
            if len(entry) == 3 and entry[2] is not None:
                waiting_offsets = self._pop_emission_offsets(entry[2])
                for late_tp, late_offset, late_gen in waiting_offsets:
                    self._offset_manager.mark_durable(late_tp, late_offset, generation=late_gen)
                continue

            trace_key = (entry[0], entry[1])
            matching_ids = [eid for eid, tk in self._emission_trace_keys.items() if tk == trace_key]
            for eid in matching_ids:
                waiting_offsets = self._pop_emission_offsets(eid)
                for late_tp, late_offset, late_gen in waiting_offsets:
                    self._offset_manager.mark_durable(late_tp, late_offset, generation=late_gen)

    def on_traces_discarded(
        self, discarded: Sequence[tuple[str, str] | tuple[str, str, int | None]]
    ) -> None:
        """Clear pending late offsets and completed markers when buffered graphs are revoked."""
        for entry in discarded:
            trace_key = (entry[0], entry[1])
            if len(entry) == 3 and entry[2] is not None:
                self._pop_emission_offsets(entry[2])
            else:
                matching_ids = [
                    eid for eid, tk in self._emission_trace_keys.items() if tk == trace_key
                ]
                for eid in matching_ids:
                    self._pop_emission_offsets(eid)
            self._state_machine.clear_completed(trace_key[0], trace_key[1])

    def _is_assignment_current(self, generations: dict[TopicPartition, int | None]) -> bool:
        for tp, expected_gen in generations.items():
            if (
                expected_gen is not None
                and self._offset_manager.partition_generation(tp) != expected_gen
            ):
                return False
        return True

    def _capture_generations(self, pending: PendingDag) -> dict[TopicPartition, int | None]:
        return {
            tp: self._offset_manager.partition_generation(tp)
            for tp, _ in pending.contributing_offsets
        }

    async def _enrich_and_emit(
        self,
        pending: PendingDag,
        generations: dict[TopicPartition, int | None],
        emission_id: int,
    ) -> None:
        trace_key = (pending.tenant_id, pending.trace_id)
        try:
            if not self._is_assignment_current(generations):
                self._pop_emission_offsets(emission_id)
                return
            resolution = await self._provenance_resolver.resolve_batch(pending.provenance_keys)
            if not self._is_assignment_current(generations):
                self._pop_emission_offsets(emission_id)
                return
            graph = build_provenance_graph(pending, resolution.resolved)
            if self._sink_accepts_emission_id:
                await cast(Any, self._emit_sink).emit(
                    graph,
                    pending.contributing_offsets,
                    emission_id=emission_id,
                )
            else:
                await self._emit_sink.emit(graph, pending.contributing_offsets)
        except asyncio.CancelledError:
            self._pop_emission_offsets(emission_id)
            self._state_machine.clear_completed(pending.tenant_id, pending.trace_id)
            raise
        except Exception as exc:
            self._logger.error(
                "dag_emission_failed",
                trace_id=pending.trace_id,
                tenant_id=pending.tenant_id,
                error=type(exc).__name__,
            )
            if self._is_assignment_current(generations):
                self._retry_dags.append((pending, generations, emission_id))
            else:
                self._pop_emission_offsets(emission_id)
                self._state_machine.clear_completed(pending.tenant_id, pending.trace_id)
            return

        if not getattr(self._emit_sink, "defers_durability", False):
            self.mark_traces_durable([(trace_key[0], trace_key[1], emission_id)])

    def _schedule_task(
        self,
        pending: PendingDag,
        generations: dict[TopicPartition, int | None],
        emission_id: int | None = None,
    ) -> None:
        trace_key = (pending.tenant_id, pending.trace_id)
        if emission_id is None:
            emission_id = self._next_emission_id
            self._next_emission_id += 1
            self._pending_emissions[emission_id] = []
            self._active_emission_by_trace[trace_key] = emission_id
            self._emission_trace_keys[emission_id] = trace_key
        task = asyncio.create_task(self._enrich_and_emit(pending, generations, emission_id))
        self._pending_tasks.add(task)
        self._task_metadata[task] = (pending, generations, emission_id)

        def _on_done(done_task: asyncio.Task[None]) -> None:
            self._pending_tasks.discard(done_task)
            self._task_metadata.pop(done_task, None)

        task.add_done_callback(_on_done)

    def _schedule_emissions(self, dags: Iterable[PendingDag]) -> int:
        scheduled = 0
        for pending in dags:
            generations = self._capture_generations(pending)
            self._schedule_task(pending, generations)
            scheduled += 1
        return scheduled

    def _drain_retry_dags(self) -> int:
        if not self._retry_dags:
            return 0
        count = 0
        batch = list(self._retry_dags)
        self._retry_dags.clear()
        for pending, generations, emission_id in batch:
            if self._is_assignment_current(generations):
                self._schedule_task(pending, generations, emission_id=emission_id)
                count += 1
            else:
                self._pop_emission_offsets(emission_id)
                self._state_machine.clear_completed(pending.tenant_id, pending.trace_id)
        return count

    async def handle_span(self, tp: TopicPartition, offset: int, span: TraceSpanRecord) -> None:
        """Synchronously correlate an incoming span without blocking the Kafka poll loop."""
        result = self._state_machine.ingest_span(tp, offset, span)
        trace_key = (span.tenant_id, span.trace_id)
        for durable_tp, durable_offset in result.immediate_durable_offsets:
            active_id = self._active_emission_by_trace.get(trace_key)
            waiting = self._pending_emissions.get(active_id) if active_id is not None else None
            if waiting is not None:
                waiting.append(
                    (
                        durable_tp,
                        durable_offset,
                        self._offset_manager.partition_generation(durable_tp),
                    )
                )
            else:
                self._offset_manager.mark_durable(durable_tp, durable_offset)
        if result.ready_dags:
            self._schedule_emissions(result.ready_dags)

    async def tick_and_flush(self, now_ms: int | None = None) -> int:
        """Advance the bucketed time-wheel and schedule emission for expired partial DAGs."""
        scheduled = self._drain_retry_dags()
        ts_ms = int(time.monotonic() * 1000) if now_ms is None else now_ms
        expired = self._state_machine.advance_time(ts_ms)
        if expired:
            scheduled += self._schedule_emissions(expired)
        return scheduled

    def on_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        """Drop in-memory open traces and cancel/drop emissions for revoked partitions."""
        revoked_set = set(revoked)
        self._state_machine.on_partitions_revoked(revoked_set)
        if not revoked_set:
            return

        for task, (pending, _generations, emission_id) in list(self._task_metadata.items()):
            if any(tp in revoked_set for tp, _ in pending.contributing_offsets):
                task.cancel()
                self._pop_emission_offsets(emission_id)
                self._state_machine.clear_completed(pending.tenant_id, pending.trace_id)

        if self._retry_dags:
            kept: deque[tuple[PendingDag, dict[TopicPartition, int | None], int]] = deque()
            for pending, generations, emission_id in self._retry_dags:
                if any(tp in revoked_set for tp, _ in pending.contributing_offsets):
                    self._pop_emission_offsets(emission_id)
                    self._state_machine.clear_completed(pending.tenant_id, pending.trace_id)
                else:
                    kept.append((pending, generations, emission_id))
            self._retry_dags = kept

    async def flush_pending_emissions(self) -> None:
        """Await all currently scheduled DAG enrichment/emission tasks and one retry pass."""
        self._drain_retry_dags()
        if not self._pending_tasks:
            return
        pending = list(self._pending_tasks)
        await asyncio.gather(*pending, return_exceptions=True)
        if self._retry_dags:
            self._drain_retry_dags()
            if self._pending_tasks:
                await asyncio.gather(*list(self._pending_tasks), return_exceptions=True)


def install_event_loop() -> str:
    """Install uvloop on POSIX platforms; fall back to asyncio on Windows."""
    if sys.platform != "win32":
        import uvloop

        uvloop.install()
        return "uvloop"
    return "asyncio"


def _emit_heartbeat(
    heartbeat_queue: Any,
    worker_index: int,
    status: str = "alive",
    kafka: str = "ok",
    redis: str = "ok",
    clickhouse: str = "ok",
    disk_wal: str = "ok",
    draining: bool = False,
) -> None:
    if heartbeat_queue is None:
        return
    hb = WorkerHeartbeat(
        worker_index=worker_index,
        pid=os.getpid(),
        timestamp=time.monotonic(),
        status=status,
        kafka=kafka,
        redis=redis,
        clickhouse=clickhouse,
        disk_wal=disk_wal,
        draining=draining,
    )
    with contextlib.suppress(queue.Full):
        heartbeat_queue.put_nowait(hb)


async def _bounded_ping(coro: Any, timeout_seconds: float) -> bool:
    try:
        return bool(await asyncio.wait_for(coro, timeout=timeout_seconds))
    except Exception:
        return False


async def _dependency_probe_loop(
    interval_seconds: float,
    probe_timeout_seconds: float,
    local_stop: asyncio.Event,
    external_stop_event: Any,
    consumer: KafkaSpanConsumer,
    provenance_resolver: ProvenanceResolver,
    ch_client: ClickHouseNativeClient,
    probe_status: dict[str, bool],
) -> None:
    while not local_stop.is_set() and not (
        external_stop_event is not None and external_stop_event.is_set()
    ):
        kafka_ok, redis_ok, ch_ok = await asyncio.gather(
            _bounded_ping(
                consumer.check_health(timeout_seconds=probe_timeout_seconds),
                probe_timeout_seconds,
            ),
            _bounded_ping(provenance_resolver.ping(), probe_timeout_seconds),
            _bounded_ping(ch_client.ping(), probe_timeout_seconds),
        )
        probe_status["kafka"] = kafka_ok
        probe_status["redis"] = redis_ok
        probe_status["clickhouse"] = ch_ok
        await asyncio.sleep(interval_seconds)


async def _heartbeat_loop(
    heartbeat_queue: Any,
    worker_index: int,
    interval_seconds: float,
    local_stop: asyncio.Event,
    external_stop_event: Any,
    consumer: KafkaSpanConsumer,
    disk_wal: DiskWal,
    jemalloc_scraper: JemallocStatsScraper,
    metrics: MetricsRegistry,
    probe_status: dict[str, bool],
) -> None:
    while not local_stop.is_set() and not (
        external_stop_event is not None and external_stop_event.is_set()
    ):
        wal_ok = disk_wal.is_healthy()
        kafka_ok = consumer.is_healthy() and probe_status["kafka"]
        redis_ok = probe_status["redis"]
        ch_ok = probe_status["clickhouse"]
        j_stats = jemalloc_scraper.scrape()
        metrics.set_jemalloc_allocated_bytes(j_stats.allocated_bytes)
        metrics.set_jemalloc_resident_bytes(j_stats.resident_bytes)
        _emit_heartbeat(
            heartbeat_queue,
            worker_index,
            status="running" if kafka_ok else "alive",
            kafka="ok" if kafka_ok else "error",
            redis="ok" if redis_ok else "error",
            clickhouse="ok" if ch_ok else "error",
            disk_wal="ok" if wal_ok else "error",
            draining=False,
        )
        await asyncio.sleep(interval_seconds)


async def _timewheel_tick_loop(
    pipeline: TracePipeline,
    tick_ms: int,
    local_stop: asyncio.Event,
    external_stop_event: Any,
) -> None:
    interval = tick_ms / 1000.0
    while not local_stop.is_set() and not (
        external_stop_event is not None and external_stop_event.is_set()
    ):
        await asyncio.sleep(interval)
        await pipeline.tick_and_flush()


async def _batch_flush_and_wal_loop(
    batch_buffer: MicroBatchBuffer,
    flush_interval_seconds: float,
    wal_drain_interval_seconds: float,
    local_stop: asyncio.Event,
    external_stop_event: Any,
) -> None:
    sleep_interval = min(0.25, max(0.05, flush_interval_seconds / 4.0))
    last_wal_drain = time.monotonic()
    while not local_stop.is_set() and not (
        external_stop_event is not None and external_stop_event.is_set()
    ):
        await asyncio.sleep(sleep_interval)
        await batch_buffer.flush_if_due()
        now = time.monotonic()
        if (now - last_wal_drain) >= wal_drain_interval_seconds:
            last_wal_drain = now
            await batch_buffer.drain_wal()


async def _run_worker_async(
    worker_index: int,
    settings: WorkerSettings,
    heartbeat_queue: Any,
    stop_event: Any,
) -> None:
    logger = get_logger("process_runner", stage="worker_lifecycle", worker_index=worker_index)
    local_stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        local_stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, ValueError):
            loop.add_signal_handler(sig, _request_stop)

    metrics = MetricsRegistry(
        worker_id=f"worker-{worker_index}",
        multiproc_dir=settings.prometheus_multiproc_dir,
    )
    jemalloc_scraper = JemallocStatsScraper()
    offset_manager = OffsetManager()
    start_ms = int(time.monotonic() * 1000)
    timewheel = BucketedTimeWheel(
        tick_ms=settings.timewheel_tick_ms,
        expiry_ms=int(settings.timewheel_expiry_seconds * 1000),
        start_ms=start_ms,
    )
    state_machine = DagStateMachine(
        timewheel=timewheel,
        max_open_traces=settings.timewheel_max_open_traces,
        metrics=metrics,
    )
    provenance_resolver = ProvenanceResolver(settings=settings, metrics=metrics)
    per_worker_wal_bytes = max(1, settings.disk_wal_max_bytes // settings.worker_process_count)
    disk_wal = DiskWal(
        wal_dir=settings.disk_wal_dir,
        worker_index=worker_index,
        max_bytes=per_worker_wal_bytes,
        metrics=metrics,
    )
    ch_client = ClickHouseNativeClient(settings=settings, metrics=metrics)
    try:
        await ch_client.ensure_schemas()
    except Exception:
        await provenance_resolver.close()
        await ch_client.close()
        raise

    pipeline = TracePipeline(
        state_machine=state_machine,
        provenance_resolver=provenance_resolver,
        offset_manager=offset_manager,
    )
    batch_buffer = MicroBatchBuffer(
        settings=settings,
        client=ch_client,
        offset_manager=offset_manager,
        disk_wal=disk_wal,
        on_traces_durable=pipeline.mark_traces_durable,
    )
    pipeline.set_emit_sink(batch_buffer)

    def _on_partitions_revoked(revoked: Iterable[TopicPartition]) -> None:
        revoked_list = list(revoked)
        pipeline.on_partitions_revoked(revoked_list)
        discarded = batch_buffer.on_partitions_revoked(revoked_list)
        if discarded:
            pipeline.on_traces_discarded(discarded)

    consumer = KafkaSpanConsumer(
        settings=settings,
        offset_manager=offset_manager,
        span_handler=pipeline.handle_span,
        on_revoked_hook=_on_partitions_revoked,
        backpressure_fn=batch_buffer.is_backpressure_active,
        metrics=metrics,
    )

    probe_status: dict[str, bool] = {
        "kafka": False,
        "redis": False,
        "clickhouse": True,
    }
    probe_timeout = max(0.5, min(settings.worker_heartbeat_interval_seconds, 2.0))
    probe_task = asyncio.create_task(
        _dependency_probe_loop(
            interval_seconds=settings.worker_heartbeat_interval_seconds,
            probe_timeout_seconds=probe_timeout,
            local_stop=local_stop,
            external_stop_event=stop_event,
            consumer=consumer,
            provenance_resolver=provenance_resolver,
            ch_client=ch_client,
            probe_status=probe_status,
        )
    )
    hb_task = asyncio.create_task(
        _heartbeat_loop(
            heartbeat_queue=heartbeat_queue,
            worker_index=worker_index,
            interval_seconds=settings.worker_heartbeat_interval_seconds,
            local_stop=local_stop,
            external_stop_event=stop_event,
            consumer=consumer,
            disk_wal=disk_wal,
            jemalloc_scraper=jemalloc_scraper,
            metrics=metrics,
            probe_status=probe_status,
        )
    )
    tick_task = asyncio.create_task(
        _timewheel_tick_loop(
            pipeline=pipeline,
            tick_ms=settings.timewheel_tick_ms,
            local_stop=local_stop,
            external_stop_event=stop_event,
        )
    )
    flush_task = asyncio.create_task(
        _batch_flush_and_wal_loop(
            batch_buffer=batch_buffer,
            flush_interval_seconds=settings.batch_flush_interval_seconds,
            wal_drain_interval_seconds=settings.disk_wal_drain_interval_seconds,
            local_stop=local_stop,
            external_stop_event=stop_event,
        )
    )

    try:
        await consumer.start()
        _emit_heartbeat(heartbeat_queue, worker_index, status="running")
        await consumer.run_until_stopped(
            lambda: local_stop.is_set() or (stop_event is not None and stop_event.is_set())
        )
    finally:
        local_stop.set()
        _emit_heartbeat(heartbeat_queue, worker_index, status="draining", draining=True)
        tick_task.cancel()
        flush_task.cancel()
        await asyncio.gather(tick_task, flush_task, return_exceptions=True)

        try:
            await asyncio.wait_for(
                pipeline.flush_pending_emissions(),
                timeout=settings.graceful_drain_timeout_seconds,
            )
            await asyncio.wait_for(
                batch_buffer.flush(),
                timeout=settings.graceful_drain_timeout_seconds,
            )
            await asyncio.wait_for(
                batch_buffer.drain_wal(),
                timeout=settings.graceful_drain_timeout_seconds,
            )
        except TimeoutError:
            logger.warning("worker_pipeline_drain_timed_out")

        try:
            await asyncio.wait_for(
                consumer.stop(),
                timeout=settings.graceful_drain_timeout_seconds,
            )
        except TimeoutError:
            logger.warning("worker_graceful_drain_timed_out")

        await provenance_resolver.close()
        await ch_client.close()
        probe_task.cancel()
        hb_task.cancel()
        await asyncio.gather(probe_task, hb_task, return_exceptions=True)


def run_worker_process(
    worker_index: int,
    heartbeat_queue: Any = None,
    stop_event: Any = None,
) -> None:
    """Subprocess entrypoint initializing event loop, settings, logging, and Kafka consumer."""
    loop_impl = install_event_loop()
    settings = WorkerSettings()
    configure_prometheus_multiproc(settings.prometheus_multiproc_dir)
    configure_logging(settings.log_level)
    logger = get_logger("process_runner", stage="startup", worker_index=worker_index)
    logger.info("worker_process_starting", event_loop=loop_impl)

    try:
        asyncio.run(
            _run_worker_async(
                worker_index=worker_index,
                settings=settings,
                heartbeat_queue=heartbeat_queue,
                stop_event=stop_event,
            )
        )
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("worker_process_exited")
