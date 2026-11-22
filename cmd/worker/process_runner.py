"""Per-process uvloop worker runtime for rag-trace-collector-worker."""

from __future__ import annotations

import asyncio
import contextlib
import os
import queue
import signal
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from api.proto.v1.trace_record_pb2 import TraceSpanRecord
from internal.config.settings import WorkerSettings
from internal.consumer.kafka_consumer import KafkaSpanConsumer
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine, PendingDag, build_provenance_graph
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.redis_client import ProvenanceResolver
from internal.telemetry.logger import configure_logging, get_logger


@dataclass(slots=True, frozen=True)
class WorkerHeartbeat:
    """IPC heartbeat emitted from a worker subprocess to the supervisor."""

    worker_index: int
    pid: int
    timestamp: float
    status: str = "alive"


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
        self._pending_tasks: set[asyncio.Task[None]] = set()
        self._logger = get_logger("trace_pipeline", stage="dag_assembly")

    async def _enrich_and_emit(self, pending: PendingDag) -> None:
        try:
            resolution = await self._provenance_resolver.resolve_batch(pending.provenance_keys)
            graph = build_provenance_graph(pending, resolution.resolved)
            await self._emit_sink.emit(graph, pending.contributing_offsets)
        except Exception as exc:
            self._logger.error(
                "dag_emission_failed",
                trace_id=pending.trace_id,
                tenant_id=pending.tenant_id,
                error=type(exc).__name__,
            )

    def _schedule_emissions(self, dags: Iterable[PendingDag]) -> int:
        scheduled = 0
        for pending in dags:
            task = asyncio.create_task(self._enrich_and_emit(pending))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)
            scheduled += 1
        return scheduled

    async def handle_span(self, tp: TopicPartition, offset: int, span: TraceSpanRecord) -> None:
        """Synchronously correlate an incoming span without blocking the Kafka poll loop."""
        result = self._state_machine.ingest_span(tp, offset, span)
        for durable_tp, durable_offset in result.immediate_durable_offsets:
            self._offset_manager.mark_durable(durable_tp, durable_offset)
        if result.ready_dags:
            self._schedule_emissions(result.ready_dags)

    async def tick_and_flush(self, now_ms: int | None = None) -> int:
        """Advance the bucketed time-wheel and schedule emission for expired partial DAGs."""
        ts_ms = int(time.monotonic() * 1000) if now_ms is None else now_ms
        expired = self._state_machine.advance_time(ts_ms)
        if not expired:
            return 0
        return self._schedule_emissions(expired)

    def on_partitions_revoked(self, revoked: Iterable[TopicPartition]) -> None:
        """Drop in-memory open traces for revoked partitions so Kafka replays cleanly."""
        self._state_machine.on_partitions_revoked(revoked)

    async def flush_pending_emissions(self) -> None:
        """Await all currently scheduled DAG enrichment/emission tasks."""
        if not self._pending_tasks:
            return
        pending = list(self._pending_tasks)
        await asyncio.gather(*pending, return_exceptions=True)


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
) -> None:
    if heartbeat_queue is None:
        return
    hb = WorkerHeartbeat(
        worker_index=worker_index,
        pid=os.getpid(),
        timestamp=time.monotonic(),
        status=status,
    )
    with contextlib.suppress(queue.Full):
        heartbeat_queue.put_nowait(hb)


async def _heartbeat_loop(
    heartbeat_queue: Any,
    worker_index: int,
    interval_seconds: float,
    local_stop: asyncio.Event,
    external_stop_event: Any,
) -> None:
    while not local_stop.is_set() and not (
        external_stop_event is not None and external_stop_event.is_set()
    ):
        _emit_heartbeat(heartbeat_queue, worker_index, status="alive")
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
    )
    provenance_resolver = ProvenanceResolver(settings=settings)
    pipeline = TracePipeline(
        state_machine=state_machine,
        provenance_resolver=provenance_resolver,
        offset_manager=offset_manager,
    )
    consumer = KafkaSpanConsumer(
        settings=settings,
        offset_manager=offset_manager,
        span_handler=pipeline.handle_span,
        on_revoked_hook=pipeline.on_partitions_revoked,
    )

    hb_task = asyncio.create_task(
        _heartbeat_loop(
            heartbeat_queue=heartbeat_queue,
            worker_index=worker_index,
            interval_seconds=settings.worker_heartbeat_interval_seconds,
            local_stop=local_stop,
            external_stop_event=stop_event,
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

    try:
        await consumer.start()
        _emit_heartbeat(heartbeat_queue, worker_index, status="running")
        await consumer.run_until_stopped(
            lambda: local_stop.is_set() or (stop_event is not None and stop_event.is_set())
        )
    finally:
        local_stop.set()
        _emit_heartbeat(heartbeat_queue, worker_index, status="draining")
        tick_task.cancel()
        await asyncio.gather(tick_task, return_exceptions=True)

        try:
            await asyncio.wait_for(
                pipeline.flush_pending_emissions(),
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
        hb_task.cancel()
        await asyncio.gather(hb_task, return_exceptions=True)


def run_worker_process(
    worker_index: int,
    heartbeat_queue: Any = None,
    stop_event: Any = None,
) -> None:
    """Subprocess entrypoint initializing event loop, settings, logging, and Kafka consumer."""
    loop_impl = install_event_loop()
    settings = WorkerSettings()
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
