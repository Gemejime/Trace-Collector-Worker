"""Per-process uvloop worker runtime for rag-trace-collector-worker."""

from __future__ import annotations

import asyncio
import contextlib
import os
import queue
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any

from internal.config.settings import WorkerSettings
from internal.consumer.kafka_consumer import KafkaSpanConsumer
from internal.consumer.offset_manager import OffsetManager
from internal.telemetry.logger import configure_logging, get_logger


@dataclass(slots=True, frozen=True)
class WorkerHeartbeat:
    """IPC heartbeat emitted from a worker subprocess to the supervisor."""

    worker_index: int
    pid: int
    timestamp: float
    status: str = "alive"


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
    consumer = KafkaSpanConsumer(settings=settings, offset_manager=offset_manager)

    hb_task = asyncio.create_task(
        _heartbeat_loop(
            heartbeat_queue=heartbeat_queue,
            worker_index=worker_index,
            interval_seconds=settings.worker_heartbeat_interval_seconds,
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
        try:
            await asyncio.wait_for(
                consumer.stop(),
                timeout=settings.graceful_drain_timeout_seconds,
            )
        except TimeoutError:
            logger.warning("worker_graceful_drain_timed_out")
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
