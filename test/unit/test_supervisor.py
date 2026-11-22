"""Unit tests for WorkerSupervisor spawn, crash respawn isolation, and shutdown."""

from __future__ import annotations

import os
import time
from cmd.worker.main import WorkerSupervisor
from cmd.worker.process_runner import WorkerHeartbeat
from typing import Any

from internal.config.settings import WorkerSettings


def _dummy_worker_run_until_stop(
    worker_index: int,
    heartbeat_queue: Any,
    stop_event: Any,
) -> None:
    while not stop_event.is_set():
        heartbeat_queue.put_nowait(
            WorkerHeartbeat(
                worker_index=worker_index,
                pid=os.getpid(),
                timestamp=time.monotonic(),
                status="alive",
            )
        )
        time.sleep(0.05)


def _dummy_worker_crash_index_zero_once(
    worker_index: int,
    heartbeat_queue: Any,
    stop_event: Any,
) -> None:
    marker_env = f"TEST_CRASHED_ONCE_{worker_index}"
    if worker_index == 0 and os.environ.get(marker_env) != "1":
        # Crash immediately on first run for worker 0; sibling worker 1 stays alive.
        raise SystemExit(1)
    _dummy_worker_run_until_stop(worker_index, heartbeat_queue, stop_event)


def test_supervisor_spawns_and_shuts_down_cleanly() -> None:
    settings = WorkerSettings(
        worker_process_count=2,
        graceful_drain_timeout_seconds=3.0,
        worker_heartbeat_interval_seconds=0.1,
        worker_heartbeat_timeout_seconds=5.0,
    )
    dead_pids: list[int] = []
    supervisor = WorkerSupervisor(
        settings=settings,
        worker_target=_dummy_worker_run_until_stop,
        on_worker_dead=dead_pids.append,
    )

    supervisor.start()
    try:
        time.sleep(0.3)
        pids = supervisor.active_worker_pids
        assert len(pids) == 2
        assert supervisor.monitor_once() is True
    finally:
        supervisor.shutdown()

    assert supervisor.active_worker_pids == {}
    assert len(dead_pids) == 2


def test_supervisor_respawns_crashed_worker_without_disturbing_sibling() -> None:
    settings = WorkerSettings(
        worker_process_count=2,
        graceful_drain_timeout_seconds=3.0,
        worker_heartbeat_interval_seconds=0.1,
        worker_heartbeat_timeout_seconds=5.0,
        worker_respawn_initial_backoff_seconds=0.1,
        worker_respawn_max_backoff_seconds=1.0,
        worker_max_restarts_in_window=5,
    )
    dead_pids: list[int] = []
    supervisor = WorkerSupervisor(
        settings=settings,
        worker_target=_dummy_worker_crash_index_zero_once,
        on_worker_dead=dead_pids.append,
    )

    supervisor.start()
    try:
        initial_pids = supervisor.active_worker_pids
        sibling_pid = initial_pids.get(1)
        assert sibling_pid is not None

        # Wait for worker 0 to crash and monitor_once to detect & respawn it
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            assert supervisor.monitor_once() is True
            if dead_pids:
                break
            time.sleep(0.05)

        assert len(dead_pids) >= 1
        current_pids = supervisor.active_worker_pids
        assert current_pids.get(1) == sibling_pid
        assert 0 in current_pids
    finally:
        supervisor.shutdown()
