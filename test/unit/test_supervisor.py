"""Unit tests for WorkerSupervisor spawn, crash respawn isolation, and shutdown."""

from __future__ import annotations

import functools
import os
import time
from cmd.worker.main import WorkerSupervisor
from cmd.worker.process_runner import WorkerHeartbeat
from pathlib import Path
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
    crash_flag_path: str,
) -> None:
    if worker_index == 0:
        try:
            fd = os.open(crash_flag_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            # Crash immediately on first run for worker 0; sibling worker 1 stays alive.
            raise SystemExit(1)
        except FileExistsError:
            pass
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


def test_supervisor_respawns_crashed_worker_without_disturbing_sibling(
    tmp_path: Path,
) -> None:
    settings = WorkerSettings(
        worker_process_count=2,
        graceful_drain_timeout_seconds=3.0,
        worker_heartbeat_interval_seconds=0.1,
        worker_heartbeat_timeout_seconds=10.0,
        worker_respawn_initial_backoff_seconds=0.1,
        worker_respawn_max_backoff_seconds=1.0,
        worker_max_restarts_in_window=5,
    )
    dead_pids: list[int] = []
    crash_flag_path = str(tmp_path / "worker0_crashed.flag")
    worker_target = functools.partial(
        _dummy_worker_crash_index_zero_once,
        crash_flag_path=crash_flag_path,
    )
    supervisor = WorkerSupervisor(
        settings=settings,
        worker_target=worker_target,
        on_worker_dead=dead_pids.append,
    )

    supervisor.start()
    try:
        initial_pids = supervisor.active_worker_pids
        sibling_pid = initial_pids.get(1)
        assert sibling_pid is not None

        # Wait for worker 0 to crash, respawn, and emit a heartbeat from the replacement PID.
        deadline = time.monotonic() + 15.0
        replacement_pid: int | None = None
        while time.monotonic() < deadline:
            assert supervisor.monitor_once() is True
            current_pid_0 = supervisor.active_worker_pids.get(0)
            if (
                dead_pids
                and current_pid_0 is not None
                and current_pid_0 not in dead_pids
                and supervisor._workers[0].last_heartbeat_pid == current_pid_0
            ):
                replacement_pid = current_pid_0
                break
            time.sleep(0.05)

        assert len(dead_pids) == 1
        assert replacement_pid is not None
        current_pids = supervisor.active_worker_pids
        assert current_pids.get(1) == sibling_pid
        assert current_pids.get(0) == replacement_pid
    finally:
        supervisor.shutdown()
