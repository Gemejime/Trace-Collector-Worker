"""Multi-process supervisor entrypoint for rag-trace-collector-worker (ADR-002)."""

from __future__ import annotations

import contextlib
import multiprocessing
import queue
import signal
import sys
import time
from cmd.worker.process_runner import WorkerHeartbeat, run_worker_process
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from multiprocessing.context import SpawnContext, SpawnProcess
from typing import Any

from internal.config.settings import WorkerSettings
from internal.telemetry.logger import configure_logging, get_logger


@dataclass(slots=True)
class _ManagedWorker:
    """Supervisor tracking record for a single worker subprocess."""

    worker_index: int
    process: SpawnProcess | None = None
    last_heartbeat_ts: float = field(default_factory=time.monotonic)
    restart_timestamps: deque[float] = field(default_factory=deque)
    consecutive_failures: int = 0


class WorkerSupervisor:
    """Spawns, monitors, and respawns isolated worker subprocesses."""

    def __init__(
        self,
        settings: WorkerSettings | None = None,
        worker_target: Callable[..., Any] = run_worker_process,
        on_worker_dead: Callable[[int], None] | None = None,
    ) -> None:
        self._settings = settings or WorkerSettings()
        self._worker_target = worker_target
        self._on_worker_dead = on_worker_dead
        self._ctx: SpawnContext = multiprocessing.get_context("spawn")
        self._stop_event = self._ctx.Event()
        self._heartbeat_queue: Any = self._ctx.Queue(maxsize=1024)
        self._workers: dict[int, _ManagedWorker] = {}
        self._logger = get_logger("supervisor", stage="supervisor")
        self._shutdown_requested = False

    @property
    def active_worker_pids(self) -> dict[int, int]:
        """Return mapping of ``worker_index -> pid`` for currently alive workers."""
        result: dict[int, int] = {}
        for idx, slot in self._workers.items():
            if (
                slot.process is not None
                and slot.process.is_alive()
                and slot.process.pid is not None
            ):
                result[idx] = slot.process.pid
        return result

    def request_shutdown(self) -> None:
        """Signal all workers and the supervisor loop to begin graceful drain."""
        self._shutdown_requested = True
        self._stop_event.set()

    def _spawn_worker(self, worker_index: int) -> _ManagedWorker:
        proc = self._ctx.Process(
            target=self._worker_target,
            args=(worker_index, self._heartbeat_queue, self._stop_event),
            name=f"rag-trace-worker-{worker_index}",
            daemon=False,
        )
        proc.start()
        slot = self._workers.get(worker_index)
        if slot is None:
            slot = _ManagedWorker(worker_index=worker_index)
            self._workers[worker_index] = slot
        slot.process = proc
        slot.last_heartbeat_ts = time.monotonic()
        self._logger.info(
            "worker_spawned",
            worker_index=worker_index,
            child_pid=proc.pid,
        )
        return slot

    def start(self) -> None:
        """Spawn the configured pool of worker subprocesses."""
        for idx in range(self._settings.worker_process_count):
            self._spawn_worker(idx)

    def _drain_heartbeats(self) -> None:
        while True:
            try:
                hb: WorkerHeartbeat = self._heartbeat_queue.get_nowait()
            except queue.Empty:
                break
            slot = self._workers.get(hb.worker_index)
            if slot is not None:
                slot.last_heartbeat_ts = time.monotonic()
                slot.consecutive_failures = 0

    def _notify_dead_pid(self, pid: int | None) -> None:
        if pid is not None and self._on_worker_dead is not None:
            try:
                self._on_worker_dead(pid)
            except Exception as exc:
                self._logger.warning(
                    "dead_worker_cleanup_hook_failed",
                    child_pid=pid,
                    error=type(exc).__name__,
                )

    def _handle_worker_failure(self, slot: _ManagedWorker, reason: str) -> bool:
        now = time.monotonic()
        window = self._settings.worker_restart_window_seconds
        while slot.restart_timestamps and (now - slot.restart_timestamps[0]) > window:
            slot.restart_timestamps.popleft()

        if len(slot.restart_timestamps) >= self._settings.worker_max_restarts_in_window:
            self._logger.critical(
                "worker_crash_loop_limit_exceeded",
                worker_index=slot.worker_index,
                restarts_in_window=len(slot.restart_timestamps),
                window_seconds=window,
                reason=reason,
            )
            return False

        slot.restart_timestamps.append(now)
        slot.consecutive_failures += 1
        backoff = min(
            self._settings.worker_respawn_max_backoff_seconds,
            self._settings.worker_respawn_initial_backoff_seconds
            * (2 ** (slot.consecutive_failures - 1)),
        )
        self._logger.warning(
            "worker_respawning",
            worker_index=slot.worker_index,
            reason=reason,
            backoff_seconds=backoff,
            attempt=slot.consecutive_failures,
        )
        if backoff > 0 and not self._stop_event.is_set():
            time.sleep(min(backoff, 0.5) if self._shutdown_requested else backoff)
        if not self._stop_event.is_set():
            self._spawn_worker(slot.worker_index)
        return True

    def monitor_once(self) -> bool:
        """Check worker health and heartbeats once. Returns False if crash-looped."""
        self._drain_heartbeats()
        if self._shutdown_requested:
            return True

        now = time.monotonic()
        for slot in list(self._workers.values()):
            proc = slot.process
            if proc is None:
                continue

            if not proc.is_alive():
                exit_code = proc.exitcode
                dead_pid = proc.pid
                proc.join(timeout=0.1)
                self._notify_dead_pid(dead_pid)
                if not self._handle_worker_failure(slot, reason=f"process_exited_code_{exit_code}"):
                    return False
                continue

            if (now - slot.last_heartbeat_ts) > self._settings.worker_heartbeat_timeout_seconds:
                hung_pid = proc.pid
                self._logger.warning(
                    "worker_heartbeat_timeout",
                    worker_index=slot.worker_index,
                    child_pid=hung_pid,
                )
                proc.terminate()
                proc.join(timeout=2.0)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=1.0)
                self._notify_dead_pid(hung_pid)
                if not self._handle_worker_failure(slot, reason="heartbeat_timeout"):
                    return False

        return True

    def shutdown(self) -> None:
        """Gracefully drain all worker subprocesses, escalating to terminate/kill."""
        self.request_shutdown()
        deadline = time.monotonic() + self._settings.graceful_drain_timeout_seconds

        for slot in self._workers.values():
            proc = slot.process
            if proc is None:
                continue
            remaining = max(0.0, deadline - time.monotonic())
            proc.join(timeout=remaining)

        for slot in self._workers.values():
            proc = slot.process
            if proc is None:
                continue
            if proc.is_alive():
                self._logger.warning(
                    "worker_terminate_escalation",
                    worker_index=slot.worker_index,
                    child_pid=proc.pid,
                )
                proc.terminate()
                proc.join(timeout=2.0)
            if proc.is_alive():
                self._logger.error(
                    "worker_kill_escalation",
                    worker_index=slot.worker_index,
                    child_pid=proc.pid,
                )
                proc.kill()
                proc.join(timeout=1.0)
            self._notify_dead_pid(proc.pid)

    def run(self, poll_interval_seconds: float = 0.25) -> int:
        """Start workers, monitor until shutdown or crash-loop, and clean up."""

        def _signal_handler(_signum: int, _frame: Any) -> None:
            self.request_shutdown()

        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(ValueError, OSError):
                signal.signal(sig, _signal_handler)

        self.start()
        exit_code = 0
        try:
            while not self._shutdown_requested:
                if not self.monitor_once():
                    exit_code = 1
                    break
                time.sleep(poll_interval_seconds)
        finally:
            self.shutdown()
        return exit_code


def main() -> None:
    """CLI entrypoint for rag-trace-worker."""
    settings = WorkerSettings()
    configure_logging(settings.log_level)
    supervisor = WorkerSupervisor(settings=settings)
    sys.exit(supervisor.run())


if __name__ == "__main__":
    main()
