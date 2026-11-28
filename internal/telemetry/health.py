"""Async HTTP telemetry & Kubernetes probe server (/healthz, /ready, /metrics, /docs)."""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from aiohttp import web

from internal.telemetry.logger import get_logger
from internal.telemetry.metrics import generate_metrics_exposition

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

_SWAGGER_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>RAG Trace Collector Worker - API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      SwaggerUIBundle({ url: '/openapi.yaml', dom_id: '#swagger-ui' });
    };
  </script>
</body>
</html>
"""

_ALLOWED_CHECK_VALUES = frozenset({"ok", "error", "draining"})


def _sanitize_check(value: str) -> str:
    return value if value in _ALLOWED_CHECK_VALUES else "error"


@dataclass(slots=True, frozen=True)
class WorkerReadinessReport:
    """Sanitized per-worker dependency readiness reported over IPC."""

    worker_index: int
    pid: int
    kafka: str = "error"
    redis: str = "error"
    clickhouse: str = "error"
    disk_wal: str = "error"
    draining: bool = False


class TelemetryState:
    """Thread-safe shared state between supervisor monitor loop and HTTP probe server."""

    def __init__(self, expected_workers: int = 1) -> None:
        self._lock = threading.RLock()
        self._expected_workers = max(1, expected_workers)
        self._draining = False
        self._reports: dict[int, WorkerReadinessReport] = {}

    def set_draining(self, draining: bool) -> None:
        with self._lock:
            self._draining = draining

    def update_worker(self, report: WorkerReadinessReport) -> None:
        with self._lock:
            self._reports[report.worker_index] = report

    def clear_worker(self, worker_index: int) -> None:
        with self._lock:
            self._reports.pop(worker_index, None)

    def clear_pid(self, pid: int) -> None:
        with self._lock:
            to_remove = [idx for idx, rep in self._reports.items() if rep.pid == pid]
            for idx in to_remove:
                self._reports.pop(idx, None)

    def readiness_payload(self, active_workers: int) -> tuple[int, dict[str, Any]]:
        """Compute HTTP status code and sanitized ``/ready`` payload."""
        with self._lock:
            if self._draining or any(rep.draining for rep in self._reports.values()):
                return 503, {
                    "status": "draining",
                    "checks": {
                        "kafka": "draining",
                        "redis": _aggregate_check(self._reports, "redis"),
                        "clickhouse": _aggregate_check(self._reports, "clickhouse"),
                        "disk_wal": _aggregate_check(self._reports, "disk_wal"),
                    },
                }

            if active_workers <= 0 or not self._reports:
                return 503, {
                    "status": "not_ready",
                    "checks": {
                        "kafka": "error",
                        "redis": "error",
                        "clickhouse": "error",
                        "disk_wal": "error",
                    },
                }

            checks = {
                "kafka": _aggregate_check(self._reports, "kafka"),
                "redis": _aggregate_check(self._reports, "redis"),
                "clickhouse": _aggregate_check(self._reports, "clickhouse"),
                "disk_wal": _aggregate_check(self._reports, "disk_wal"),
            }
            if all(val == "ok" for val in checks.values()):
                return 200, {"status": "ready", "checks": checks}
            return 503, {"status": "not_ready", "checks": checks}


def _aggregate_check(reports: dict[int, WorkerReadinessReport], field_name: str) -> str:
    if not reports:
        return "error"
    values = [_sanitize_check(str(getattr(rep, field_name, "error"))) for rep in reports.values()]
    if any(val == "error" for val in values):
        return "error"
    if any(val == "draining" for val in values):
        return "draining"
    return "ok"


def _default_openapi_path() -> Path:
    return Path(__file__).resolve().parents[2] / "api" / "openapi" / "openapi.yaml"


def build_telemetry_app(
    state: TelemetryState,
    active_workers_fn: Callable[[], int],
    multiproc_dir: str = "",
    registry: CollectorRegistry | None = None,
    openapi_path: Path | None = None,
) -> web.Application:
    """Create the ``aiohttp`` application for Kubernetes probes and Prometheus scrapes."""
    spec_path = openapi_path or _default_openapi_path()
    app = web.Application()

    async def handle_healthz(_request: web.Request) -> web.Response:
        return web.json_response(
            {
                "status": "alive",
                "active_workers": max(0, active_workers_fn()),
            },
            status=200,
        )

    async def handle_ready(_request: web.Request) -> web.Response:
        status_code, payload = state.readiness_payload(active_workers_fn())
        return web.json_response(payload, status=status_code)

    async def handle_metrics(_request: web.Request) -> web.Response:
        payload = generate_metrics_exposition(multiproc_dir=multiproc_dir, registry=registry)
        return web.Response(
            body=payload,
            status=200,
            headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
        )

    async def handle_openapi(_request: web.Request) -> web.Response:
        if not spec_path.exists():
            return web.json_response({"error": "openapi_spec_unavailable"}, status=404)
        content = spec_path.read_text(encoding="utf-8")
        return web.Response(
            text=content,
            status=200,
            headers={"Content-Type": "application/yaml; charset=utf-8"},
        )

    async def handle_docs(_request: web.Request) -> web.Response:
        return web.Response(
            text=_SWAGGER_HTML,
            status=200,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

    app.router.add_get("/healthz", handle_healthz)
    app.router.add_get("/ready", handle_ready)
    app.router.add_get("/metrics", handle_metrics)
    app.router.add_get("/openapi.yaml", handle_openapi)
    app.router.add_get("/docs", handle_docs)
    return app


class TelemetryHttpServer:
    """Runs the async HTTP telemetry server in a dedicated daemon thread."""

    def __init__(
        self,
        host: str,
        port: int,
        state: TelemetryState,
        active_workers_fn: Callable[[], int],
        multiproc_dir: str = "",
        registry: CollectorRegistry | None = None,
        openapi_path: Path | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._state = state
        self._active_workers_fn = active_workers_fn
        self._multiproc_dir = multiproc_dir
        self._registry = registry
        self._openapi_path = openapi_path
        self._logger = get_logger("telemetry_http", stage="telemetry")

        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner: web.AppRunner | None = None
        self._thread: threading.Thread | None = None
        self._started_event = threading.Event()
        self._stop_requested = threading.Event()
        self._start_error: BaseException | None = None
        self._stop_future: asyncio.Future[None] | None = None
        self._bound_port: int = port

    @property
    def bound_port(self) -> int:
        return self._bound_port

    def start(self, timeout: float = 5.0) -> None:
        """Start the background event loop thread and bind the HTTP socket."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._started_event.clear()
        self._stop_requested.clear()
        self._start_error = None
        self._thread = threading.Thread(
            target=self._run_thread,
            name="telemetry-http-server",
            daemon=True,
        )
        self._thread.start()
        if not self._started_event.wait(timeout=timeout):
            self.stop()
            raise TimeoutError(
                f"TelemetryHttpServer failed to bind {self._host}:{self._port} within {timeout}s"
            )
        if self._start_error is not None:
            err = self._start_error
            self.stop()
            raise err

    def _run_thread(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._serve(loop))
        except BaseException as exc:
            self._start_error = exc
            self._started_event.set()
        finally:
            self._runner = None
            self._stop_future = None
            loop.close()
            self._loop = None

    async def _serve(self, loop: asyncio.AbstractEventLoop) -> None:
        self._stop_future = loop.create_future()
        if self._stop_requested.is_set():
            self._started_event.set()
            return
        app = build_telemetry_app(
            state=self._state,
            active_workers_fn=self._active_workers_fn,
            multiproc_dir=self._multiproc_dir,
            registry=self._registry,
            openapi_path=self._openapi_path,
        )
        runner = web.AppRunner(app, access_log=None)
        self._runner = runner
        try:
            await runner.setup()
            if self._stop_requested.is_set():
                self._started_event.set()
                return
            site = web.TCPSite(runner, host=self._host, port=self._port)
            await site.start()
            if runner.addresses:
                self._bound_port = int(runner.addresses[0][1])
            self._started_event.set()
            self._logger.info("telemetry_http_started", host=self._host, port=self._bound_port)
            if not self._stop_requested.is_set():
                await self._stop_future
        finally:
            await runner.cleanup()

    def stop(self) -> None:
        """Stop the HTTP server and join its background thread."""
        self._stop_requested.set()
        loop = self._loop
        stop_fut = self._stop_future
        if loop is not None and stop_fut is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(
                    lambda: None if stop_fut.done() else stop_fut.set_result(None)
                )
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None
