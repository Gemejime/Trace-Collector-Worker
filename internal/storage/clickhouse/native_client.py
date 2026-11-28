"""Non-blocking ClickHouse 24.3+ native TCP client (zero HTTP / single-row inserts)."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any

from clickhouse_driver import Client as ClickHouseDriverClient

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from internal.config.settings import WorkerSettings
from internal.storage.clickhouse.schemas import (
    build_insert_sql,
    build_schema_statements,
    serialize_provenance_graph_batch,
)
from internal.telemetry.logger import get_logger
from internal.telemetry.metrics import MetricsRegistry


class ClickHouseNativeClient:
    """Async-safe wrapper around ``clickhouse_driver.Client`` using ``asyncio.to_thread``."""

    def __init__(
        self,
        settings: WorkerSettings,
        client: Any | None = None,
        client_factory: Callable[[], Any] | None = None,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._client_factory = client_factory
        self._owns_client = client is None
        self._metrics = metrics
        self._io_lock = asyncio.Lock()
        self._insert_sql = build_insert_sql(settings.clickhouse_database)
        self._logger = get_logger("clickhouse_native_client", stage="storage")

    def _create_client(self, database: str | None = None) -> Any:
        if self._client_factory is not None:
            return self._client_factory()
        password = (
            self._settings.clickhouse_password.get_secret_value()
            if self._settings.clickhouse_password is not None
            else ""
        )
        kwargs: dict[str, Any] = {
            "host": self._settings.clickhouse_host,
            "port": self._settings.clickhouse_port,
            "database": database if database is not None else self._settings.clickhouse_database,
            "user": self._settings.clickhouse_user,
            "password": password,
            "connect_timeout": self._settings.clickhouse_connect_timeout_seconds,
            "send_receive_timeout": self._settings.clickhouse_send_receive_timeout_seconds,
            "secure": self._settings.clickhouse_tls_enabled,
            "settings": {
                "insert_deduplicate": 1 if self._settings.clickhouse_insert_deduplicate else 0,
                "async_insert": 0,
            },
        }
        if self._settings.clickhouse_tls_enabled and self._settings.clickhouse_ssl_cafile:
            kwargs["ca_certs"] = self._settings.clickhouse_ssl_cafile
        return ClickHouseDriverClient(**kwargs)

    def _get_or_create_client(self) -> Any:
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def _reset_client(self) -> None:
        if self._client is not None and self._owns_client:
            disconnect = getattr(self._client, "disconnect", None)
            if callable(disconnect):
                with contextlib.suppress(Exception):
                    disconnect()
            self._client = None

    def _execute_sync(
        self,
        query: str,
        params: Any = None,
        query_settings: dict[str, Any] | None = None,
    ) -> Any:
        client = self._get_or_create_client()
        if params is not None and query_settings is not None:
            return client.execute(query, params, settings=query_settings)
        if params is not None:
            return client.execute(query, params)
        if query_settings is not None:
            return client.execute(query, settings=query_settings)
        return client.execute(query)

    async def _execute_in_thread_locked(
        self,
        query: str,
        params: Any = None,
        query_settings: dict[str, Any] | None = None,
    ) -> Any:
        """Run ``_execute_sync`` in a worker thread while keeping ``_io_lock`` held."""
        exec_task = asyncio.create_task(
            asyncio.to_thread(self._execute_sync, query, params, query_settings)
        )
        cancelled: asyncio.CancelledError | None = None
        while not exec_task.done():
            try:
                await asyncio.shield(exec_task)
            except asyncio.CancelledError as exc:
                cancelled = exc
        if cancelled is not None:
            with contextlib.suppress(BaseException):
                exec_task.exception()
            raise cancelled
        return exec_task.result()

    async def ensure_schemas(self) -> None:
        """Create ClickHouse database, tables, and materialized views if missing."""
        statements = build_schema_statements(
            database=self._settings.clickhouse_database,
            replicated=self._settings.clickhouse_replicated,
            cluster_name=self._settings.clickhouse_cluster_name,
        )
        async with self._io_lock:
            remaining_statements = statements
            if (
                self._client is None
                and self._client_factory is None
                and self._settings.clickhouse_database != "default"
                and statements
            ):
                bootstrap_client = self._create_client(database="default")
                try:
                    await asyncio.to_thread(bootstrap_client.execute, statements[0])
                except Exception as exc:
                    if self._metrics is not None:
                        self._metrics.inc_clickhouse_errors(type(exc).__name__)
                    self._logger.error(
                        "clickhouse_ensure_schemas_failed",
                        error=type(exc).__name__,
                    )
                    raise
                finally:
                    disconnect = getattr(bootstrap_client, "disconnect", None)
                    if callable(disconnect):
                        with contextlib.suppress(Exception):
                            disconnect()
                remaining_statements = statements[1:]

            for stmt in remaining_statements:
                try:
                    await self._execute_in_thread_locked(stmt)
                except Exception as exc:
                    if self._metrics is not None:
                        self._metrics.inc_clickhouse_errors(type(exc).__name__)
                    self._logger.error(
                        "clickhouse_ensure_schemas_failed",
                        error=type(exc).__name__,
                    )
                    self._reset_client()
                    raise

    async def insert_graphs(
        self,
        graphs: Sequence[ProvenanceGraph],
        *,
        created_at: datetime | None = None,
    ) -> int:
        """Insert a micro-batch of ``ProvenanceGraph`` records via native TCP."""
        if not graphs:
            return 0
        rows = serialize_provenance_graph_batch(graphs, created_at=created_at)
        query_settings = {
            "insert_deduplicate": 1 if self._settings.clickhouse_insert_deduplicate else 0,
            "async_insert": 0,
        }
        started = time.monotonic()
        async with self._io_lock:
            try:
                await self._execute_in_thread_locked(
                    self._insert_sql,
                    rows,
                    query_settings,
                )
            except Exception as exc:
                if self._metrics is not None:
                    self._metrics.inc_clickhouse_errors(type(exc).__name__)
                self._logger.error(
                    "clickhouse_native_insert_failed",
                    batch_rows=len(rows),
                    error=type(exc).__name__,
                )
                self._reset_client()
                raise
        duration_s = time.monotonic() - started
        if self._metrics is not None:
            self._metrics.observe_clickhouse_flush_duration(duration_s)
        return len(rows)

    async def ping(self) -> bool:
        """Verify ClickHouse native TCP connectivity for readiness probes."""
        async with self._io_lock:
            try:
                result = await self._execute_in_thread_locked("SELECT 1")
                return bool(result)
            except Exception as exc:
                if self._metrics is not None:
                    self._metrics.inc_clickhouse_errors(type(exc).__name__)
                self._logger.warning("clickhouse_ping_failed", error=type(exc).__name__)
                self._reset_client()
                return False

    async def close(self) -> None:
        """Disconnect the owned ClickHouse native TCP client."""
        async with self._io_lock:
            self._reset_client()
