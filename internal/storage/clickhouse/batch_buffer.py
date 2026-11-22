"""Two-tier micro-batcher (50,000 rows or 2.0s) with atomic offsets and WAL spillover."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from typing import Protocol

from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from internal.config.settings import WorkerSettings
from internal.consumer.offset_manager import OffsetManager
from internal.storage.spillover.disk_wal import DiskWal, OffsetRecord
from internal.telemetry.logger import get_logger


class ClickHouseBatchWriter(Protocol):
    """Protocol for native ClickHouse batch insertion."""

    async def insert_graphs(self, graphs: Sequence[ProvenanceGraph]) -> int: ...


TraceDurableCallback = Callable[[Sequence[tuple[str, str]]], None]


class MicroBatchBuffer:
    """Accumulates ``ProvenanceGraph`` records and flushes via native TCP or Disk WAL."""

    def __init__(
        self,
        settings: WorkerSettings,
        client: ClickHouseBatchWriter,
        offset_manager: OffsetManager,
        disk_wal: DiskWal,
        on_traces_durable: TraceDurableCallback | None = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._offset_manager = offset_manager
        self._disk_wal = disk_wal
        self._on_traces_durable = on_traces_durable
        self._logger = get_logger("batch_buffer", stage="storage")

        self._graphs: list[ProvenanceGraph] = []
        self._offsets: list[OffsetRecord] = []
        self._graph_partitions: list[set[TopicPartition]] = []
        self._overflow_batches: deque[tuple[list[ProvenanceGraph], list[OffsetRecord]]] = deque()
        self._pending_flush_count = 0
        self._last_flush_ts = time.monotonic()
        self._flush_lock = asyncio.Lock()

    @property
    def defers_durability(self) -> bool:
        """Signal to TracePipeline that late offsets wait for ClickHouse ACK."""
        return True

    @property
    def buffered_rows(self) -> int:
        return len(self._graphs)

    @property
    def pending_batches(self) -> int:
        return self._pending_flush_count + len(self._overflow_batches)

    def is_backpressure_active(self) -> bool:
        """Return True when downstream ClickHouse or WAL limits require pausing Kafka."""
        return (
            self.pending_batches >= self._settings.batch_max_pending_batches
            or self._disk_wal.spillover_active
            or self._disk_wal.is_full()
        )

    def _mark_batch_durable(
        self,
        graphs: Sequence[ProvenanceGraph],
        offsets: Sequence[OffsetRecord],
    ) -> None:
        for tp, offset, gen in offsets:
            self._offset_manager.mark_durable(tp, offset, generation=gen)
        if self._on_traces_durable is not None and graphs:
            self._on_traces_durable([(g.tenant_id, g.trace_id) for g in graphs])

    async def emit(
        self,
        graph: ProvenanceGraph,
        contributing_offsets: tuple[tuple[TopicPartition, int], ...],
    ) -> None:
        """Buffer an assembled DAG and flush immediately if ``batch_max_rows`` is reached."""
        captured_offsets: list[OffsetRecord] = []
        parts: set[TopicPartition] = set()
        for tp, offset in contributing_offsets:
            gen = self._offset_manager.partition_generation(tp)
            captured_offsets.append((tp, offset, gen))
            parts.add(tp)

        self._graphs.append(graph)
        self._offsets.extend(captured_offsets)
        self._graph_partitions.append(parts)

        if len(self._graphs) >= self._settings.batch_max_rows:
            await self.flush()

    async def flush_if_due(self) -> int:
        """Flush buffered records if ``batch_flush_interval_seconds`` has elapsed."""
        if not self._graphs:
            return 0
        if (time.monotonic() - self._last_flush_ts) < self._settings.batch_flush_interval_seconds:
            return 0
        return await self.flush()

    async def flush(self) -> int:
        """Flush currently buffered records to ClickHouse, spooling to WAL on error."""
        async with self._flush_lock:
            if not self._graphs:
                return 0
            graphs = self._graphs
            offsets = self._offsets
            self._graphs = []
            self._offsets = []
            self._graph_partitions = []
            self._last_flush_ts = time.monotonic()
            self._pending_flush_count += 1

            try:
                written = await self._client.insert_graphs(graphs)
            except Exception as exc:
                self._logger.warning(
                    "clickhouse_flush_spooling_to_wal",
                    batch_rows=len(graphs),
                    error=type(exc).__name__,
                )
                if not self._disk_wal.write_batch(graphs, offsets):
                    self._overflow_batches.append((graphs, offsets))
                return 0
            finally:
                self._pending_flush_count = max(0, self._pending_flush_count - 1)

            self._mark_batch_durable(graphs, offsets)
            return written

    async def drain_wal(self) -> int:
        """Replay overflow and disk WAL segments in FIFO order once ClickHouse recovers."""
        drained_rows = 0
        async with self._flush_lock:
            while self._overflow_batches:
                graphs, offsets = self._overflow_batches[0]
                try:
                    written = await self._client.insert_graphs(graphs)
                except Exception:
                    return drained_rows
                self._overflow_batches.popleft()
                self._mark_batch_durable(graphs, offsets)
                drained_rows += written

            while True:
                segment = self._disk_wal.peek_oldest()
                if segment is None:
                    break
                try:
                    written = await self._client.insert_graphs(segment.graphs)
                except Exception:
                    break
                self._mark_batch_durable(segment.graphs, segment.offsets)
                self._disk_wal.ack_segment(segment)
                drained_rows += written

        return drained_rows

    def on_partitions_revoked(self, revoked_tps: Iterable[TopicPartition]) -> None:
        """Drop buffered offsets and un-flushed graphs belonging to revoked partitions."""
        revoked_set = set(revoked_tps)
        if not revoked_set:
            return

        if self._graphs:
            kept_graphs: list[ProvenanceGraph] = []
            kept_parts: list[set[TopicPartition]] = []
            for graph, parts in zip(self._graphs, self._graph_partitions, strict=True):
                if not (parts and parts.issubset(revoked_set)):
                    kept_graphs.append(graph)
                    kept_parts.append(parts)
            self._graphs = kept_graphs
            self._graph_partitions = kept_parts

        if self._offsets:
            self._offsets = [
                (tp, offset, gen) for tp, offset, gen in self._offsets if tp not in revoked_set
            ]

        if self._overflow_batches:
            for _graphs, offsets in self._overflow_batches:
                offsets[:] = [
                    (tp, offset, gen) for tp, offset, gen in offsets if tp not in revoked_set
                ]
