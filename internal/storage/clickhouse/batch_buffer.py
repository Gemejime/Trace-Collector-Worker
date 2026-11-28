"""Two-tier micro-batcher (50,000 rows or 2.0s) with atomic offsets and WAL spillover."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections import deque
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from internal.config.settings import WorkerSettings
from internal.consumer.offset_manager import OffsetManager
from internal.storage.spillover.disk_wal import DiskWal, OffsetRecord
from internal.telemetry.logger import get_logger


class ClickHouseBatchWriter(Protocol):
    """Protocol for native ClickHouse batch insertion."""

    async def insert_graphs(self, graphs: Sequence[ProvenanceGraph]) -> int: ...


TraceDurableEntry = tuple[str, str] | tuple[str, str, int | None]
TraceDurableCallback = Callable[[Sequence[TraceDurableEntry]], None]


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
        self._supports_created_at = (
            "created_at" in inspect.signature(client.insert_graphs).parameters
        )
        self._offset_manager = offset_manager
        self._disk_wal = disk_wal
        self._on_traces_durable = on_traces_durable
        self._logger = get_logger("batch_buffer", stage="storage")

        self._graphs: list[ProvenanceGraph] = []
        self._offsets: list[OffsetRecord] = []
        self._graph_partitions: list[set[TopicPartition]] = []
        self._emission_ids: list[int | None] = []
        self._overflow_batches: deque[
            tuple[list[ProvenanceGraph], list[OffsetRecord], list[int | None], datetime]
        ] = deque()
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

    async def _insert_batch(
        self,
        graphs: Sequence[ProvenanceGraph],
        created_at: datetime,
    ) -> int:
        if self._supports_created_at:
            return int(await cast(Any, self._client).insert_graphs(graphs, created_at=created_at))
        return await self._client.insert_graphs(graphs)

    def _mark_batch_durable(
        self,
        graphs: Sequence[ProvenanceGraph],
        offsets: Sequence[OffsetRecord],
        emission_ids: Sequence[int | None] | None = None,
    ) -> None:
        for tp, offset, gen in offsets:
            self._offset_manager.mark_durable(tp, offset, generation=gen)
        if self._on_traces_durable is not None and graphs:
            if emission_ids is not None:
                entries: list[TraceDurableEntry] = [
                    (g.tenant_id, g.trace_id, eid)
                    for g, eid in zip(graphs, emission_ids, strict=True)
                ]
                self._on_traces_durable(entries)
            else:
                self._on_traces_durable([(g.tenant_id, g.trace_id) for g in graphs])

    async def emit(
        self,
        graph: ProvenanceGraph,
        contributing_offsets: tuple[tuple[TopicPartition, int], ...],
        *,
        emission_id: int | None = None,
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
        self._emission_ids.append(emission_id)

        if len(self._graphs) >= self._settings.batch_max_rows:
            await self.flush()

    async def flush_if_due(self) -> int:
        """Flush buffered records if ``batch_flush_interval_seconds`` has elapsed."""
        if not self._graphs:
            return 0
        if (time.monotonic() - self._last_flush_ts) < self._settings.batch_flush_interval_seconds:
            return 0
        return await self.flush()

    def _spool_or_retain(
        self,
        graphs: list[ProvenanceGraph],
        offsets: list[OffsetRecord],
        emission_ids: list[int | None],
        batch_created_at: datetime,
    ) -> None:
        """Spool a detached batch to disk WAL, retaining it in memory on failure or quota hit."""
        try:
            spooled = self._disk_wal.write_batch(graphs, offsets, created_at=batch_created_at)
        except Exception:
            self._overflow_batches.append((graphs, offsets, emission_ids, batch_created_at))
            raise
        if not spooled:
            self._overflow_batches.append((graphs, offsets, emission_ids, batch_created_at))

    async def flush(self) -> int:
        """Flush currently buffered records to ClickHouse, spooling to WAL on error."""
        async with self._flush_lock:
            if not self._graphs:
                return 0
            graphs = self._graphs
            offsets = self._offsets
            emission_ids = self._emission_ids
            self._graphs = []
            self._offsets = []
            self._graph_partitions = []
            self._emission_ids = []
            batch_created_at = datetime.now(tz=UTC)
            self._last_flush_ts = time.monotonic()
            self._pending_flush_count += 1

            try:
                written = await self._insert_batch(graphs, batch_created_at)
            except asyncio.CancelledError:
                self._logger.warning(
                    "clickhouse_flush_cancelled_spooling_to_wal",
                    batch_rows=len(graphs),
                )
                self._spool_or_retain(graphs, offsets, emission_ids, batch_created_at)
                raise
            except Exception as exc:
                self._logger.warning(
                    "clickhouse_flush_spooling_to_wal",
                    batch_rows=len(graphs),
                    error=type(exc).__name__,
                )
                self._spool_or_retain(graphs, offsets, emission_ids, batch_created_at)
                return 0
            finally:
                self._pending_flush_count = max(0, self._pending_flush_count - 1)

            self._mark_batch_durable(graphs, offsets, emission_ids)
            return written

    async def drain_wal(self) -> int:
        """Replay overflow and disk WAL segments in FIFO order once ClickHouse recovers."""
        drained_rows = 0
        async with self._flush_lock:
            while self._overflow_batches:
                graphs, offsets, emission_ids, batch_created_at = self._overflow_batches[0]
                try:
                    written = await self._insert_batch(graphs, batch_created_at)
                except Exception:
                    return drained_rows
                self._overflow_batches.popleft()
                self._mark_batch_durable(graphs, offsets, emission_ids)
                drained_rows += written

            while True:
                segment = self._disk_wal.peek_oldest()
                if segment is None:
                    break
                seg_created_at = segment.created_at or datetime.now(tz=UTC)
                try:
                    written = await self._insert_batch(segment.graphs, seg_created_at)
                except Exception:
                    break
                self._mark_batch_durable(segment.graphs, segment.offsets)
                self._disk_wal.ack_segment(segment)
                drained_rows += written

        return drained_rows

    def on_partitions_revoked(
        self, revoked_tps: Iterable[TopicPartition]
    ) -> list[tuple[str, str, int | None]]:
        """Drop buffered offsets and un-flushed graphs belonging to revoked partitions."""
        revoked_set = set(revoked_tps)
        if not revoked_set:
            return []

        discarded_traces: list[tuple[str, str, int | None]] = []
        if self._graphs:
            kept_graphs: list[ProvenanceGraph] = []
            kept_parts: list[set[TopicPartition]] = []
            kept_eids: list[int | None] = []
            for graph, parts, eid in zip(
                self._graphs,
                self._graph_partitions,
                self._emission_ids,
                strict=True,
            ):
                if parts and parts.issubset(revoked_set):
                    discarded_traces.append((graph.tenant_id, graph.trace_id, eid))
                else:
                    kept_graphs.append(graph)
                    kept_parts.append(parts)
                    kept_eids.append(eid)
            self._graphs = kept_graphs
            self._graph_partitions = kept_parts
            self._emission_ids = kept_eids

        if self._offsets:
            self._offsets = [
                (tp, offset, gen) for tp, offset, gen in self._offsets if tp not in revoked_set
            ]

        if self._overflow_batches:
            for _graphs, offsets, _eids, _created_at in self._overflow_batches:
                offsets[:] = [
                    (tp, offset, gen) for tp, offset, gen in offsets if tp not in revoked_set
                ]

        return discarded_traces
