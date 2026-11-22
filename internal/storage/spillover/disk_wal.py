"""Worker-isolated atomic local disk WAL for ClickHouse outage resilience."""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import msgpack
from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from internal.telemetry.logger import get_logger
from internal.telemetry.metrics import MetricsRegistry

OffsetRecord = tuple[TopicPartition, int, int | None]


@dataclass(slots=True, frozen=True)
class WalSegment:
    """Decoded WAL segment preserving exact batch boundaries and offset generations."""

    path: Path
    size_bytes: int
    graphs: tuple[ProvenanceGraph, ...]
    offsets: tuple[OffsetRecord, ...]
    created_at: datetime | None = None


class DiskWal:
    """Per-worker bounded atomic WAL segment writer and FIFO reader."""

    def __init__(
        self,
        wal_dir: str | Path,
        worker_index: int,
        max_bytes: int,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        self._worker_dir = Path(wal_dir) / f"worker-{worker_index}"
        self._worker_dir.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes
        self._metrics = metrics
        self._logger = get_logger("disk_wal", stage="spillover", worker_index=worker_index)
        self._next_seq = 1
        self._current_bytes = 0
        self._segments: list[Path] = []
        self._scan_existing_segments()

    def _scan_existing_segments(self) -> None:
        for tmp_file in self._worker_dir.glob("*.tmp"):
            tmp_file.unlink(missing_ok=True)

        segments = sorted(self._worker_dir.glob("seg-*.wal"))
        self._segments = segments
        self._current_bytes = sum(p.stat().st_size for p in segments if p.exists())
        if segments:
            last_name = segments[-1].stem
            try:
                self._next_seq = int(last_name.split("-", 1)[1]) + 1
            except (IndexError, ValueError):
                self._next_seq = len(segments) + 1
        self._update_metrics()

    def _update_metrics(self) -> None:
        if self._metrics is not None:
            self._metrics.set_spillover_active(self.spillover_active)

    @property
    def worker_dir(self) -> Path:
        return self._worker_dir

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def total_bytes(self) -> int:
        return self._current_bytes

    @property
    def pending_segments(self) -> int:
        return len(self._segments)

    @property
    def spillover_active(self) -> bool:
        return len(self._segments) > 0

    def is_full(self) -> bool:
        """Return True when the per-worker WAL byte quota is exhausted."""
        return self._current_bytes >= self._max_bytes

    def is_healthy(self) -> bool:
        """Return True when the WAL directory is writable and below its byte cap."""
        return (
            not self.is_full()
            and self._worker_dir.exists()
            and os.access(self._worker_dir, os.W_OK)
        )

    def write_batch(
        self,
        graphs: Sequence[ProvenanceGraph],
        offsets: Sequence[OffsetRecord],
        created_at: datetime | None = None,
    ) -> bool:
        """Atomically spool a micro-batch to disk. Returns False if quota would be exceeded."""
        if not graphs:
            return True

        payload = msgpack.packb(
            {
                "graphs": [g.SerializeToString() for g in graphs],
                "offsets": [[tp.topic, tp.partition, offset, gen] for tp, offset, gen in offsets],
                "created_at": created_at.isoformat() if created_at is not None else None,
            },
            use_bin_type=True,
        )
        if self._current_bytes + len(payload) > self._max_bytes:
            self._logger.error(
                "disk_wal_quota_exceeded",
                current_bytes=self._current_bytes,
                batch_bytes=len(payload),
                max_bytes=self._max_bytes,
            )
            return False

        seq = self._next_seq
        self._next_seq += 1
        final_path = self._worker_dir / f"seg-{seq:012d}.wal"
        tmp_path = self._worker_dir / f"seg-{seq:012d}.wal.tmp"

        with tmp_path.open("wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, final_path)

        self._segments.append(final_path)
        self._current_bytes += len(payload)
        self._update_metrics()
        self._logger.warning(
            "disk_wal_segment_written",
            segment_seq=seq,
            batch_rows=len(graphs),
            segment_bytes=len(payload),
            pending_segments=len(self._segments),
        )
        return True

    def peek_oldest(self) -> WalSegment | None:
        """Read and decode the oldest pending WAL segment without removing it."""
        while self._segments:
            path = self._segments[0]
            if not path.exists():
                self._segments.pop(0)
                continue
            try:
                raw = path.read_bytes()
                decoded = msgpack.unpackb(raw, raw=False)
                graphs: list[ProvenanceGraph] = []
                for g_bytes in decoded["graphs"]:
                    graph = ProvenanceGraph()
                    graph.ParseFromString(g_bytes)
                    graphs.append(graph)
                offsets: list[OffsetRecord] = [
                    (TopicPartition(str(topic), int(part)), int(offset), gen)
                    for topic, part, offset, gen in decoded["offsets"]
                ]
                raw_created_at = decoded.get("created_at")
                created_at = (
                    datetime.fromisoformat(raw_created_at)
                    if isinstance(raw_created_at, str) and raw_created_at
                    else None
                )
                return WalSegment(
                    path=path,
                    size_bytes=len(raw),
                    graphs=tuple(graphs),
                    offsets=tuple(offsets),
                    created_at=created_at,
                )
            except Exception as exc:
                self._logger.error(
                    "disk_wal_segment_corrupt",
                    error=type(exc).__name__,
                )
                corrupt_path = path.with_suffix(".corrupt")
                os.replace(path, corrupt_path)
                self._segments.pop(0)
                self._current_bytes = sum(p.stat().st_size for p in self._segments if p.exists())
                self._update_metrics()
        return None

    def ack_segment(self, segment: WalSegment) -> None:
        """Remove a WAL segment after ClickHouse confirms persistence."""
        if self._segments and self._segments[0] == segment.path:
            self._segments.pop(0)
        elif segment.path in self._segments:
            self._segments.remove(segment.path)

        segment.path.unlink(missing_ok=True)
        self._current_bytes = max(0, self._current_bytes - segment.size_bytes)
        self._update_metrics()
        self._logger.info(
            "disk_wal_segment_drained",
            batch_rows=len(segment.graphs),
            pending_segments=len(self._segments),
        )
