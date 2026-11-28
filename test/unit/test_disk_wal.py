"""Unit tests for worker-isolated bounded DiskWal."""

from __future__ import annotations

from pathlib import Path

from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from internal.storage.spillover.disk_wal import DiskWal


def _graph(trace_id: str) -> ProvenanceGraph:
    return ProvenanceGraph(
        trace_id=trace_id,
        tenant_id="tenant-a",
        event_date="2026-11-22",
        is_partial=False,
        total_duration_ms=5.0,
    )


def test_disk_wal_isolates_workers_and_drains_fifo(tmp_path: Path) -> None:
    wal0 = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)
    wal1 = DiskWal(wal_dir=tmp_path, worker_index=1, max_bytes=1024 * 1024)
    tp0 = TopicPartition("rag-diagnostic-traces", 0)
    tp1 = TopicPartition("rag-diagnostic-traces", 1)

    assert wal0.write_batch([_graph("t0")], [(tp0, 10, 1)]) is True
    assert wal0.write_batch([_graph("t1")], [(tp0, 11, 1)]) is True
    assert wal1.write_batch([_graph("t2")], [(tp1, 20, 1)]) is True

    assert wal0.pending_segments == 2
    assert wal1.pending_segments == 1
    assert wal0.worker_dir.name == "worker-0"
    assert wal1.worker_dir.name == "worker-1"

    seg0 = wal0.peek_oldest()
    assert seg0 is not None
    assert seg0.graphs[0].trace_id == "t0"
    assert seg0.offsets == ((tp0, 10, 1),)
    wal0.ack_segment(seg0)

    seg1 = wal0.peek_oldest()
    assert seg1 is not None
    assert seg1.graphs[0].trace_id == "t1"
    wal0.ack_segment(seg1)
    assert wal0.spillover_active is False


def test_disk_wal_enforces_byte_quota_without_dropping(tmp_path: Path) -> None:
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=120)
    tp = TopicPartition("rag-diagnostic-traces", 0)

    first = wal.write_batch([_graph("trace-first")], [(tp, 0, 1)])
    assert first is True
    second = wal.write_batch([_graph("trace-second-overflow-padding")], [(tp, 1, 1)])
    assert second is False
    assert wal.spillover_active is True


def test_disk_wal_read_error_does_not_quarantine_and_unlink_failure_retains_segment(
    tmp_path: Path, monkeypatch: object
) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    wal = DiskWal(wal_dir=tmp_path, worker_index=0, max_bytes=1024 * 1024)
    tp = TopicPartition("rag-diagnostic-traces", 0)
    assert wal.write_batch([_graph("t-retry")], [(tp, 5, 1)]) is True
    seg_path = wal._segments[0]

    def _raise_oserror(self: Path) -> bytes:
        raise OSError("transient read error")

    mp.setattr(Path, "read_bytes", _raise_oserror)
    assert wal.peek_oldest() is None
    assert seg_path.exists()
    assert not seg_path.with_suffix(".corrupt").exists()
    mp.undo()

    seg = wal.peek_oldest()
    assert seg is not None

    def _raise_unlink(self: Path, missing_ok: bool = False) -> None:
        raise OSError("unlink failed")

    mp.setattr(Path, "unlink", _raise_unlink)
    with pytest.raises(OSError):
        wal.ack_segment(seg)
    assert wal.pending_segments == 1
    assert wal.total_bytes == seg.size_bytes
    mp.undo()

    wal.ack_segment(seg)
    assert wal.pending_segments == 0
    assert wal.total_bytes == 0
