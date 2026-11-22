"""Unit tests for ADR-001 contiguous per-partition OffsetManager."""

from __future__ import annotations

from aiokafka import TopicPartition

from internal.consumer.offset_manager import OffsetManager


def test_out_of_order_acks_respect_contiguous_boundary() -> None:
    mgr = OffsetManager()
    tp = TopicPartition("rag-diagnostic-traces", 0)
    mgr.on_partitions_assigned([tp])

    for off in (10, 11, 12, 13):
        mgr.track_fetched(tp, off)

    assert mgr.total_in_flight() == 4
    assert mgr.committable_offsets() == {}

    # ACK 11 and 12 while 10 is still open -> cannot advance past 10
    mgr.mark_durable(tp, 11)
    mgr.mark_durable(tp, 12)
    assert mgr.committable_offsets() == {}

    # ACK 10 -> contiguous watermark jumps to 12, so Kafka commit is 12 + 1 = 13
    mgr.mark_durable(tp, 10)
    commits = mgr.committable_offsets()
    assert commits[tp].offset == 13
    assert mgr.partition_in_flight(tp) == 1

    mgr.mark_committed(commits)
    assert mgr.committable_offsets() == {}


def test_quarantined_offset_unblocks_contiguous_watermark() -> None:
    mgr = OffsetManager()
    tp = TopicPartition("rag-diagnostic-traces", 3)
    mgr.track_fetched(tp, 50)
    mgr.track_fetched(tp, 51)
    mgr.track_fetched(tp, 52)

    mgr.mark_durable(tp, 50)
    mgr.mark_durable(tp, 52)
    assert mgr.committable_offsets()[tp].offset == 51

    # Quarantining corrupt offset 51 unblocks 51 and 52 -> commit 53
    mgr.mark_quarantined(tp, 51)
    assert mgr.committable_offsets()[tp].offset == 53
    assert mgr.total_in_flight() == 0


def test_partition_revocation_returns_final_commits_and_clears_state() -> None:
    mgr = OffsetManager()
    tp0 = TopicPartition("rag-diagnostic-traces", 0)
    tp1 = TopicPartition("rag-diagnostic-traces", 1)
    mgr.on_partitions_assigned([tp0, tp1])

    mgr.track_fetched(tp0, 100)
    mgr.track_fetched(tp0, 101)
    mgr.track_fetched(tp1, 200)
    mgr.track_fetched(tp1, 201)
    mgr.mark_durable(tp0, 100)
    mgr.mark_durable(tp1, 200)

    final_commits = mgr.on_partitions_revoked([tp0])
    assert final_commits[tp0].offset == 101
    assert tp1 not in final_commits
    assert mgr.partition_in_flight(tp0) == 0
    assert mgr.partition_in_flight(tp1) == 1
