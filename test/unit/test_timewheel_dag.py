"""Unit tests for BucketedTimeWheel, DagStateMachine, and TracePipeline."""

from __future__ import annotations

from cmd.worker.process_runner import TracePipeline

import msgpack
import pytest
from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from api.proto.v1.trace_record_pb2 import CandidateTrace, TraceSpanRecord
from internal.config.settings import WorkerSettings
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine, build_provenance_graph
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.lru_cache import ChunkProvenance
from internal.provenance.redis_client import ProvenanceResolver


def _make_span(
    trace_id: str,
    span_id: str,
    *,
    tenant_id: str = "tenant-a",
    stage: str = "dense_retrieval",
    start_ns: int = 1_700_000_000_000_000_000,
    end_ns: int = 1_700_000_000_050_000_000,
    is_terminal: bool = False,
    expected_span_count: int = 0,
    candidates: list[CandidateTrace] | None = None,
    eval_tags: dict[str, str] | None = None,
) -> TraceSpanRecord:
    return TraceSpanRecord(
        trace_id=trace_id,
        span_id=span_id,
        tenant_id=tenant_id,
        stage=stage,
        start_time_ns=start_ns,
        end_time_ns=end_ns,
        is_terminal=is_terminal,
        expected_span_count=expected_span_count,
        candidates=candidates or [],
        eval_tags=eval_tags or {},
    )


def test_out_of_order_span_correlation_and_candidate_merging() -> None:
    tp = TopicPartition("rag-diagnostic-traces", 0)
    wheel = BucketedTimeWheel(tick_ms=50, expiry_ms=1500, start_ms=0)
    sm = DagStateMachine(timewheel=wheel, max_open_traces=100)

    # Arrival out of order: cross_encoder arrives before dense_retrieval
    span_rerank = _make_span(
        "trace-1",
        "s-2",
        stage="cross_encoder",
        start_ns=1_700_000_000_020_000_000,
        end_ns=1_700_000_000_080_000_000,
        is_terminal=False,
        expected_span_count=2,
        candidates=[
            CandidateTrace(
                doc_id=1,
                chunk_id="c1",
                dense_rank=-1,
                sparse_rank=-1,
                rrf_rank=4,
                rerank_rank=1,
                score=0.92,
                logit=2.4,
            )
        ],
        eval_tags={"golden": "true"},
    )
    res1 = sm.ingest_span(tp, 10, span_rerank)
    assert res1.ready_dags == ()
    assert sm.open_trace_count == 1

    span_dense = _make_span(
        "trace-1",
        "s-1",
        stage="dense_retrieval",
        start_ns=1_700_000_000_000_000_000,
        end_ns=1_700_000_000_015_000_000,
        is_terminal=False,
        expected_span_count=2,
        candidates=[
            CandidateTrace(
                doc_id=1,
                chunk_id="c1",
                dense_rank=10,
                sparse_rank=-1,
                rrf_rank=-1,
                rerank_rank=-1,
                score=0.0,
                logit=0.0,
            )
        ],
    )
    res2 = sm.ingest_span(tp, 11, span_dense)
    assert len(res2.ready_dags) == 1
    pending = res2.ready_dags[0]
    assert pending.is_partial is False
    assert pending.total_duration_ms == pytest.approx(80.0)
    assert pending.contributing_offsets == ((tp, 10), (tp, 11))
    assert pending.eval_tags == {"golden": "true"}

    graph = build_provenance_graph(
        pending,
        {
            (1, "c1"): ChunkProvenance(
                doc_version=2,
                parent_container_hash="pch-1",
                page_index=3,
                bbox=(0.1, 0.2, 0.3, 0.4),
            )
        },
    )
    assert len(graph.nodes) == 1
    node = graph.nodes[0]
    assert node.dense_rank == 10
    assert node.rrf_rank == 4
    assert node.rerank_rank == 1
    assert node.dense_to_rrf_delta == 6
    assert node.rrf_to_rerank_delta == 3
    assert node.dense_to_rerank_delta == 9
    assert node.score == pytest.approx(0.92)


def test_timewheel_partial_eviction_and_late_span_durability() -> None:
    tp = TopicPartition("rag-diagnostic-traces", 1)
    wheel = BucketedTimeWheel(tick_ms=50, expiry_ms=150, start_ms=0)
    sm = DagStateMachine(timewheel=wheel, max_open_traces=100)

    sm.ingest_span(tp, 5, _make_span("trace-expire", "s-1"))
    assert sm.advance_time(100) == []

    expired = sm.advance_time(150)
    assert len(expired) == 1
    assert expired[0].trace_id == "trace-expire"
    assert expired[0].is_partial is True
    assert sm.dag_evictions_partial_count == 1

    # Late span for already-evicted trace must yield immediate durable offset
    late_res = sm.ingest_span(tp, 6, _make_span("trace-expire", "s-2", is_terminal=True))
    assert late_res.ready_dags == ()
    assert late_res.immediate_durable_offsets == ((tp, 6),)


def test_max_open_traces_overflow_force_evicts_oldest_as_partial() -> None:
    tp = TopicPartition("rag-diagnostic-traces", 2)
    wheel = BucketedTimeWheel(tick_ms=50, expiry_ms=1500, start_ms=0)
    sm = DagStateMachine(timewheel=wheel, max_open_traces=2)

    sm.ingest_span(tp, 0, _make_span("t-oldest", "s-1"))
    sm.ingest_span(tp, 1, _make_span("t-second", "s-1"))
    res = sm.ingest_span(tp, 2, _make_span("t-third", "s-1"))

    assert len(res.ready_dags) == 1
    assert res.ready_dags[0].trace_id == "t-oldest"
    assert res.ready_dags[0].is_partial is True
    assert sm.open_trace_count == 2


def test_partition_revocation_drops_open_traces_without_emitting() -> None:
    tp0 = TopicPartition("rag-diagnostic-traces", 0)
    tp1 = TopicPartition("rag-diagnostic-traces", 1)
    wheel = BucketedTimeWheel(tick_ms=50, expiry_ms=1500, start_ms=0)
    sm = DagStateMachine(timewheel=wheel, max_open_traces=100)

    sm.ingest_span(tp0, 100, _make_span("t-revoked", "s-1"))
    sm.ingest_span(tp1, 200, _make_span("t-kept", "s-1"))

    dropped = sm.on_partitions_revoked([tp0])
    assert dropped == 1
    assert sm.open_trace_count == 1
    assert "t-revoked" not in wheel
    assert "t-kept" in wheel


@pytest.mark.asyncio
async def test_trace_pipeline_end_to_end_with_offset_durability() -> None:
    class _FakeRedis:
        async def mget(self, keys: list[str]) -> list[bytes | None]:
            _ = keys
            return [
                msgpack.packb(
                    {
                        "doc_version": 4,
                        "parent_container_hash": "h4",
                        "page_index": 1,
                        "bbox": [0.0, 0.0, 1.0, 1.0],
                    }
                )
            ]

    class _RecordingSink:
        def __init__(self, offset_mgr: OffsetManager) -> None:
            self._offset_mgr = offset_mgr
            self.graphs: list[ProvenanceGraph] = []

        async def emit(
            self,
            graph: ProvenanceGraph,
            contributing_offsets: tuple[tuple[TopicPartition, int], ...],
        ) -> None:
            self.graphs.append(graph)
            for tp_item, off in contributing_offsets:
                self._offset_mgr.mark_durable(tp_item, off)

    tp = TopicPartition("rag-diagnostic-traces", 0)
    offset_mgr = OffsetManager()
    offset_mgr.on_partitions_assigned([tp])
    offset_mgr.track_fetched(tp, 0)

    wheel = BucketedTimeWheel(tick_ms=50, expiry_ms=1500, start_ms=0)
    sm = DagStateMachine(timewheel=wheel, max_open_traces=100)
    resolver = ProvenanceResolver(
        settings=WorkerSettings(),
        redis_client=_FakeRedis(),
    )
    sink = _RecordingSink(offset_mgr)
    pipeline = TracePipeline(
        state_machine=sm,
        provenance_resolver=resolver,
        offset_manager=offset_mgr,
        emit_sink=sink,
    )

    span = _make_span(
        "t-pipe",
        "s-term",
        is_terminal=True,
        candidates=[CandidateTrace(doc_id=7, chunk_id="chk-7", dense_rank=3, rrf_rank=1)],
    )
    await pipeline.handle_span(tp, 0, span)
    await pipeline.flush_pending_emissions()

    assert len(sink.graphs) == 1
    assert sink.graphs[0].nodes[0].doc_version == 4
    assert offset_mgr.committable_offsets()[tp].offset == 1
