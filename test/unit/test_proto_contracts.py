"""Unit tests for Protobuf v3 TraceSpanRecord and ProvenanceGraph contracts."""

from __future__ import annotations

from api.proto.v1.provenance_graph_pb2 import (
    BoundingBox,
    EnrichedTraceNode,
    ProvenanceGraph,
)
from api.proto.v1.trace_record_pb2 import CandidateTrace, TraceSpanRecord


def test_trace_span_record_round_trip() -> None:
    candidate = CandidateTrace(
        doc_id=101,
        chunk_id="chk-abc-1",
        dense_rank=2,
        sparse_rank=-1,
        rrf_rank=1,
        rerank_rank=0,
        score=0.94,
        logit=3.21,
    )
    span = TraceSpanRecord(
        trace_id="trace-001",
        span_id="span-002",
        parent_span_id="span-001",
        tenant_id="tenant-acme",
        stage="cross_encoder",
        start_time_ns=1_000_000,
        end_time_ns=2_500_000,
        is_terminal=True,
        expected_span_count=5,
        candidates=[candidate],
        eval_tags={"eval_candidate": "true"},
    )

    raw = span.SerializeToString()
    decoded = TraceSpanRecord()
    decoded.ParseFromString(raw)

    assert decoded.trace_id == "trace-001"
    assert decoded.span_id == "span-002"
    assert decoded.parent_span_id == "span-001"
    assert decoded.tenant_id == "tenant-acme"
    assert decoded.stage == "cross_encoder"
    assert decoded.is_terminal is True
    assert decoded.expected_span_count == 5
    assert len(decoded.candidates) == 1
    assert decoded.candidates[0].sparse_rank == -1
    assert decoded.candidates[0].doc_id == 101
    assert decoded.eval_tags["eval_candidate"] == "true"


def test_provenance_graph_round_trip() -> None:
    node = EnrichedTraceNode(
        chunk_id="chk-abc-1",
        doc_id=101,
        doc_version=3,
        parent_container_hash="sha256-parent",
        page_index=4,
        bbox=BoundingBox(x1=0.1, y1=0.2, x2=0.8, y2=0.6),
        dense_rank=5,
        sparse_rank=8,
        rrf_rank=3,
        rerank_rank=1,
        dense_to_rrf_delta=2,
        rrf_to_rerank_delta=2,
        dense_to_rerank_delta=4,
        score=0.97,
        logit=4.12,
    )
    graph = ProvenanceGraph(
        trace_id="trace-001",
        tenant_id="tenant-acme",
        event_date="2026-11-22",
        is_partial=False,
        total_duration_ms=42.5,
        nodes=[node],
        eval_tags={"dataset": "golden"},
    )

    raw = graph.SerializeToString()
    decoded = ProvenanceGraph()
    decoded.ParseFromString(raw)

    assert decoded.trace_id == "trace-001"
    assert decoded.event_date == "2026-11-22"
    assert decoded.is_partial is False
    assert decoded.total_duration_ms == 42.5
    assert len(decoded.nodes) == 1
    assert decoded.nodes[0].bbox.x1 == 0.1
    assert decoded.nodes[0].dense_to_rerank_delta == 4
    assert decoded.eval_tags["dataset"] == "golden"
