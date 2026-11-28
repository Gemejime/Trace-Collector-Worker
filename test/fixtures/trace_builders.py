"""Normalized TraceSpanRecord, multi-stage DAG, and MsgPack provenance fixtures."""

from __future__ import annotations

from collections.abc import Sequence

import msgpack

from api.proto.v1.trace_record_pb2 import CandidateTrace, TraceSpanRecord
from internal.provenance.lru_cache import ChunkProvenance, ProvenanceKey

DEFAULT_STAGES: tuple[str, ...] = (
    "intent_classify",
    "dense_retrieval",
    "sparse_retrieval",
    "rrf_blend",
    "cross_encoder",
    "parent_hydrate",
)


def make_candidate(
    doc_id: int = 1,
    chunk_id: str = "chunk-1",
    dense_rank: int = 1,
    sparse_rank: int = -1,
    rrf_rank: int = 1,
    rerank_rank: int = 1,
    score: float = 0.92,
    logit: float = 1.45,
) -> CandidateTrace:
    """Build a normalized ``CandidateTrace`` Protobuf message."""
    return CandidateTrace(
        doc_id=doc_id,
        chunk_id=chunk_id,
        dense_rank=dense_rank,
        sparse_rank=sparse_rank,
        rrf_rank=rrf_rank,
        rerank_rank=rerank_rank,
        score=score,
        logit=logit,
    )


def make_span(
    trace_id: str = "trace-001",
    span_id: str = "span-001",
    parent_span_id: str = "",
    tenant_id: str = "tenant-test",
    stage: str = "intent_classify",
    start_time_ns: int = 1_732_272_000_000_000_000,
    end_time_ns: int = 1_732_272_000_010_000_000,
    is_terminal: bool = False,
    expected_span_count: int = 0,
    candidates: Sequence[CandidateTrace] = (),
    eval_tags: dict[str, str] | None = None,
) -> TraceSpanRecord:
    """Build a normalized ``TraceSpanRecord`` Protobuf message."""
    return TraceSpanRecord(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        tenant_id=tenant_id,
        stage=stage,
        start_time_ns=start_time_ns,
        end_time_ns=end_time_ns,
        is_terminal=is_terminal,
        expected_span_count=expected_span_count,
        candidates=list(candidates),
        eval_tags=eval_tags or {},
    )


def make_span_bytes(
    trace_id: str = "trace-001",
    span_id: str = "span-001",
    parent_span_id: str = "",
    tenant_id: str = "tenant-test",
    stage: str = "intent_classify",
    start_time_ns: int = 1_732_272_000_000_000_000,
    end_time_ns: int = 1_732_272_000_010_000_000,
    is_terminal: bool = False,
    expected_span_count: int = 0,
    candidates: Sequence[CandidateTrace] = (),
    eval_tags: dict[str, str] | None = None,
) -> bytes:
    """Serialize a ``TraceSpanRecord`` into Protobuf v3 bytes."""
    return make_span(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        tenant_id=tenant_id,
        stage=stage,
        start_time_ns=start_time_ns,
        end_time_ns=end_time_ns,
        is_terminal=is_terminal,
        expected_span_count=expected_span_count,
        candidates=candidates,
        eval_tags=eval_tags,
    ).SerializeToString()


def build_multistage_dag_spans(
    trace_id: str = "trace-dag-001",
    tenant_id: str = "tenant-test",
    doc_id: int = 101,
    chunk_id: str = "chunk-a",
    base_time_ns: int = 1_732_272_000_000_000_000,
    eval_tags: dict[str, str] | None = None,
) -> list[TraceSpanRecord]:
    """Build a complete 6-stage RAG execution DAG spanning retrieval through hydration."""
    step = 10_000_000  # 10 ms per stage
    return [
        make_span(
            trace_id=trace_id,
            span_id=f"{trace_id}-s0",
            parent_span_id="",
            tenant_id=tenant_id,
            stage="intent_classify",
            start_time_ns=base_time_ns,
            end_time_ns=base_time_ns + step,
            expected_span_count=6,
            eval_tags=eval_tags,
        ),
        make_span(
            trace_id=trace_id,
            span_id=f"{trace_id}-s1",
            parent_span_id=f"{trace_id}-s0",
            tenant_id=tenant_id,
            stage="dense_retrieval",
            start_time_ns=base_time_ns + step,
            end_time_ns=base_time_ns + 2 * step,
            candidates=[
                make_candidate(
                    doc_id=doc_id,
                    chunk_id=chunk_id,
                    dense_rank=5,
                    sparse_rank=-1,
                    rrf_rank=-1,
                    rerank_rank=-1,
                    score=0.81,
                )
            ],
        ),
        make_span(
            trace_id=trace_id,
            span_id=f"{trace_id}-s2",
            parent_span_id=f"{trace_id}-s0",
            tenant_id=tenant_id,
            stage="sparse_retrieval",
            start_time_ns=base_time_ns + step,
            end_time_ns=base_time_ns + 2 * step,
            candidates=[
                make_candidate(
                    doc_id=doc_id,
                    chunk_id=chunk_id,
                    dense_rank=-1,
                    sparse_rank=8,
                    rrf_rank=-1,
                    rerank_rank=-1,
                    score=0.72,
                )
            ],
        ),
        make_span(
            trace_id=trace_id,
            span_id=f"{trace_id}-s3",
            parent_span_id=f"{trace_id}-s1",
            tenant_id=tenant_id,
            stage="rrf_blend",
            start_time_ns=base_time_ns + 2 * step,
            end_time_ns=base_time_ns + 3 * step,
            candidates=[
                make_candidate(
                    doc_id=doc_id,
                    chunk_id=chunk_id,
                    dense_rank=5,
                    sparse_rank=8,
                    rrf_rank=3,
                    rerank_rank=-1,
                    score=0.86,
                )
            ],
        ),
        make_span(
            trace_id=trace_id,
            span_id=f"{trace_id}-s4",
            parent_span_id=f"{trace_id}-s3",
            tenant_id=tenant_id,
            stage="cross_encoder",
            start_time_ns=base_time_ns + 3 * step,
            end_time_ns=base_time_ns + 4 * step,
            candidates=[
                make_candidate(
                    doc_id=doc_id,
                    chunk_id=chunk_id,
                    dense_rank=5,
                    sparse_rank=8,
                    rrf_rank=3,
                    rerank_rank=1,
                    score=0.95,
                    logit=2.3,
                )
            ],
        ),
        make_span(
            trace_id=trace_id,
            span_id=f"{trace_id}-s5",
            parent_span_id=f"{trace_id}-s4",
            tenant_id=tenant_id,
            stage="parent_hydrate",
            start_time_ns=base_time_ns + 4 * step,
            end_time_ns=base_time_ns + 5 * step,
            is_terminal=True,
        ),
    ]


def make_msgpack_provenance(
    doc_version: int = 3,
    parent_container_hash: str = "ph-sha256-001",
    page_index: int = 2,
    bbox: tuple[float, float, float, float] = (100.0, 200.0, 500.0, 800.0),
    page_width: float | None = 1000.0,
    page_height: float | None = 1000.0,
) -> bytes:
    """Serialize a mock Redis MsgPack chunk provenance coordinate payload."""
    payload: dict[str, object] = {
        "doc_version": doc_version,
        "parent_container_hash": parent_container_hash,
        "page_index": page_index,
        "bbox": list(bbox),
    }
    if page_width is not None:
        payload["page_width"] = page_width
    if page_height is not None:
        payload["page_height"] = page_height
    packed: bytes = msgpack.packb(payload, use_bin_type=True)
    return packed


def make_provenance_map(
    entries: Sequence[tuple[int, str, ChunkProvenance]] = (),
) -> dict[ProvenanceKey, ChunkProvenance]:
    """Build a ``(doc_id, chunk_id) -> ChunkProvenance`` lookup map for test seeding."""
    return {(doc_id, chunk_id): prov for doc_id, chunk_id, prov in entries}
