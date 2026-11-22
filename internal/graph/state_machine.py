"""Out-of-order span correlation DAG state machine with bounded memory and ADR-001 offsets."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import BoundingBox, EnrichedTraceNode, ProvenanceGraph
from api.proto.v1.trace_record_pb2 import TraceSpanRecord
from internal.graph.rank_tracker import RANK_ABSENT, compute_rank_deltas
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.lru_cache import DEFAULT_PROVENANCE, ChunkProvenance, ProvenanceKey

TraceKey = tuple[str, str]


@dataclass(slots=True)
class MergedCandidate:
    """Cross-span merged candidate signals keyed by ``(doc_id, chunk_id)``."""

    doc_id: int
    chunk_id: str
    dense_rank: int = RANK_ABSENT
    sparse_rank: int = RANK_ABSENT
    rrf_rank: int = RANK_ABSENT
    rerank_rank: int = RANK_ABSENT
    score: float = 0.0
    logit: float = 0.0

    def merge(
        self,
        dense_rank: int,
        sparse_rank: int,
        rrf_rank: int,
        rerank_rank: int,
        score: float,
        logit: float,
    ) -> None:
        if dense_rank != RANK_ABSENT:
            self.dense_rank = dense_rank
        if sparse_rank != RANK_ABSENT:
            self.sparse_rank = sparse_rank
        if rrf_rank != RANK_ABSENT:
            self.rrf_rank = rrf_rank
        if rerank_rank != RANK_ABSENT:
            self.rerank_rank = rerank_rank
        if score != 0.0:
            self.score = score
        if logit != 0.0:
            self.logit = logit


@dataclass(slots=True)
class _OpenTrace:
    """In-flight trace state accumulated in the bucketed time-wheel."""

    trace_id: str
    tenant_id: str
    min_start_ns: int | None = None
    max_end_ns: int | None = None
    expected_span_count: int = 0
    has_terminal: bool = False
    span_ids: set[str] = field(default_factory=set)
    candidates: dict[ProvenanceKey, MergedCandidate] = field(default_factory=dict)
    eval_tags: dict[str, str] = field(default_factory=dict)
    contributing_offsets: list[tuple[TopicPartition, int]] = field(default_factory=list)
    partitions: set[TopicPartition] = field(default_factory=set)

    def add_span(self, tp: TopicPartition, offset: int, span: TraceSpanRecord) -> None:
        self.contributing_offsets.append((tp, offset))
        self.partitions.add(tp)
        self.span_ids.add(span.span_id)

        if span.start_time_ns > 0 and (
            self.min_start_ns is None or span.start_time_ns < self.min_start_ns
        ):
            self.min_start_ns = span.start_time_ns
        if span.end_time_ns > 0 and (self.max_end_ns is None or span.end_time_ns > self.max_end_ns):
            self.max_end_ns = span.end_time_ns

        if span.expected_span_count > 0:
            self.expected_span_count = max(self.expected_span_count, span.expected_span_count)
        if span.is_terminal:
            self.has_terminal = True

        for tag_k, tag_v in span.eval_tags.items():
            self.eval_tags[tag_k] = tag_v

        for cand in span.candidates:
            key = (cand.doc_id, cand.chunk_id)
            existing = self.candidates.get(key)
            if existing is None:
                self.candidates[key] = MergedCandidate(
                    doc_id=cand.doc_id,
                    chunk_id=cand.chunk_id,
                    dense_rank=cand.dense_rank,
                    sparse_rank=cand.sparse_rank,
                    rrf_rank=cand.rrf_rank,
                    rerank_rank=cand.rerank_rank,
                    score=cand.score,
                    logit=cand.logit,
                )
            else:
                existing.merge(
                    dense_rank=cand.dense_rank,
                    sparse_rank=cand.sparse_rank,
                    rrf_rank=cand.rrf_rank,
                    rerank_rank=cand.rerank_rank,
                    score=cand.score,
                    logit=cand.logit,
                )

    def is_complete(self) -> bool:
        if self.expected_span_count > 0:
            return len(self.span_ids) >= self.expected_span_count
        return self.has_terminal


@dataclass(slots=True, frozen=True)
class PendingDag:
    """Assembled domain DAG ready for provenance enrichment and ClickHouse persistence."""

    trace_id: str
    tenant_id: str
    event_date: str
    is_partial: bool
    total_duration_ms: float
    candidates: tuple[MergedCandidate, ...]
    eval_tags: dict[str, str]
    contributing_offsets: tuple[tuple[TopicPartition, int], ...]

    @property
    def provenance_keys(self) -> list[ProvenanceKey]:
        return [(c.doc_id, c.chunk_id) for c in self.candidates]


@dataclass(slots=True, frozen=True)
class SpanIngestResult:
    """Synchronous output of ingesting a single ``TraceSpanRecord``."""

    ready_dags: tuple[PendingDag, ...] = ()
    immediate_durable_offsets: tuple[tuple[TopicPartition, int], ...] = ()


def _compute_duration_and_date(
    min_start_ns: int | None,
    max_end_ns: int | None,
) -> tuple[float, str]:
    if min_start_ns is not None and max_end_ns is not None and max_end_ns >= min_start_ns:
        duration_ms = (max_end_ns - min_start_ns) / 1_000_000.0
    else:
        duration_ms = 0.0

    anchor_ns = min_start_ns if min_start_ns is not None else max_end_ns
    if anchor_ns is not None and anchor_ns > 0:
        dt = datetime.fromtimestamp(anchor_ns / 1_000_000_000.0, tz=UTC)
    else:
        dt = datetime.now(tz=UTC)
    return duration_ms, dt.strftime("%Y-%m-%d")


def build_provenance_graph(
    pending: PendingDag,
    provenance_map: Mapping[ProvenanceKey, ChunkProvenance],
) -> ProvenanceGraph:
    """Build a ``ProvenanceGraph`` Protobuf from a ``PendingDag`` and resolved provenance."""
    nodes: list[EnrichedTraceNode] = []
    for cand in pending.candidates:
        prov = provenance_map.get((cand.doc_id, cand.chunk_id), DEFAULT_PROVENANCE)
        deltas = compute_rank_deltas(cand.dense_rank, cand.rrf_rank, cand.rerank_rank)
        x1, y1, x2, y2 = prov.bbox
        nodes.append(
            EnrichedTraceNode(
                chunk_id=cand.chunk_id,
                doc_id=cand.doc_id,
                doc_version=prov.doc_version,
                parent_container_hash=prov.parent_container_hash,
                page_index=prov.page_index,
                bbox=BoundingBox(x1=x1, y1=y1, x2=x2, y2=y2),
                dense_rank=cand.dense_rank,
                sparse_rank=cand.sparse_rank,
                rrf_rank=cand.rrf_rank,
                rerank_rank=cand.rerank_rank,
                dense_to_rrf_delta=deltas.dense_to_rrf_delta,
                rrf_to_rerank_delta=deltas.rrf_to_rerank_delta,
                dense_to_rerank_delta=deltas.dense_to_rerank_delta,
                score=cand.score,
                logit=cand.logit,
            )
        )

    return ProvenanceGraph(
        trace_id=pending.trace_id,
        tenant_id=pending.tenant_id,
        event_date=pending.event_date,
        is_partial=pending.is_partial,
        total_duration_ms=pending.total_duration_ms,
        nodes=nodes,
        eval_tags=pending.eval_tags,
    )


class DagStateMachine:
    """Correlates out-of-order spans into bounded-memory DAGs using a bucketed time-wheel."""

    def __init__(
        self,
        timewheel: BucketedTimeWheel,
        max_open_traces: int = 25_000,
        completed_cache_size: int = 50_000,
    ) -> None:
        if max_open_traces < 1:
            raise ValueError("max_open_traces must be >= 1")
        if completed_cache_size < 1:
            raise ValueError("completed_cache_size must be >= 1")

        self._timewheel = timewheel
        self._max_open_traces = max_open_traces
        self._completed_cache_size = completed_cache_size
        self._open_traces: OrderedDict[TraceKey, _OpenTrace] = OrderedDict()
        self._completed_traces: OrderedDict[TraceKey, None] = OrderedDict()

        self.dags_assembled_count = 0
        self.dag_evictions_partial_count = 0
        self.dag_evictions_complete_count = 0

    @property
    def open_trace_count(self) -> int:
        return len(self._open_traces)

    def _record_completed(self, trace_key: TraceKey) -> None:
        if trace_key in self._completed_traces:
            self._completed_traces.move_to_end(trace_key)
            return
        self._completed_traces[trace_key] = None
        if len(self._completed_traces) > self._completed_cache_size:
            self._completed_traces.popitem(last=False)

    def clear_completed(self, tenant_id: str, trace_id: str) -> None:
        """Clear a trace from the completed cache so replayed spans can rebuild it."""
        self._completed_traces.pop((tenant_id, trace_id), None)

    def _finalize_trace(self, trace: _OpenTrace, is_partial: bool) -> PendingDag:
        trace_key: TraceKey = (trace.tenant_id, trace.trace_id)
        self._timewheel.cancel(trace_key)
        self._record_completed(trace_key)
        duration_ms, event_date = _compute_duration_and_date(trace.min_start_ns, trace.max_end_ns)
        self.dags_assembled_count += 1
        if is_partial:
            self.dag_evictions_partial_count += 1
        else:
            self.dag_evictions_complete_count += 1

        return PendingDag(
            trace_id=trace.trace_id,
            tenant_id=trace.tenant_id,
            event_date=event_date,
            is_partial=is_partial,
            total_duration_ms=duration_ms,
            candidates=tuple(trace.candidates.values()),
            eval_tags=dict(trace.eval_tags),
            contributing_offsets=tuple(trace.contributing_offsets),
        )

    def ingest_span(
        self,
        tp: TopicPartition,
        offset: int,
        span: TraceSpanRecord,
    ) -> SpanIngestResult:
        """Ingest one validated ``TraceSpanRecord`` synchronously without blocking."""
        trace_key: TraceKey = (span.tenant_id, span.trace_id)
        if trace_key in self._completed_traces:
            return SpanIngestResult(immediate_durable_offsets=((tp, offset),))

        ready: list[PendingDag] = []
        trace = self._open_traces.get(trace_key)
        if trace is None:
            while len(self._open_traces) >= self._max_open_traces:
                _, oldest = self._open_traces.popitem(last=False)
                ready.append(self._finalize_trace(oldest, is_partial=True))

            trace = _OpenTrace(trace_id=span.trace_id, tenant_id=span.tenant_id)
            self._open_traces[trace_key] = trace
            self._timewheel.schedule(trace_key)

        trace.add_span(tp, offset, span)
        if trace.is_complete():
            self._open_traces.pop(trace_key, None)
            ready.append(self._finalize_trace(trace, is_partial=False))

        return SpanIngestResult(ready_dags=tuple(ready))

    def advance_time(self, now_ms: int) -> list[PendingDag]:
        """Advance the time-wheel to ``now_ms`` and emit expired partial DAGs."""
        expired_keys = self._timewheel.advance_to(now_ms)
        emitted: list[PendingDag] = []
        for trace_key in expired_keys:
            trace = self._open_traces.pop(trace_key, None)
            if trace is not None:
                emitted.append(self._finalize_trace(trace, is_partial=True))
        return emitted

    def tick(self) -> list[PendingDag]:
        """Advance the time-wheel by one tick and emit expired partial DAGs."""
        expired_keys = self._timewheel.tick()
        emitted: list[PendingDag] = []
        for trace_key in expired_keys:
            trace = self._open_traces.pop(trace_key, None)
            if trace is not None:
                emitted.append(self._finalize_trace(trace, is_partial=True))
        return emitted

    def on_partitions_revoked(self, revoked_tps: Iterable[TopicPartition]) -> int:
        """Drop in-memory open traces owned by revoked partitions for clean replay (ADR-001)."""
        revoked_set = set(revoked_tps)
        if not revoked_set or not self._open_traces:
            return 0

        to_drop = [
            trace_key
            for trace_key, trace in self._open_traces.items()
            if trace.partitions & revoked_set
        ]
        for trace_key in to_drop:
            self._open_traces.pop(trace_key, None)
            self._timewheel.cancel(trace_key)
        return len(to_drop)
