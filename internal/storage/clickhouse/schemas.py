"""Version-controlled ClickHouse DDL, tenant row policies, and ProvenanceGraph serialization."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

RAG_TRACE_DAGS_COLUMNS: tuple[str, ...] = (
    "tenant_id",
    "event_date",
    "trace_id",
    "is_partial",
    "total_duration_ms",
    "nodes.chunk_id",
    "nodes.doc_id",
    "nodes.doc_version",
    "nodes.parent_container_hash",
    "nodes.page_index",
    "nodes.bbox_x1",
    "nodes.bbox_y1",
    "nodes.bbox_x2",
    "nodes.bbox_y2",
    "nodes.dense_rank",
    "nodes.sparse_rank",
    "nodes.rrf_rank",
    "nodes.rerank_rank",
    "nodes.dense_to_rrf_delta",
    "nodes.rrf_to_rerank_delta",
    "nodes.dense_to_rerank_delta",
    "nodes.score",
    "nodes.logit",
    "eval_tags",
    "created_at",
)


def _validate_identifier(value: str, name: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid ClickHouse identifier for {name}: {value!r}")
    return value


def _escape_sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def build_insert_sql(database: str) -> str:
    """Build the native TCP batch INSERT statement for ``rag_trace_dags``."""
    db = _validate_identifier(database, "database")
    cols = ", ".join(RAG_TRACE_DAGS_COLUMNS)
    return f"INSERT INTO {db}.rag_trace_dags ({cols}) VALUES"


def build_schema_statements(
    database: str,
    replicated: bool = True,
    cluster_name: str = "rag_cluster",
) -> tuple[str, ...]:
    """Return ordered ClickHouse DDL statements for databases, tables, and views."""
    db = _validate_identifier(database, "database")
    cluster_clause = ""
    if replicated:
        cluster = _validate_identifier(cluster_name, "cluster_name")
        cluster_clause = f" ON CLUSTER {cluster}"

    create_db = f"CREATE DATABASE IF NOT EXISTS {db}{cluster_clause}"

    if replicated:
        dags_engine = (
            f"ENGINE = ReplicatedMergeTree("
            f"'/clickhouse/tables/{{shard}}/{db}/rag_trace_dags', '{{replica}}')"
        )
        eval_engine = (
            f"ENGINE = ReplicatedReplacingMergeTree("
            f"'/clickhouse/tables/{{shard}}/{db}/rag_eval_candidates', '{{replica}}', created_at)"
        )
    else:
        dags_engine = "ENGINE = MergeTree()"
        eval_engine = "ENGINE = ReplacingMergeTree(created_at)"

    create_dags_table = f"""
CREATE TABLE IF NOT EXISTS {db}.rag_trace_dags{cluster_clause} (
    tenant_id LowCardinality(String) CODEC(ZSTD(1)),
    event_date Date CODEC(Delta(2), ZSTD(1)),
    trace_id String CODEC(ZSTD(1)),
    is_partial UInt8 CODEC(T64, ZSTD(1)),
    total_duration_ms Float64 CODEC(Gorilla, ZSTD(1)),
    nodes Nested(
        chunk_id String CODEC(ZSTD(1)),
        doc_id UInt32 CODEC(T64, ZSTD(1)),
        doc_version UInt32 CODEC(T64, ZSTD(1)),
        parent_container_hash String CODEC(ZSTD(1)),
        page_index UInt32 CODEC(T64, ZSTD(1)),
        bbox_x1 Float64 CODEC(Gorilla, ZSTD(1)),
        bbox_y1 Float64 CODEC(Gorilla, ZSTD(1)),
        bbox_x2 Float64 CODEC(Gorilla, ZSTD(1)),
        bbox_y2 Float64 CODEC(Gorilla, ZSTD(1)),
        dense_rank Int32 CODEC(T64, ZSTD(1)),
        sparse_rank Int32 CODEC(T64, ZSTD(1)),
        rrf_rank Int32 CODEC(T64, ZSTD(1)),
        rerank_rank Int32 CODEC(T64, ZSTD(1)),
        dense_to_rrf_delta Int32 CODEC(T64, ZSTD(1)),
        rrf_to_rerank_delta Int32 CODEC(T64, ZSTD(1)),
        dense_to_rerank_delta Int32 CODEC(T64, ZSTD(1)),
        score Float64 CODEC(Gorilla, ZSTD(1)),
        logit Float64 CODEC(Gorilla, ZSTD(1))
    ),
    eval_tags Map(String, String) CODEC(ZSTD(1)),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(Delta(8), ZSTD(1))
)
{dags_engine}
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, event_date, trace_id)
SETTINGS index_granularity = 8192
""".strip()

    create_eval_table = f"""
CREATE TABLE IF NOT EXISTS {db}.rag_eval_candidates{cluster_clause} (
    tenant_id LowCardinality(String) CODEC(ZSTD(1)),
    event_date Date CODEC(Delta(2), ZSTD(1)),
    trace_id String CODEC(ZSTD(1)),
    is_partial UInt8 CODEC(T64, ZSTD(1)),
    total_duration_ms Float64 CODEC(Gorilla, ZSTD(1)),
    eval_tags Map(String, String) CODEC(ZSTD(1)),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(Delta(8), ZSTD(1))
)
{eval_engine}
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant_id, event_date, trace_id)
SETTINGS index_granularity = 8192
""".strip()

    create_eval_mv = (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.rag_eval_candidates_mv{cluster_clause}\n"
        f"TO {db}.rag_eval_candidates\n"
        "AS SELECT\n"
        "    tenant_id,\n"
        "    event_date,\n"
        "    trace_id,\n"
        "    is_partial,\n"
        "    total_duration_ms,\n"
        "    eval_tags,\n"
        "    created_at\n"
        f"FROM {db}.rag_trace_dags\n"
        "WHERE notEmpty(eval_tags)"
    )  # noqa: S608

    return (create_db, create_dags_table, create_eval_table, create_eval_mv)


def build_tenant_row_policy_ddl(
    database: str,
    policy_name: str,
    role_or_user: str,
    tenant_id: str,
    replicated: bool = True,
    cluster_name: str = "rag_cluster",
) -> tuple[str, str]:
    """Build ClickHouse Row Policy DDL enforcing tenant isolation on shared tables."""
    db = _validate_identifier(database, "database")
    pol = _validate_identifier(policy_name, "policy_name")
    principal = _validate_identifier(role_or_user, "role_or_user")
    escaped_tenant = _escape_sql_literal(tenant_id)
    cluster_clause = (
        f" ON CLUSTER {_validate_identifier(cluster_name, 'cluster_name')}" if replicated else ""
    )

    dags_policy = (
        f"CREATE ROW POLICY IF NOT EXISTS {pol}_dags{cluster_clause} "
        f"ON {db}.rag_trace_dags FOR SELECT USING tenant_id = '{escaped_tenant}' TO {principal}"
    )
    eval_policy = (
        f"CREATE ROW POLICY IF NOT EXISTS {pol}_eval{cluster_clause} "
        f"ON {db}.rag_eval_candidates FOR SELECT "
        f"USING tenant_id = '{escaped_tenant}' TO {principal}"
    )
    return (dags_policy, eval_policy)


def parse_event_date(event_date: str) -> date:
    """Parse ``YYYY-MM-DD`` event_date string into ``datetime.date``."""
    if not event_date:
        return datetime.now(tz=UTC).date()
    return date.fromisoformat(event_date)


def serialize_provenance_graph_row(
    graph: ProvenanceGraph,
    created_at: datetime | None = None,
) -> tuple[Any, ...]:
    """Serialize a ``ProvenanceGraph`` Protobuf into a ClickHouse native TCP row tuple."""
    ts = created_at if created_at is not None else datetime.now(tz=UTC)
    parsed_date = parse_event_date(graph.event_date)

    chunk_ids: list[str] = []
    doc_ids: list[int] = []
    doc_versions: list[int] = []
    parent_hashes: list[str] = []
    page_indices: list[int] = []
    bbox_x1: list[float] = []
    bbox_y1: list[float] = []
    bbox_x2: list[float] = []
    bbox_y2: list[float] = []
    dense_ranks: list[int] = []
    sparse_ranks: list[int] = []
    rrf_ranks: list[int] = []
    rerank_ranks: list[int] = []
    dense_to_rrf_deltas: list[int] = []
    rrf_to_rerank_deltas: list[int] = []
    dense_to_rerank_deltas: list[int] = []
    scores: list[float] = []
    logits: list[float] = []

    for node in graph.nodes:
        chunk_ids.append(node.chunk_id)
        doc_ids.append(node.doc_id)
        doc_versions.append(node.doc_version)
        parent_hashes.append(node.parent_container_hash)
        page_indices.append(node.page_index)
        bbox_x1.append(node.bbox.x1)
        bbox_y1.append(node.bbox.y1)
        bbox_x2.append(node.bbox.x2)
        bbox_y2.append(node.bbox.y2)
        dense_ranks.append(node.dense_rank)
        sparse_ranks.append(node.sparse_rank)
        rrf_ranks.append(node.rrf_rank)
        rerank_ranks.append(node.rerank_rank)
        dense_to_rrf_deltas.append(node.dense_to_rrf_delta)
        rrf_to_rerank_deltas.append(node.rrf_to_rerank_delta)
        dense_to_rerank_deltas.append(node.dense_to_rerank_delta)
        scores.append(node.score)
        logits.append(node.logit)

    return (
        graph.tenant_id,
        parsed_date,
        graph.trace_id,
        1 if graph.is_partial else 0,
        float(graph.total_duration_ms),
        chunk_ids,
        doc_ids,
        doc_versions,
        parent_hashes,
        page_indices,
        bbox_x1,
        bbox_y1,
        bbox_x2,
        bbox_y2,
        dense_ranks,
        sparse_ranks,
        rrf_ranks,
        rerank_ranks,
        dense_to_rrf_deltas,
        rrf_to_rerank_deltas,
        dense_to_rerank_deltas,
        scores,
        logits,
        dict(graph.eval_tags),
        ts,
    )


def serialize_provenance_graph_batch(
    graphs: Sequence[ProvenanceGraph],
    created_at: datetime | None = None,
) -> list[tuple[Any, ...]]:
    """Serialize a batch of ``ProvenanceGraph`` records for ClickHouse native insert."""
    ts = created_at if created_at is not None else datetime.now(tz=UTC)
    return [serialize_provenance_graph_row(graph, created_at=ts) for graph in graphs]
