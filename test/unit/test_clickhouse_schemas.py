"""Unit tests for ClickHouse DDL, tenant row policies, and ProvenanceGraph serialization."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from api.proto.v1.provenance_graph_pb2 import BoundingBox, EnrichedTraceNode, ProvenanceGraph
from internal.storage.clickhouse.schemas import (
    RAG_TRACE_DAGS_COLUMNS,
    build_insert_sql,
    build_schema_statements,
    build_tenant_row_policy_ddl,
    serialize_provenance_graph_row,
)


def test_build_schema_statements_replicated_includes_codecs_and_mv() -> None:
    stmts = build_schema_statements(
        database="rag_traces",
        replicated=True,
        cluster_name="rag_cluster",
    )
    assert len(stmts) == 4
    joined = "\n".join(stmts)
    assert "CREATE DATABASE IF NOT EXISTS rag_traces ON CLUSTER rag_cluster" in stmts[0]
    assert "ReplicatedMergeTree" in stmts[1]
    assert "PARTITION BY toYYYYMM(event_date)" in stmts[1]
    assert "ORDER BY (tenant_id, event_date, trace_id)" in stmts[1]
    assert "SETTINGS index_granularity = 8192" in stmts[1]
    assert "CODEC(ZSTD(1))" in stmts[1]
    assert "CODEC(Delta(2), ZSTD(1))" in stmts[1]
    assert "CODEC(T64, ZSTD(1))" in stmts[1]
    assert "CODEC(Gorilla, ZSTD(1))" in stmts[1]
    assert "ReplicatedReplacingMergeTree" in stmts[2]
    assert "rag_eval_candidates_mv" in stmts[3]
    assert "WHERE notEmpty(eval_tags)" in joined


def test_build_schema_statements_non_replicated_uses_local_engines() -> None:
    stmts = build_schema_statements(database="rag_traces", replicated=False)
    assert "ON CLUSTER" not in "\n".join(stmts)
    assert "ENGINE = MergeTree()" in stmts[1]
    assert "ENGINE = ReplacingMergeTree(created_at)" in stmts[2]


def test_build_tenant_row_policy_ddl() -> None:
    dags_pol, eval_pol = build_tenant_row_policy_ddl(
        database="rag_traces",
        policy_name="tenant_acme_policy",
        role_or_user="eval_reader_acme",
        tenant_id="tenant-acme",
        replicated=False,
    )
    assert "CREATE ROW POLICY IF NOT EXISTS tenant_acme_policy_dags" in dags_pol
    assert "ON rag_traces.rag_trace_dags FOR SELECT" in dags_pol
    assert "USING tenant_id = 'tenant-acme' TO eval_reader_acme" in dags_pol
    assert "ON rag_traces.rag_eval_candidates FOR SELECT" in eval_pol


def test_build_schema_rejects_invalid_identifier() -> None:
    with pytest.raises(ValueError, match="Invalid ClickHouse identifier"):
        build_insert_sql("rag_traces; DROP TABLE users")


def test_serialize_provenance_graph_row_flattens_nested_and_parses_date() -> None:
    graph = ProvenanceGraph(
        trace_id="trace-1",
        tenant_id="tenant-a",
        event_date="2026-11-22",
        is_partial=True,
        total_duration_ms=42.5,
        nodes=[
            EnrichedTraceNode(
                chunk_id="c1",
                doc_id=10,
                doc_version=2,
                parent_container_hash="ph-1",
                page_index=3,
                bbox=BoundingBox(x1=0.1, y1=0.2, x2=0.8, y2=0.9),
                dense_rank=1,
                sparse_rank=2,
                rrf_rank=1,
                rerank_rank=1,
                dense_to_rrf_delta=0,
                rrf_to_rerank_delta=0,
                dense_to_rerank_delta=0,
                score=0.95,
                logit=2.1,
            ),
            EnrichedTraceNode(
                chunk_id="c2",
                doc_id=11,
                doc_version=1,
                parent_container_hash="ph-2",
                page_index=0,
                bbox=BoundingBox(x1=0.0, y1=0.0, x2=1.0, y2=1.0),
                dense_rank=3,
                sparse_rank=-1,
                rrf_rank=2,
                rerank_rank=2,
                dense_to_rrf_delta=1,
                rrf_to_rerank_delta=0,
                dense_to_rerank_delta=1,
                score=0.75,
                logit=1.1,
            ),
        ],
        eval_tags={"dataset": "golden"},
    )
    ts = datetime(2026, 11, 22, 10, 0, 0, tzinfo=UTC)
    row = serialize_provenance_graph_row(graph, created_at=ts)

    assert len(row) == len(RAG_TRACE_DAGS_COLUMNS)
    assert row[0] == "tenant-a"
    assert row[1] == date(2026, 11, 22)
    assert row[2] == "trace-1"
    assert row[3] == 1
    assert row[4] == pytest.approx(42.5)
    assert row[5] == ["c1", "c2"]
    assert row[6] == [10, 11]
    assert row[9] == [3, 0]
    assert row[10] == [0.1, 0.0]
    assert row[21] == [0.95, 0.75]
    assert row[23] == {"dataset": "golden"}
    assert row[24] == ts
