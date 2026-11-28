"""Unit tests for deterministic retrieval rank transition deltas."""

from __future__ import annotations

from internal.graph.rank_tracker import RANK_ABSENT, compute_rank_delta, compute_rank_deltas


def test_compute_rank_deltas_positive_and_negative_shifts() -> None:
    deltas = compute_rank_deltas(dense_rank=10, rrf_rank=2, rerank_rank=5)
    assert deltas.dense_to_rrf_delta == 8
    assert deltas.rrf_to_rerank_delta == -3
    assert deltas.dense_to_rerank_delta == 5


def test_compute_rank_deltas_with_absent_sentinel() -> None:
    assert compute_rank_delta(RANK_ABSENT, 3) == 0
    assert compute_rank_delta(4, RANK_ABSENT) == 0

    deltas = compute_rank_deltas(dense_rank=12, rrf_rank=RANK_ABSENT, rerank_rank=1)
    assert deltas.dense_to_rrf_delta == 0
    assert deltas.rrf_to_rerank_delta == 0
    assert deltas.dense_to_rerank_delta == 11
