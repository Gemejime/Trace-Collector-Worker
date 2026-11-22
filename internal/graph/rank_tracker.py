"""Deterministic retrieval rank transition delta calculation."""

from __future__ import annotations

from dataclasses import dataclass

RANK_ABSENT: int = -1


@dataclass(slots=True, frozen=True)
class RankDeltas:
    """Rank transition deltas across Dense -> RRF -> Cross-Encoder stages."""

    dense_to_rrf_delta: int
    rrf_to_rerank_delta: int
    dense_to_rerank_delta: int


def compute_rank_delta(earlier_rank: int, later_rank: int) -> int:
    """Compute ``earlier_rank - later_rank``; positive indicates rank improvement.

    Returns ``0`` when either stage is absent (``-1`` sentinel).
    """
    if earlier_rank < 0 or later_rank < 0:
        return 0
    return earlier_rank - later_rank


def compute_rank_deltas(dense_rank: int, rrf_rank: int, rerank_rank: int) -> RankDeltas:
    """Compute all three stage-transition rank deltas for an ``EnrichedTraceNode``."""
    return RankDeltas(
        dense_to_rrf_delta=compute_rank_delta(dense_rank, rrf_rank),
        rrf_to_rerank_delta=compute_rank_delta(rrf_rank, rerank_rank),
        dense_to_rerank_delta=compute_rank_delta(dense_rank, rerank_rank),
    )
