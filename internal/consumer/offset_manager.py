"""Per-partition contiguous Kafka offset durability manager (ADR-001)."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from aiokafka import OffsetAndMetadata, TopicPartition


@dataclass(slots=True)
class _PartitionOffsetState:
    """Tracks in-flight and resolved offsets for a single Kafka partition."""

    pending_queue: deque[int] = field(default_factory=deque)
    pending_set: set[int] = field(default_factory=set)
    resolved_set: set[int] = field(default_factory=set)
    highest_contiguous_offset: int | None = None
    last_committed_offset: int | None = None

    def track_fetched(self, offset: int) -> None:
        if offset in self.pending_set:
            return
        if self.highest_contiguous_offset is not None and offset <= self.highest_contiguous_offset:
            return
        self.pending_queue.append(offset)
        self.pending_set.add(offset)

    def mark_resolved(self, offset: int) -> None:
        if offset not in self.pending_set:
            return
        self.resolved_set.add(offset)
        while self.pending_queue and self.pending_queue[0] in self.resolved_set:
            head = self.pending_queue.popleft()
            self.pending_set.remove(head)
            self.resolved_set.remove(head)
            self.highest_contiguous_offset = head

    def next_committable_offset(self) -> int | None:
        if self.highest_contiguous_offset is None:
            return None
        candidate = self.highest_contiguous_offset + 1
        if self.last_committed_offset is not None and candidate <= self.last_committed_offset:
            return None
        return candidate


class OffsetManager:
    """Enforces ADR-001 contiguous per-partition offset durability.

    Offsets are only eligible for Kafka commit once all earlier fetched offsets
    on the same partition have been either confirmed durable by ClickHouse
    (``mark_durable``) or quarantined as corrupt/oversized (``mark_quarantined``).
    """

    def __init__(self) -> None:
        self._partitions: dict[TopicPartition, _PartitionOffsetState] = {}

    def on_partitions_assigned(self, partitions: Iterable[TopicPartition]) -> None:
        """Initialize tracking state for newly assigned partitions."""
        for tp in partitions:
            self._partitions.setdefault(tp, _PartitionOffsetState())

    def on_partitions_revoked(
        self, partitions: Iterable[TopicPartition]
    ) -> dict[TopicPartition, OffsetAndMetadata]:
        """Return final committable offsets for revoked partitions and clear their state."""
        revoked_set = set(partitions)
        final_commits: dict[TopicPartition, OffsetAndMetadata] = {}
        for tp in revoked_set:
            state = self._partitions.pop(tp, None)
            if state is None:
                continue
            next_offset = state.next_committable_offset()
            if next_offset is not None:
                final_commits[tp] = OffsetAndMetadata(next_offset, "")
        return final_commits

    def track_fetched(self, tp: TopicPartition, offset: int) -> None:
        """Record a fetched Kafka offset as in-flight for its partition."""
        state = self._partitions.setdefault(tp, _PartitionOffsetState())
        state.track_fetched(offset)

    def mark_durable(self, tp: TopicPartition, offset: int) -> None:
        """Mark an offset durable after ClickHouse flush ACK."""
        state = self._partitions.get(tp)
        if state is not None:
            state.mark_resolved(offset)

    def mark_quarantined(self, tp: TopicPartition, offset: int) -> None:
        """Mark a corrupt or oversized offset resolved so it does not deadlock commits."""
        state = self._partitions.get(tp)
        if state is not None:
            state.mark_resolved(offset)

    def committable_offsets(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """Compute highest contiguous ``OffsetAndMetadata(offset + 1, '')`` per partition."""
        commits: dict[TopicPartition, OffsetAndMetadata] = {}
        for tp, state in self._partitions.items():
            next_offset = state.next_committable_offset()
            if next_offset is not None:
                commits[tp] = OffsetAndMetadata(next_offset, "")
        return commits

    def mark_committed(self, committed: Mapping[TopicPartition, OffsetAndMetadata]) -> None:
        """Record offsets successfully committed to Kafka."""
        for tp, meta in committed.items():
            state = self._partitions.get(tp)
            if state is not None and (
                state.last_committed_offset is None or meta.offset > state.last_committed_offset
            ):
                state.last_committed_offset = meta.offset

    def total_in_flight(self) -> int:
        """Return total unresolved in-flight offsets across all assigned partitions."""
        return sum(len(state.pending_queue) for state in self._partitions.values())

    def partition_in_flight(self, tp: TopicPartition) -> int:
        """Return unresolved in-flight offset count for a single partition."""
        state = self._partitions.get(tp)
        return len(state.pending_queue) if state is not None else 0
