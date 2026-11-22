"""Process-local thread-safe O(1) LRU cache for hot chunk provenance metadata (ADR-005)."""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from internal.provenance.coordinate_mapper import UnitBBox

ProvenanceKey = tuple[int, str]


@dataclass(slots=True, frozen=True)
class ChunkProvenance:
    """Normalized lineage and visual coordinate metadata for a document chunk."""

    doc_version: int
    parent_container_hash: str
    page_index: int
    bbox: UnitBBox


DEFAULT_PROVENANCE = ChunkProvenance(
    doc_version=0,
    parent_container_hash="",
    page_index=0,
    bbox=(0.0, 0.0, 0.0, 0.0),
)


class ProvenanceLRUCache:
    """Thread-safe O(1) LRU cache keyed by ``(doc_id, chunk_id)``."""

    def __init__(self, capacity: int = 50_000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._capacity = capacity
        self._lock = threading.RLock()
        self._entries: OrderedDict[ProvenanceKey, ChunkProvenance] = OrderedDict()
        self._hits = 0
        self._misses = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._entries)

    @property
    def hits(self) -> int:
        with self._lock:
            return self._hits

    @property
    def misses(self) -> int:
        with self._lock:
            return self._misses

    @property
    def hit_ratio(self) -> float:
        with self._lock:
            total = self._hits + self._misses
            return (self._hits / total) if total > 0 else 0.0

    def get(self, doc_id: int, chunk_id: str) -> ChunkProvenance | None:
        """Fetch a single entry and promote it as most-recently used."""
        key = (doc_id, chunk_id)
        with self._lock:
            value = self._entries.get(key)
            if value is None:
                self._misses += 1
                return None
            self._entries.move_to_end(key)
            self._hits += 1
            return value

    def put(self, doc_id: int, chunk_id: str, provenance: ChunkProvenance) -> None:
        """Insert or update an entry, evicting the least-recently used if at capacity."""
        key = (doc_id, chunk_id)
        with self._lock:
            if key in self._entries:
                self._entries[key] = provenance
                self._entries.move_to_end(key)
                return
            self._entries[key] = provenance
            if len(self._entries) > self._capacity:
                self._entries.popitem(last=False)

    def get_many(
        self, keys: Iterable[ProvenanceKey]
    ) -> tuple[dict[ProvenanceKey, ChunkProvenance], list[ProvenanceKey]]:
        """Batch lookup returning ``(hits_map, missing_keys)`` under a single lock."""
        hits_map: dict[ProvenanceKey, ChunkProvenance] = {}
        missing: list[ProvenanceKey] = []
        with self._lock:
            for key in keys:
                value = self._entries.get(key)
                if value is None:
                    self._misses += 1
                    missing.append(key)
                else:
                    self._entries.move_to_end(key)
                    self._hits += 1
                    hits_map[key] = value
        return hits_map, missing

    def put_many(self, items: Mapping[ProvenanceKey, ChunkProvenance]) -> None:
        """Insert or update multiple entries under a single lock."""
        with self._lock:
            for key, provenance in items.items():
                if key in self._entries:
                    self._entries[key] = provenance
                    self._entries.move_to_end(key)
                else:
                    self._entries[key] = provenance
                    if len(self._entries) > self._capacity:
                        self._entries.popitem(last=False)

    def clear(self) -> None:
        """Evict all cached entries."""
        with self._lock:
            self._entries.clear()

    def reset_stats(self) -> None:
        """Reset hit/miss counters without clearing cached entries."""
        with self._lock:
            self._hits = 0
            self._misses = 0
