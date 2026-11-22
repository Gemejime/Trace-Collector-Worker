"""Unit tests for process-local thread-safe 50k LRU provenance cache."""

from __future__ import annotations

import threading

from internal.provenance.lru_cache import ChunkProvenance, ProvenanceLRUCache


def _prov(version: int) -> ChunkProvenance:
    return ChunkProvenance(
        doc_version=version,
        parent_container_hash=f"hash-{version}",
        page_index=version,
        bbox=(0.1, 0.2, 0.3, 0.4),
    )


def test_lru_cache_eviction_order_and_stats() -> None:
    cache = ProvenanceLRUCache(capacity=2)
    cache.put(1, "c1", _prov(1))
    cache.put(1, "c2", _prov(2))

    # Promote c1 so c2 becomes least-recently used
    assert cache.get(1, "c1") == _prov(1)
    assert cache.get(1, "missing") is None

    cache.put(1, "c3", _prov(3))
    assert cache.size == 2
    assert cache.get(1, "c2") is None
    assert cache.get(1, "c1") == _prov(1)
    assert cache.get(1, "c3") == _prov(3)
    assert cache.hits == 3
    assert cache.misses == 2
    assert cache.hit_ratio == 0.6


def test_lru_cache_thread_safety_under_concurrent_access() -> None:
    cache = ProvenanceLRUCache(capacity=50_000)
    errors: list[Exception] = []

    def worker(tid: int) -> None:
        try:
            for i in range(200):
                cache.put(tid, f"chunk-{i}", _prov(i))
                _ = cache.get(tid, f"chunk-{i}")
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert cache.size == 1_600
    assert cache.capacity == 50_000
