"""Unit tests for slot-grouped pipelined Redis Cluster provenance client."""

from __future__ import annotations

import json
from typing import Any

import msgpack
import pytest

from internal.config.settings import WorkerSettings
from internal.provenance.lru_cache import DEFAULT_PROVENANCE, ProvenanceLRUCache
from internal.provenance.redis_client import (
    ProvenanceResolver,
    decode_provenance_payload,
    format_provenance_key,
)


class _FakeRedisCluster:
    def __init__(self, store: dict[str, bytes | str]) -> None:
        self._store = store
        self.mget_calls: list[list[str]] = []

    async def mget(self, keys: list[str]) -> list[bytes | str | None]:
        # Enforce single-slot invariant per MGET call (all keys share {doc_id} hash tag)
        hash_tags = {k.split(":", 1)[0] for k in keys}
        assert len(hash_tags) == 1, f"CROSSSLOT risk in MGET: {keys}"
        self.mget_calls.append(list(keys))
        return [self._store.get(k) for k in keys]

    async def ping(self) -> bool:
        return True


def test_format_provenance_key_uses_doc_id_hash_tag() -> None:
    assert format_provenance_key(42, "chk-abc") == "{42}:chk-abc"


def test_decode_msgpack_with_pixel_bbox_and_json_fallback() -> None:
    msgpack_bytes = msgpack.packb(
        {
            "doc_version": 3,
            "parent_container_hash": "container-x",
            "page_index": 7,
            "bbox": [100, 200, 500, 800],
            "page_width": 1000,
            "page_height": 1000,
        },
        use_bin_type=True,
    )
    prov = decode_provenance_payload(msgpack_bytes)
    assert prov is not None
    assert prov.doc_version == 3
    assert prov.parent_container_hash == "container-x"
    assert prov.page_index == 7
    assert prov.bbox == (0.1, 0.2, 0.5, 0.8)

    json_bytes = json.dumps(
        {
            "doc_version": 5,
            "parent_container_hash": "container-y",
            "page_index": 2,
            "bbox": [0.25, 0.25, 0.75, 0.75],
        }
    ).encode("utf-8")
    prov_json = decode_provenance_payload(json_bytes)
    assert prov_json is not None
    assert prov_json.doc_version == 5
    assert prov_json.bbox == (0.25, 0.25, 0.75, 0.75)


@pytest.mark.asyncio
async def test_resolver_l1_l2_slot_grouped_and_missing_fallback() -> None:
    store: dict[str, bytes | str] = {
        "{10}:c1": msgpack.packb(
            {
                "doc_version": 1,
                "parent_container_hash": "p1",
                "page_index": 0,
                "bbox": [0.1, 0.1, 0.2, 0.2],
            }
        ),
        "{20}:c2": msgpack.packb(
            {
                "doc_version": 2,
                "parent_container_hash": "p2",
                "page_index": 4,
                "bbox": [0.3, 0.3, 0.6, 0.6],
            }
        ),
    }
    fake_redis: Any = _FakeRedisCluster(store)
    lru = ProvenanceLRUCache(capacity=100)
    settings = WorkerSettings(redis_pipeline_batch_size=256)
    resolver = ProvenanceResolver(settings=settings, lru_cache=lru, redis_client=fake_redis)

    result = await resolver.resolve_batch([(10, "c1"), (20, "c2"), (30, "missing")])
    assert result.hits == 2
    assert result.misses == 1
    assert result.resolved[(10, "c1")].doc_version == 1
    assert result.resolved[(20, "c2")].doc_version == 2
    assert result.resolved[(30, "missing")] == DEFAULT_PROVENANCE
    assert len(fake_redis.mget_calls) == 3

    # Second lookup for (10, "c1") and (20, "c2") hits L1 LRU with zero Redis calls
    calls_before = len(fake_redis.mget_calls)
    cached_result = await resolver.resolve_batch([(10, "c1"), (20, "c2")])
    assert cached_result.hits == 2
    assert cached_result.misses == 0
    assert len(fake_redis.mget_calls) == calls_before
