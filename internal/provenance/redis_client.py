"""Async pipelined Redis 7.2 Cluster provenance client (L2 cache)."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import msgpack
from redis.asyncio.cluster import ClusterNode, RedisCluster

from internal.config.settings import WorkerSettings
from internal.provenance.coordinate_mapper import normalize_bbox
from internal.provenance.lru_cache import (
    DEFAULT_PROVENANCE,
    ChunkProvenance,
    ProvenanceKey,
    ProvenanceLRUCache,
)
from internal.telemetry.logger import get_logger
from internal.telemetry.metrics import MetricsRegistry


def format_provenance_key(doc_id: int, chunk_id: str) -> str:
    """Format Redis Cluster key with ``{doc_id}`` hash tag for slot-local MGETs."""
    return f"{{{doc_id}}}:{chunk_id}"


def _parse_optional_positive_float(raw: object) -> float | None:
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            val = float(raw)
        except OverflowError:
            return None
        return val if val > 0.0 else None
    try:
        val = float(str(raw))
    except (TypeError, ValueError):
        return None
    return val if val > 0.0 else None


def _parse_non_negative_int(raw: object, default: int = 0) -> int:
    if isinstance(raw, bool) or raw is None:
        return default
    if isinstance(raw, int):
        return max(0, raw)
    try:
        return max(0, int(str(raw)))
    except (TypeError, ValueError):
        return default


def decode_provenance_payload(raw: bytes | bytearray | str | None) -> ChunkProvenance | None:
    """Decode MsgPack (with JSON fallback) provenance payload and normalize bbox."""
    if raw is None:
        return None

    decoded: object = None
    if isinstance(raw, (bytes, bytearray)):
        if not raw:
            return None
        try:
            decoded = msgpack.unpackb(raw, raw=False)
        except Exception:
            try:
                decoded = json.loads(raw.decode("utf-8"))
            except Exception:
                return None
    elif isinstance(raw, str):
        if not raw:
            return None
        try:
            decoded = json.loads(raw)
        except Exception:
            return None
    else:
        return None

    if not isinstance(decoded, Mapping):
        return None

    doc_version = _parse_non_negative_int(decoded.get("doc_version"), default=0)
    parent_raw = decoded.get("parent_container_hash", "")
    parent_container_hash = str(parent_raw) if parent_raw is not None else ""
    page_index = _parse_non_negative_int(decoded.get("page_index"), default=0)

    bbox_raw = decoded.get("bbox", (0.0, 0.0, 0.0, 0.0))
    if not isinstance(bbox_raw, Sequence) or isinstance(bbox_raw, (str, bytes, bytearray)):
        bbox_seq: Sequence[object] = (0.0, 0.0, 0.0, 0.0)
    else:
        bbox_seq = bbox_raw

    page_width = _parse_optional_positive_float(decoded.get("page_width"))
    page_height = _parse_optional_positive_float(decoded.get("page_height"))
    bbox = normalize_bbox(bbox_seq, page_width=page_width, page_height=page_height)

    return ChunkProvenance(
        doc_version=doc_version,
        parent_container_hash=parent_container_hash,
        page_index=page_index,
        bbox=bbox,
    )


def _parse_startup_nodes(nodes_csv: str) -> list[ClusterNode]:
    nodes: list[ClusterNode] = []
    for entry in nodes_csv.split(","):
        trimmed = entry.strip()
        if not trimmed:
            continue
        if ":" in trimmed:
            host, port_str = trimmed.rsplit(":", 1)
            port = int(port_str)
        else:
            host, port = trimmed, 6379
        nodes.append(ClusterNode(host=host, port=port))
    if not nodes:
        nodes.append(ClusterNode(host="localhost", port=6379))
    return nodes


@dataclass(slots=True, frozen=True)
class ProvenanceResolutionResult:
    """Result of a dual-tier L1/L2 provenance resolution batch."""

    resolved: dict[ProvenanceKey, ChunkProvenance]
    hits: int
    misses: int


class ProvenanceResolver:
    """Coordinates L1 process-local LRU cache and L2 slot-grouped Redis Cluster MGET."""

    def __init__(
        self,
        settings: WorkerSettings,
        lru_cache: ProvenanceLRUCache | None = None,
        redis_client: Any | None = None,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        self._settings = settings
        self._lru = lru_cache or ProvenanceLRUCache(capacity=settings.redis_lru_capacity)
        self._client = redis_client
        self._owns_client = redis_client is None
        self._metrics = metrics
        self._total_requested = 0
        self._total_resolved = 0
        self._logger = get_logger("redis_client", stage="provenance_resolution")

    @property
    def lru_cache(self) -> ProvenanceLRUCache:
        return self._lru

    def _get_or_create_client(self) -> Any:
        if self._client is None:
            startup_nodes = _parse_startup_nodes(self._settings.redis_cluster_nodes)
            password = (
                self._settings.redis_password.get_secret_value()
                if self._settings.redis_password is not None
                else None
            )
            kwargs: dict[str, Any] = {
                "startup_nodes": startup_nodes,
                "password": password,
                "socket_timeout": self._settings.redis_socket_timeout_seconds,
                "decode_responses": False,
            }
            if self._settings.redis_tls_enabled:
                kwargs["ssl"] = True
                if self._settings.redis_ssl_cafile:
                    kwargs["ssl_ca_certs"] = self._settings.redis_ssl_cafile
            self._client = RedisCluster(**kwargs)
        return self._client

    async def _fetch_doc_group(
        self,
        client: Any,
        doc_id: int,
        chunk_ids: list[str],
    ) -> dict[ProvenanceKey, ChunkProvenance]:
        found: dict[ProvenanceKey, ChunkProvenance] = {}
        batch_size = self._settings.redis_pipeline_batch_size
        for start in range(0, len(chunk_ids), batch_size):
            batch_chunks = chunk_ids[start : start + batch_size]
            redis_keys = [format_provenance_key(doc_id, cid) for cid in batch_chunks]
            try:
                raw_values = await client.mget(redis_keys)
            except Exception as exc:
                self._logger.warning(
                    "redis_mget_failed",
                    doc_id=doc_id,
                    batch_len=len(redis_keys),
                    error=type(exc).__name__,
                )
                continue

            for cid, raw_val in zip(batch_chunks, raw_values, strict=False):
                parsed = decode_provenance_payload(raw_val)
                if parsed is not None:
                    found[(doc_id, cid)] = parsed
        return found

    async def resolve_batch(self, keys: Iterable[ProvenanceKey]) -> ProvenanceResolutionResult:
        """Resolve chunk provenance via L1 LRU -> slot-grouped L2 Redis Cluster MGET."""
        unique_keys = list(dict.fromkeys(keys))
        if not unique_keys:
            return ProvenanceResolutionResult(resolved={}, hits=0, misses=0)

        resolved, missing = self._lru.get_many(unique_keys)
        if missing:
            grouped: dict[int, list[str]] = defaultdict(list)
            for doc_id, chunk_id in missing:
                grouped[doc_id].append(chunk_id)

            client = self._get_or_create_client()
            group_results = await asyncio.gather(
                *(
                    self._fetch_doc_group(client, doc_id, chunk_ids)
                    for doc_id, chunk_ids in grouped.items()
                )
            )
            l2_hits: dict[ProvenanceKey, ChunkProvenance] = {}
            for group_map in group_results:
                l2_hits.update(group_map)

            if l2_hits:
                self._lru.put_many(l2_hits)
                resolved.update(l2_hits)

        hits = len(resolved)
        misses = 0
        for key in unique_keys:
            if key not in resolved:
                resolved[key] = DEFAULT_PROVENANCE
                misses += 1

        self._total_requested += len(unique_keys)
        self._total_resolved += hits
        if self._metrics is not None:
            self._metrics.set_lru_hit_ratio(self._lru.hit_ratio)
            if self._total_requested > 0:
                self._metrics.set_provenance_resolution_ratio(
                    self._total_resolved / self._total_requested
                )

        return ProvenanceResolutionResult(resolved=resolved, hits=hits, misses=misses)

    async def ping(self) -> bool:
        """Check Redis Cluster connectivity for readiness probes."""
        try:
            client = self._get_or_create_client()
            return bool(await client.ping())
        except Exception as exc:
            self._logger.warning("redis_ping_failed", error=type(exc).__name__)
            return False

    async def close(self) -> None:
        """Close the owned Redis Cluster client."""
        if self._client is not None and self._owns_client:
            close_fn = getattr(self._client, "aclose", None) or getattr(self._client, "close", None)
            if close_fn is not None:
                result = close_fn()
                if asyncio.iscoroutine(result):
                    await result
            self._client = None
