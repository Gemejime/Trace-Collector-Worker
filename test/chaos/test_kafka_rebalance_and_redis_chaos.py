"""Chaos tests for Kafka rebalance storms and Redis latency/outage spikes."""

from __future__ import annotations

import asyncio
import time
from cmd.worker.process_runner import TracePipeline

from aiokafka import TopicPartition
from prometheus_client import CollectorRegistry

from internal.config.settings import WorkerSettings
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.lru_cache import DEFAULT_PROVENANCE, ChunkProvenance, ProvenanceLRUCache
from internal.provenance.redis_client import ProvenanceResolver
from internal.telemetry.metrics import MetricsRegistry
from test.fixtures.trace_builders import make_candidate, make_msgpack_provenance, make_span


class _SlowFlakyRedisClient:
    def __init__(self, delay_s: float = 0.02, fail_doc_ids: set[int] | None = None) -> None:
        self._delay_s = delay_s
        self._fail_doc_ids = fail_doc_ids or set()
        self.calls = 0
        self.started_event: asyncio.Event | None = None
        self.release_event: asyncio.Event | None = None

    async def mget(self, keys: list[str]) -> list[bytes | None]:
        self.calls += 1
        if self.started_event is not None:
            self.started_event.set()
        if self.release_event is not None:
            await self.release_event.wait()
        else:
            await asyncio.sleep(self._delay_s)
        for key in keys:
            if key.startswith("{"):
                doc_part = key[1:].split("}", 1)[0]
                if int(doc_part) in self._fail_doc_ids:
                    raise TimeoutError("redis cluster slot timeout")
        return [make_msgpack_provenance(doc_version=7) for _ in keys]

    async def ping(self) -> bool:
        return True


async def test_kafka_rebalance_storm_prevents_split_brain_and_stale_commits() -> None:
    """Rapidly revoke and reassign partitions while spans & enrichment tasks are in flight."""
    tp0 = TopicPartition("rag-diagnostic-traces", 0)
    tp1 = TopicPartition("rag-diagnostic-traces", 1)
    offsets = OffsetManager()
    tw = BucketedTimeWheel(tick_ms=50, expiry_ms=500, start_ms=0)
    sm = DagStateMachine(timewheel=tw)
    settings = WorkerSettings()
    slow_redis = _SlowFlakyRedisClient(delay_s=0.05)
    resolver = ProvenanceResolver(settings=settings, redis_client=slow_redis)

    emitted: list[str] = []

    class _RecordingSink:
        async def emit(self, graph: object, contributing_offsets: object) -> None:
            _ = contributing_offsets
            emitted.append(getattr(graph, "trace_id", ""))

    pipeline = TracePipeline(
        state_machine=sm,
        provenance_resolver=resolver,
        offset_manager=offsets,
        emit_sink=_RecordingSink(),
    )

    for storm_round in range(5):
        slow_redis.started_event = asyncio.Event()
        slow_redis.release_event = asyncio.Event()
        try:
            offsets.on_partitions_assigned([tp0, tp1])
            gen0 = offsets.partition_generation(tp0)
            assert gen0 is not None

            # Open trace on tp0 (incomplete) and complete trace on tp1 (in-flight Redis enrichment).
            open_span = make_span(
                trace_id=f"open-{storm_round}",
                span_id="s0",
                is_terminal=False,
                expected_span_count=2,
            )
            ready_span = make_span(
                trace_id=f"ready-{storm_round}",
                span_id="s1",
                is_terminal=True,
                candidates=[make_candidate(doc_id=10 + storm_round, chunk_id="c1")],
            )
            offsets.track_fetched(tp0, storm_round * 10)
            offsets.track_fetched(tp1, storm_round * 10)
            await pipeline.handle_span(tp0, storm_round * 10, open_span)
            await pipeline.handle_span(tp1, storm_round * 10, ready_span)

            # Wait until Redis enrichment has actually started before revoking.
            await asyncio.wait_for(slow_redis.started_event.wait(), timeout=2.0)
            pipeline.on_partitions_revoked([tp0, tp1])
            final_commits = offsets.on_partitions_revoked([tp0, tp1])
            assert final_commits == {}
            assert sm.open_trace_count == 0

            # Reassign tp0 and verify stale generation cannot mark durable.
            offsets.on_partitions_assigned([tp0])
            offsets.track_fetched(tp0, storm_round * 10)
            offsets.mark_durable(tp0, storm_round * 10, generation=gen0)
            assert offsets.committable_offsets() == {}
            offsets.on_partitions_revoked([tp0])
        finally:
            slow_redis.release_event.set()

    await pipeline.flush_pending_emissions()
    assert emitted == []


async def test_redis_latency_spikes_and_timeouts_isolate_l1_and_track_ratio() -> None:
    """Inject Redis timeouts; verify L1 hits stay fast and resolution ratio degrades cleanly."""
    reg = CollectorRegistry()
    metrics = MetricsRegistry(worker_id="worker-0", registry=reg)
    settings = WorkerSettings(redis_socket_timeout_seconds=0.1, redis_lru_capacity=1000)

    lru = ProvenanceLRUCache(capacity=settings.redis_lru_capacity)
    lru.put(1, "hot-chunk", ChunkProvenance(2, "ph-hot", 1, (0.1, 0.2, 0.3, 0.4)))

    flaky_redis = _SlowFlakyRedisClient(delay_s=0.01, fail_doc_ids={999})
    resolver = ProvenanceResolver(
        settings=settings,
        lru_cache=lru,
        redis_client=flaky_redis,
        metrics=metrics,
    )

    # L1-only batch: zero Redis calls, < 5ms.
    t0 = time.perf_counter()
    res_l1 = await resolver.resolve_batch([(1, "hot-chunk")])
    assert (time.perf_counter() - t0) < 0.05
    assert flaky_redis.calls == 0
    assert res_l1.hits == 1
    assert res_l1.misses == 0

    # Mixed batch: doc_id=999 times out, doc_id=2 succeeds, L1 stays isolated.
    res_mixed = await resolver.resolve_batch([(1, "hot-chunk"), (2, "warm-chunk"), (999, "cold")])
    assert res_mixed.hits == 2
    assert res_mixed.misses == 1
    assert res_mixed.resolved[(999, "cold")] == DEFAULT_PROVENANCE

    # Verify Prometheus resolution ratio updated to 3/4 = 0.75 (1 + 2 hits over 1 + 3 requested).
    ratio_val = None
    for metric in reg.collect():
        if metric.name == "trace_worker_provenance_resolution_ratio":
            ratio_val = float(metric.samples[0].value)
    assert ratio_val == 0.75
