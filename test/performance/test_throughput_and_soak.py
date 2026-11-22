"""Throughput and 8-hour memory soak tests for rag-trace-collector-worker."""

from __future__ import annotations

import multiprocessing
import os
import statistics
import time
import tracemalloc
from cmd.worker.process_runner import TracePipeline
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from aiokafka import TopicPartition

from api.proto.v1.provenance_graph_pb2 import ProvenanceGraph
from internal.config.settings import WorkerSettings
from internal.consumer.offset_manager import OffsetManager
from internal.graph.state_machine import DagStateMachine
from internal.graph.timewheel import BucketedTimeWheel
from internal.provenance.lru_cache import ChunkProvenance, ProvenanceLRUCache
from internal.provenance.redis_client import ProvenanceResolver
from internal.storage.clickhouse.batch_buffer import MicroBatchBuffer
from internal.storage.spillover.disk_wal import DiskWal
from internal.telemetry.jemalloc_stats import JemallocStatsScraper
from test.fixtures.trace_builders import make_candidate, make_span

MAX_POD_RSS_BYTES = 2 * 1024 * 1024 * 1024  # 2.0 GiB ceiling
TARGET_AGGREGATE_SPANS_PER_SEC = 12_000.0
MAX_P95_LATENCY_SECONDS = 2.5


class _BenchmarkClickHouseSink:
    def __init__(self, submit_times: dict[str, float]) -> None:
        self.rows_written = 0
        self.last_flush_ts = time.perf_counter()
        self._submit_times = submit_times
        self.latencies: list[float] = []

    async def insert_graphs(self, graphs: Sequence[ProvenanceGraph]) -> int:
        now = time.perf_counter()
        self.rows_written += len(graphs)
        self.last_flush_ts = now
        for graph in graphs:
            submitted = self._submit_times.pop(graph.trace_id, None)
            if submitted is not None:
                self.latencies.append(now - submitted)
        return len(graphs)


def _current_rss_bytes() -> int:
    """Read current process RSS bytes cross-platform when available."""
    try:
        import resource

        rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        # Linux reports KiB; macOS reports bytes.
        return rss if os.uname().sysname == "Darwin" else rss * 1024
    except Exception:
        return 0


def _p95(samples: list[float]) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    idx = min(len(ordered) - 1, max(0, int(0.95 * (len(ordered) - 1))))
    return ordered[idx]


def _setup_worker_pipeline_benchmark(
    tmp_dir: Path,
    worker_index: int = 0,
    batch_max_rows: int = 1000,
) -> tuple[
    TopicPartition,
    OffsetManager,
    TracePipeline,
    MicroBatchBuffer,
    _BenchmarkClickHouseSink,
    dict[str, float],
    Any,
]:
    tp = TopicPartition("rag-diagnostic-traces", worker_index)
    settings = WorkerSettings(
        batch_max_rows=batch_max_rows,
        batch_flush_interval_seconds=0.5,
        disk_wal_dir=str(tmp_dir),
    )
    offsets = OffsetManager()
    offsets.on_partitions_assigned([tp])
    tw = BucketedTimeWheel(tick_ms=50, expiry_ms=1000, start_ms=0)
    sm = DagStateMachine(timewheel=tw)
    lru = ProvenanceLRUCache(capacity=settings.redis_lru_capacity)
    for idx in range(500):
        lru.put(idx, f"chunk-{idx}", ChunkProvenance(1, "ph", 0, (0.1, 0.1, 0.9, 0.9)))
    resolver = ProvenanceResolver(settings=settings, lru_cache=lru, redis_client=object())
    submit_times: dict[str, float] = {}
    sink = _BenchmarkClickHouseSink(submit_times=submit_times)
    wal = DiskWal(wal_dir=tmp_dir, worker_index=worker_index, max_bytes=16 * 1024 * 1024)
    pipeline = TracePipeline(state_machine=sm, provenance_resolver=resolver, offset_manager=offsets)
    buffer = MicroBatchBuffer(
        settings=settings,
        client=sink,
        offset_manager=offsets,
        disk_wal=wal,
        on_traces_durable=pipeline.mark_traces_durable,
    )
    pipeline.set_emit_sink(buffer)
    return tp, offsets, pipeline, buffer, sink, submit_times, (sm, lru, resolver, wal)


async def _run_benchmark_workload(
    tp: TopicPartition,
    offsets: OffsetManager,
    pipeline: TracePipeline,
    buffer: MicroBatchBuffer,
    sink: _BenchmarkClickHouseSink,
    submit_times: dict[str, float],
    duration_seconds: float,
    worker_index: int,
    offset_start: int = 0,
) -> tuple[int, float, list[float], int]:
    lat_start_idx = len(sink.latencies)
    spans_processed = 0
    offset = offset_start
    start = time.perf_counter()
    deadline = start + duration_seconds

    while time.perf_counter() < deadline:
        for trace_idx in range(200):
            tid = f"perf-{worker_index}-{offset}"
            chunk_idx = trace_idx % 500
            span = make_span(
                trace_id=tid,
                span_id=f"s-{offset}",
                tenant_id="tenant-perf",
                stage="parent_hydrate",
                is_terminal=True,
                candidates=[make_candidate(doc_id=chunk_idx, chunk_id=f"chunk-{chunk_idx}")],
            )
            offsets.track_fetched(tp, offset)
            submit_times[tid] = time.perf_counter()
            await pipeline.handle_span(tp, offset, span)
            offset += 1
            spans_processed += 1
        await pipeline.flush_pending_emissions()
        await buffer.flush_if_due()

    await pipeline.flush_pending_emissions()
    await buffer.flush()
    elapsed = max(1e-6, time.perf_counter() - start)
    return spans_processed, elapsed, list(sink.latencies[lat_start_idx:]), offset


async def _run_worker_pipeline_benchmark(
    duration_seconds: float,
    tmp_dir: Path,
    worker_index: int = 0,
    batch_max_rows: int = 1000,
) -> tuple[int, float, list[float], int]:
    tp, offsets, pipeline, buffer, sink, submit_times, _refs = _setup_worker_pipeline_benchmark(
        tmp_dir=tmp_dir,
        worker_index=worker_index,
        batch_max_rows=batch_max_rows,
    )
    spans_processed, elapsed, latencies, _ = await _run_benchmark_workload(
        tp=tp,
        offsets=offsets,
        pipeline=pipeline,
        buffer=buffer,
        sink=sink,
        submit_times=submit_times,
        duration_seconds=duration_seconds,
        worker_index=worker_index,
    )
    jemalloc_stats = JemallocStatsScraper().scrape()
    rss_bytes = jemalloc_stats.resident_bytes if jemalloc_stats.available else _current_rss_bytes()
    return spans_processed, elapsed, latencies, rss_bytes


def _multiprocess_worker_entry(
    worker_index: int,
    duration_seconds: float,
    tmp_dir_str: str,
    result_queue: Any,
) -> None:
    import asyncio

    spans, elapsed, latencies, rss_bytes = asyncio.run(
        _run_worker_pipeline_benchmark(
            duration_seconds=duration_seconds,
            tmp_dir=Path(tmp_dir_str),
            worker_index=worker_index,
            batch_max_rows=5000,
        )
    )
    result_queue.put((spans, elapsed, _p95(latencies), rss_bytes))


async def test_pipeline_throughput_and_flat_memory_smoke(
    soak_seconds: float,
    tmp_path: Path,
) -> None:
    """Fast CI smoke benchmark verifying pipeline throughput, latency, and flat heap growth."""
    tp, offsets, pipeline, buffer, sink, submit_times, _refs = _setup_worker_pipeline_benchmark(
        tmp_dir=tmp_path,
        worker_index=0,
    )
    tracemalloc.start()
    try:
        half = max(0.5, soak_seconds / 2.0)
        spans_1, elapsed_1, lat_1, next_offset = await _run_benchmark_workload(
            tp=tp,
            offsets=offsets,
            pipeline=pipeline,
            buffer=buffer,
            sink=sink,
            submit_times=submit_times,
            duration_seconds=half,
            worker_index=0,
            offset_start=0,
        )
        snap_mid, _ = tracemalloc.get_traced_memory()

        spans_2, elapsed_2, lat_2, _ = await _run_benchmark_workload(
            tp=tp,
            offsets=offsets,
            pipeline=pipeline,
            buffer=buffer,
            sink=sink,
            submit_times=submit_times,
            duration_seconds=half,
            worker_index=0,
            offset_start=next_offset,
        )
        snap_end, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    total_spans = spans_1 + spans_2
    total_elapsed = elapsed_1 + elapsed_2
    assert total_spans > 0
    assert (total_spans / total_elapsed) > 100.0
    assert _p95(lat_1 + lat_2) <= MAX_P95_LATENCY_SECONDS
    assert peak_bytes < MAX_POD_RSS_BYTES
    # Verify heap did not grow unboundedly between first and second half (< 16 MiB delta).
    assert (snap_end - snap_mid) < 16 * 1024 * 1024


@pytest.mark.soak
def test_full_scale_multiprocess_soak(soak_seconds: float, tmp_path: Path) -> None:
    """Multi-process soak test enforcing >= 12,000 spans/sec, P95 <= 2.5s, and RSS <= 2.0 GiB."""
    worker_count = int(os.environ.get("WORKER_PROCESS_COUNT", "4"))
    ctx = multiprocessing.get_context("spawn")
    result_queue: Any = ctx.Queue()
    procs = [
        ctx.Process(
            target=_multiprocess_worker_entry,
            args=(idx, soak_seconds, str(tmp_path), result_queue),
        )
        for idx in range(worker_count)
    ]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join(timeout=soak_seconds + 60.0)
        assert proc.exitcode == 0

    results = [result_queue.get(timeout=5.0) for _ in range(worker_count)]
    total_spans = sum(r[0] for r in results)
    max_elapsed = max(r[1] for r in results)
    aggregate_throughput = total_spans / max(1e-6, max_elapsed)
    p95_latency = (
        statistics.quantiles([r[2] for r in results], n=20)[-1]
        if len(results) > 1
        else results[0][2]
    )
    total_rss = sum(r[3] for r in results)

    assert aggregate_throughput >= TARGET_AGGREGATE_SPANS_PER_SEC
    assert p95_latency <= MAX_P95_LATENCY_SECONDS
    assert total_rss <= MAX_POD_RSS_BYTES
