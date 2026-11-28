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


_LATENCY_BUCKET_BOUNDS: tuple[float, ...] = (
    0.001,
    0.002,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    1.5,
    2.0,
    2.5,
    5.0,
    10.0,
    float("inf"),
)


class _LatencyHistogram:
    """Fixed-memory latency histogram accounting for every completed trace."""

    def __init__(self, counts: list[int] | None = None) -> None:
        self._counts: list[int] = (
            list(counts) if counts is not None else [0] * len(_LATENCY_BUCKET_BOUNDS)
        )
        self._total: int = sum(self._counts)
        self._max_observed: float = 0.0

    def observe(self, latency_seconds: float) -> None:
        if latency_seconds > self._max_observed:
            self._max_observed = latency_seconds
        self._total += 1
        for idx, upper in enumerate(_LATENCY_BUCKET_BOUNDS):
            if latency_seconds <= upper:
                self._counts[idx] += 1
                return

    def reset(self) -> None:
        for idx in range(len(self._counts)):
            self._counts[idx] = 0
        self._total = 0
        self._max_observed = 0.0

    def copy(self) -> _LatencyHistogram:
        copied = _LatencyHistogram(self._counts)
        copied._max_observed = self._max_observed
        return copied

    def merge(self, other: _LatencyHistogram) -> _LatencyHistogram:
        merged = _LatencyHistogram(
            [a + b for a, b in zip(self._counts, other._counts, strict=True)]
        )
        merged._max_observed = max(self._max_observed, other._max_observed)
        return merged

    def p95(self) -> float:
        if self._total == 0:
            return 0.0
        target = min(self._total, max(1, int(0.95 * (self._total - 1)) + 1))
        cumulative = 0
        for upper, count in zip(_LATENCY_BUCKET_BOUNDS, self._counts, strict=True):
            cumulative += count
            if cumulative >= target:
                return self._max_observed if upper == float("inf") else upper
        return self._max_observed


class _BenchmarkClickHouseSink:
    def __init__(self, submit_times: dict[str, float]) -> None:
        self.rows_written = 0
        self.last_flush_ts = time.perf_counter()
        self._submit_times = submit_times
        self.latency_histogram = _LatencyHistogram()

    async def insert_graphs(self, graphs: Sequence[ProvenanceGraph]) -> int:
        now = time.perf_counter()
        self.rows_written += len(graphs)
        self.last_flush_ts = now
        for graph in graphs:
            submitted = self._submit_times.pop(graph.trace_id, None)
            if submitted is not None:
                self.latency_histogram.observe(now - submitted)
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
) -> tuple[int, float, _LatencyHistogram, int]:
    sink.latency_histogram.reset()
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
    return spans_processed, elapsed, sink.latency_histogram.copy(), offset


async def _run_worker_pipeline_benchmark(
    duration_seconds: float,
    tmp_dir: Path,
    worker_index: int = 0,
    batch_max_rows: int = 1000,
) -> tuple[int, float, _LatencyHistogram, int]:
    tp, offsets, pipeline, buffer, sink, submit_times, _refs = _setup_worker_pipeline_benchmark(
        tmp_dir=tmp_dir,
        worker_index=worker_index,
        batch_max_rows=batch_max_rows,
    )
    spans_processed, elapsed, histogram, _ = await _run_benchmark_workload(
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
    return spans_processed, elapsed, histogram, rss_bytes


def _multiprocess_worker_entry(
    worker_index: int,
    duration_seconds: float,
    tmp_dir_str: str,
    result_queue: Any,
) -> None:
    import asyncio

    spans, elapsed, histogram, rss_bytes = asyncio.run(
        _run_worker_pipeline_benchmark(
            duration_seconds=duration_seconds,
            tmp_dir=Path(tmp_dir_str),
            worker_index=worker_index,
            batch_max_rows=5000,
        )
    )
    result_queue.put((spans, elapsed, histogram.p95(), rss_bytes))


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
        spans_1, elapsed_1, hist_1, next_offset = await _run_benchmark_workload(
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

        spans_2, elapsed_2, hist_2, _ = await _run_benchmark_workload(
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
    assert hist_1.merge(hist_2).p95() <= MAX_P95_LATENCY_SECONDS
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
