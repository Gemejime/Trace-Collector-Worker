# Phase 3 Implementation Plan: High-Throughput ClickHouse Native Ingestion

## Objective

Implement **Phase 3: High-Throughput ClickHouse Native Ingestion** for `rag-trace-collector-worker` per [`.ai/progress-tracker.md`](../.ai/progress-tracker.md), [`.ai/architecture.md`](../.ai/architecture.md), [`.ai/doc-api.md`](../.ai/doc-api.md), [`.ai/code-standards.md`](../.ai/code-standards.md), and [`.ai/security.md`](../.ai/security.md).

---

## Scope & Deliverables

### 1. Dependencies & Configuration (`pyproject.toml`, `internal/config/settings.py`)
- **`pyproject.toml`**:
  - Add ClickHouse native TCP driver (`clickhouse-driver>=0.2.9`, executed off the `uvloop` thread via `asyncio.to_thread(...)`, or `asynch>=0.2.5`), `prometheus-client>=0.21.0`, and an async HTTP server dependency if not using stdlib `asyncio.start_server` (e.g., `aiohttp>=3.11.0`, verified clean under `pip-audit`).
  - Add `[tool.mypy.overrides]` entries for third-party packages without `py.typed` (e.g., `clickhouse_driver.*`).
- **`internal/config/settings.py`**:
  - Add bounded Phase 3 settings:
    - `clickhouse_connect_timeout_seconds: float = 5.0`
    - `clickhouse_send_receive_timeout_seconds: float = 10.0`
    - `clickhouse_insert_deduplicate: bool = True`
    - `clickhouse_async_insert: bool = False` (enforces synchronous native TCP per `.ai/doc-api.md` and Runbook 3; never enable `async_insert=1` without `async_insert_deduplicate=1` and `wait_for_async_insert=1`)
    - `clickhouse_replicated: bool = True`
    - `clickhouse_cluster_name: str = "rag_cluster"`
    - `disk_wal_max_bytes: int = 536_870_912` (`512 MiB` pod-wide cap; per-worker cap = `disk_wal_max_bytes // worker_process_count`)
    - `disk_wal_drain_interval_seconds: float = 2.0`
    - `batch_max_pending_batches: int = 4` (backpressure threshold before pausing Kafka consumption)

### 2. ClickHouse Schemas, Native TCP Client & Two-Tier Micro-Batcher (`internal/storage/clickhouse/`)
- **`internal/storage/clickhouse/schemas.py`**:
  - Version-controlled DDL for `rag_trace_dags` (`ReplicatedMergeTree`, `ON CLUSTER {cluster}` when `clickhouse_replicated=True`, `PARTITION BY toYYYYMM(event_date)`, `ORDER BY (tenant_id, event_date, trace_id)`), with explicit ClickHouse column compression codecs (e.g., `ZSTD(1)` / `Delta` / `T64` / `Gorilla` per column type) and `SETTINGS index_granularity = 8192`.
  - `nodes Nested(...)` columns mapped 1:1 to `EnrichedTraceNode` (`chunk_id`, `doc_id`, `doc_version`, `parent_container_hash`, `page_index`, `bbox_x1..y2`, `dense_rank`, `sparse_rank`, `rrf_rank`, `rerank_rank`, `dense_to_rrf_delta`, `rrf_to_rerank_delta`, `dense_to_rerank_delta`, `score`, `logit`), plus `eval_tags Map(String, String)` and `created_at DateTime64(3, 'UTC')`.
  - `rag_eval_candidates` (`ReplicatedReplacingMergeTree(created_at)`, `PARTITION BY toYYYYMM(event_date)`, `ORDER BY (tenant_id, event_date, trace_id)`) and `rag_eval_candidates_mv` materialized view projecting evaluation-tagged traces (`notEmpty(eval_tags)`) for decoupled `rag-eval-governance-worker` polling (ADR-003, `tasks/My Naming Convention.pdf`).
  - Document/include tenant isolation Row Policy DDL (`CREATE ROW POLICY ... ON rag_trace_dags USING tenant_id = ...`) per `.ai/security.md`.
  - Row/column serialization helper converting `ProvenanceGraph` Protobuf records for native TCP batch insert: parse `event_date` string (`"YYYY-MM-DD"`) into `datetime.date`, and serialize `nodes Nested(...)` as flattened parallel arrays (`nodes.chunk_id`, `nodes.doc_id`, ..., `nodes.logit`) compatible with ClickHouse `flatten_nested=1`.
- **`internal/storage/clickhouse/native_client.py`**:
  - Native TCP protocol client (zero HTTP/single-insert operations) with `insert_deduplicate=1`, `async_insert=0`, non-blocking `uvloop` execution (`asyncio.to_thread(...)` if using `clickhouse-driver`), TLS support (`clickhouse_tls_enabled`, `clickhouse_ssl_cafile`), `SecretStr` password handling, `ping()` readiness probe, and `ensure_schemas()`.
  - Emits structured JSON logs with strict credential/DSN redaction.
- **`internal/storage/clickhouse/batch_buffer.py`**:
  - Implements `DagEmitSink` (`emit(graph, contributing_offsets)`), `flush()`, `drain_wal()`, and `on_partitions_revoked(revoked_tps)`.
  - Captures `(tp, offset, generation)` via `OffsetManager.partition_generation(tp)` when records enter the buffer. Move `TracePipeline._pending_emissions` late-offset durability tracking to flush-confirmation time so late duplicate spans for a `trace_key` are only marked durable **after** the micro-batch containing that trace is ACKed by ClickHouse.
  - Accumulates `ProvenanceGraph` records and flushes via `native_client.py` upon reaching `batch_max_rows` (`50,000`) OR `batch_flush_interval_seconds` (`2.0s`), whichever occurs first.
  - Calls `OffsetManager.mark_durable(tp, offset, generation=gen)` **if and only after** ClickHouse confirms disk persistence; drops/skips revoked partition offsets on `on_partitions_revoked`.
  - On ClickHouse failure, spools the exact micro-batch (preserving row order, block boundaries, and contributing `(TopicPartition, offset, generation)` metadata) to `disk_wal.py` without marking offsets durable.
  - Exposes `is_backpressure_active()` (`pending_batches >= batch_max_pending_batches` or `disk_wal.spillover_active` or `disk_wal.is_full()`).

### 3. Local Disk WAL Spillover (`internal/storage/spillover/disk_wal.py`)
- Worker-isolated atomic segment writer/reader under `{settings.disk_wal_dir}/worker-{worker_index}` so sibling worker subprocesses never collide on segment files or drain each other's partition offsets.
- Bounded by per-worker quota `disk_wal_max_bytes // worker_process_count` (preserving the `512 MiB` pod-wide cap). When full, refuse new WAL writes, keep `spillover_active=True` / `is_full()=True`, trigger Kafka consumer backpressure, and fail `/ready` (`disk_wal: error`) without dropping data or exhausting node disk.
- Preserves exact batch boundaries, row order, and `(TopicPartition, offset, generation)` metadata so WAL replay triggers ClickHouse `insert_deduplicate=1` cleanly.
- Drains oldest segments first upon ClickHouse recovery, marking contributing offsets durable (with generation check) only after ClickHouse ACKs each drained segment.
- Exposes `is_healthy()`, `is_full()`, and `spillover_active` for `/ready`, `trace_worker_spillover_active`, and Kafka consumer backpressure.

### 4. Telemetry, `jemalloc` Scraper & HTTP Probe Server (`internal/telemetry/`)
- **`internal/telemetry/metrics.py`**:
  - Configures `PROMETHEUS_MULTIPROC_DIR` before registering `prometheus_client` multiprocess metrics, with clean single-process `CollectorRegistry` fallback when `prometheus_multiproc_dir` is empty (unit tests).
  - Defines and wires all 10 contract metrics in `.ai/doc-api.md`:
    - `kafka_consumer.py`: `trace_worker_spans_consumed_total` (`partition`, `worker_id`) and periodic `trace_worker_kafka_consumer_lag` (`partition`) computed via `highwater(tp) - position(tp)`.
    - `state_machine.py` / `process_runner.py`: `trace_worker_dags_assembled_total` (`worker_id`) and `trace_worker_dag_evictions_total` (`status`).
    - `lru_cache.py` / `redis_client.py`: `trace_worker_lru_hit_ratio` (`worker_id`) and `trace_worker_provenance_resolution_ratio` (`worker_id`).
    - `batch_buffer.py` / `native_client.py`: `trace_worker_clickhouse_flush_duration_seconds` (`worker_id`) and `trace_worker_clickhouse_errors_total` (`error_type`).
    - `disk_wal.py`: `trace_worker_spillover_active` (`worker_id`).
    - `jemalloc_stats.py`: `trace_worker_jemalloc_resident_bytes` (`worker_id`).
- **`internal/telemetry/jemalloc_stats.py`**:
  - Loads `ctypes.CDLL(None)` (global process symbol table when `libjemalloc.so` is `LD_PRELOAD`ed), advances `mallctl("epoch", ...)` before scraping `stats.allocated`, `stats.resident`, and `stats.mapped`, and degrades cleanly to `0` when absent.
- **`internal/telemetry/health.py`**:
  - Async HTTP server on `telemetry_host:telemetry_port` (`:8080`) serving:
    - `GET /healthz` (supervisor & active worker count)
    - `GET /ready` (aggregated worker IPC readiness for `kafka`, `redis`, `clickhouse`, `disk_wal`, and `draining` state; sanitized error statuses with no filesystem paths, DSNs, or stack traces)
    - `GET /metrics` (`MultiProcessCollector` exposition)
    - `GET /openapi.yaml` (`api/openapi/openapi.yaml`)
    - `GET /docs` (Swagger UI HTML)

### 5. Supervisor & Worker Runtime Wiring (`cmd/worker/main.py`, `cmd/worker/process_runner.py`, `internal/consumer/kafka_consumer.py`)
- Wire `backpressure_fn=batch_buffer.is_backpressure_active` into `KafkaSpanConsumer._update_backpressure()` so `batch_max_pending_batches` and `disk_wal` spillover pause Kafka consumption while `getmany()` continues heartbeating within `max.poll.interval.ms`.
- Wire `ClickHouseNativeClient`, `MicroBatchBuffer`, `DiskWal`, `MetricsRegistry`, and `JemallocStatsScraper` into `process_runner.py`, including periodic `batch_flush_interval_seconds` flush + WAL drain loop, `on_partitions_revoked` cleanup, and graceful shutdown drain.
- Extend `WorkerHeartbeat` with per-dependency readiness checks (`kafka`, `redis`, `clickhouse`, `disk_wal`, `draining`); clear stale worker readiness when a worker exits or times out.
- Wire `WorkerSupervisor` (`main.py`) to run the `:8080` async HTTP server in a background daemon thread with its own event loop (preserving the synchronous supervisor monitor/signal loop), aggregate worker readiness over IPC, and call `mark_process_dead(pid)` via `on_worker_dead`.

### 6. Tests, Docs & Git Commits
- **`test/unit/`**: Unit tests for `schemas.py` (DDL, codecs, `Date` & `Nested` serialization), `batch_buffer.py` (50k/2.0s triggers, generation safety, late offsets, revocation), `disk_wal.py` (per-worker isolation, byte cap, FIFO drain), `metrics.py`, `jemalloc_stats.py`, and `health.py`.
- **`test/integration/`**:
  - Deterministic in-process/mocked protocol integration tests for CI verifying two-tier micro-batching (`50,000` rows or `2.0s`), atomic Kafka offset commits, backpressure pause/resume, and WAL spillover/drain.
  - `@pytest.mark.integration` suite for live Docker/Testcontainers verification of L1/L2 cache latencies (`<0.05ms` L1, `<1.5ms` L2 over 10k chunks), zero HTTP/single-insert operations, and zero `TOO_MANY_PARTS` errors on ClickHouse.
- **Docs & Commits**: Sync `.ai/doc-api.md`, `api/openapi/openapi.yaml`, and `.ai/progress-tracker.md`; create $\ge 2$ conventional commits.

---

## Verification Plan

1. `ruff format --check .` and `ruff check .`
2. `mypy --strict cmd internal`
3. `bandit -r cmd internal -ll -ii` and `pip-audit`
4. OpenAPI 3.1 validation (`openapi-spec-validator`) & `.ai/doc-api.md` sync check
5. `pytest test/unit -v` and `pytest test/integration -v` (with `-m "not integration"` when live ClickHouse/Redis/Kafka containers are absent)
