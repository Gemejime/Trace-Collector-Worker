# Phase 2 Implementation Plan: Provenance Resolution & Time-Wheel DAG

## Objective

Implement **Phase 2: Provenance Resolution & Time-Wheel DAG** for `rag-trace-collector-worker` per [`.ai/progress-tracker.md`](../.ai/progress-tracker.md), [`.ai/architecture.md`](../.ai/architecture.md), [`.ai/doc-api.md`](../.ai/doc-api.md), [`.ai/code-standards.md`](../.ai/code-standards.md), and [`.ai/security.md`](../.ai/security.md).

---

## Scope & Deliverables

### 1. Dependencies & Configuration (`pyproject.toml`, `internal/config/settings.py`)
- **`pyproject.toml`**:
  - Add runtime dependencies for L2 provenance resolution: `redis>=5.2.0,<6.0` (async `RedisCluster` support) and `msgpack>=1.1.0,<2.0`.
  - Add `types-redis` / `types-msgpack` (or mypy overrides if unavailable) for `mypy --strict`.
- **`internal/config/settings.py`**:
  - Add bounded Phase 2 settings (with validation):
    - `timewheel_tick_ms: int = 50` (bucket granularity for `timewheel_expiry_seconds=1.5`; note combined `timewheel_expiry_seconds` + Phase 3 `batch_flush_interval_seconds` must be tuned to respect the `P95 <= 2.5s` end-to-end SLO)
    - `timewheel_max_open_traces: int = 25_000` (per-process hard memory bound calibrated to pod `RSS <= 2.0 GiB` across 4 workers and `kafka_max_in_flight_offsets`)
    - `redis_socket_timeout_seconds: float = 0.5`
    - `redis_pipeline_batch_size: int = 256`

### 2. Dual-Tier Provenance Resolution (`internal/provenance/`)
- **`internal/provenance/coordinate_mapper.py`** *(pure Python, zero network I/O)*:
  - Normalizes bounding boxes `[x1, y1, x2, y2]` into unit floats `[0.0, 1.0]`.
  - Supports already-normalized `[0.0, 1.0]` inputs (clamped) as well as pixel coordinates when `page_width` / `page_height` (> 0) are provided.
  - Safely handles `NaN` / `inf`, negative values, values `> 1.0`, and inverted coordinates (`x1 > x2` or `y1 > y2`).
- **`internal/provenance/lru_cache.py`**:
  - Process-local, thread-safe (`threading.RLock`) O(1) LRU cache (`collections.OrderedDict`) capped at `50,000` (`settings.redis_lru_capacity`) hot chunk metadata entries keyed by `(doc_id, chunk_id)`.
  - Tracks hits/misses for `trace_worker_lru_hit_ratio` and uses `dataclass(slots=True, frozen=True)` for `ChunkProvenance`.
- **`internal/provenance/redis_client.py`**:
  - Async pipelined Redis 7.2 Cluster client (`redis.asyncio.RedisCluster`) querying `{doc_id}:{chunk_id}` keys.
  - Groups lookups by `{doc_id}` hash tag (slot-grouped `MGET` or `mget_nonatomic()` / pipelined `GET`s) to prevent `CROSSSLOT` errors across multi-document batches.
  - Decodes MsgPack payloads (`doc_version`, `parent_container_hash`, `page_index`, `bbox`, optional `page_width`, `page_height`) with JSON fallback for test compatibility, normalizes coordinates via `coordinate_mapper.py`, and populates the L1 LRU cache.
  - Supports TLS (`redis_tls_enabled`, `redis_ssl_cafile`) and `SecretStr` password auth without ever logging credentials or raw chunk text.
  - Provides `ProvenanceResolver` coordinating L1 LRU -> L2 pipelined Redis fallback (`resolve_batch`), returning resolved metadata and resolution hit/miss counts.
  - **Missing Provenance Fallback**: When a chunk is absent from both L1 LRU and L2 Redis Cluster, emit the `EnrichedTraceNode` with default provenance (`doc_version=0`, `parent_container_hash=""`, `page_index=0`, `bbox=(0.0, 0.0, 0.0, 0.0)`) and record a resolution miss rather than dropping the trace.

### 3. Bucketed Time-Wheel DAG Assembly (`internal/graph/`)
*(Pure Python domain logic with zero network I/O dependencies)*
- **`internal/graph/rank_tracker.py`**:
  - Computes deterministic rank transition deltas across retrieval stages (`dense_rank`, `sparse_rank`, `rrf_rank`, `rerank_rank`) using `-1` sentinel for stage absence:
    - `dense_to_rrf_delta = dense_rank - rrf_rank` (positive = rank improvement, e.g., `10 -> 2` yields `+8`; `0` if either stage is `-1`)
    - `rrf_to_rerank_delta = rrf_rank - rerank_rank` (`0` if either stage is `-1`)
    - `dense_to_rerank_delta = dense_rank - rerank_rank` (`0` if either stage is `-1`)
- **`internal/graph/timewheel.py`**:
  - Fixed-capacity bucketed time-wheel ring buffer (`O(1)` insert/cancel/tick) replacing per-trace `asyncio.TimerHandle` objects (ADR-004).
  - Configured with `timewheel_tick_ms = 50` (with `timewheel_expiry_seconds = 1.5` default) for deterministic expiration without timer bloat.
  - Advances ticks deterministically (`advance_to(now_ms)` / `tick()`), returning expired `trace_id`s whose deadline has elapsed.
- **`internal/graph/state_machine.py`**:
  - `DagStateMachine` correlating out-of-order `TraceSpanRecord`s by `trace_id` using `slots=True` data structures and tracking all contributing `(TopicPartition, offset)` pairs for ADR-001 offset durability.
  - **Cross-Span Candidate Merging**: Merges `CandidateTrace` records keyed by `(doc_id, chunk_id)` across all spans of a trace (preserving non-`-1` stage ranks and latest `score`/`logit`) before computing `rank_tracker.py` deltas.
  - **Early Completion & Partial Eviction**: Supports early completion when `is_terminal == True` (and/or `expected_span_count > 0` is satisfied) and partial eviction (`is_partial = True`) when expired by the time-wheel without raising `AttributeError` or `NoneType` errors.
  - **Bounded Memory Overflow Policy**: When `len(open_traces) >= timewheel_max_open_traces`, force-evicts the oldest open trace as `is_partial = True` and emits it so `OffsetManager` never deadlocks.
  - **Late / Duplicate Span Handling**: If a span arrives for an already-completed or already-evicted `trace_id`, marks its `(TopicPartition, offset)` durable/quarantined immediately (or emits a single-span `is_partial = True` record) so partition offset commits never stall.
  - **Partition Revocation Cleanup (ADR-001)**: Provides `on_partitions_revoked(revoked_tps)` to drop in-memory open traces owned by revoked partitions without emitting them, allowing clean replay by the new partition owner.
  - **Graph Construction & Metrics**: Builds `ProvenanceGraph` records (merging `eval_tags`, computing `total_duration_ms` and UTC `event_date`, enriching `EnrichedTraceNode`s with `ProvenanceResolver.resolve_batch` + `rank_tracker.py`) and exposes `dags_assembled_count`, `dag_evictions_partial_count`, and `dag_evictions_complete_count` for Phase 3 metrics.

### 4. Worker Pipeline Integration (`cmd/worker/process_runner.py`)
- Wire `DagStateMachine` + `BucketedTimeWheel` + `ProvenanceResolver` into `process_runner.py` as `KafkaSpanConsumer`'s `span_handler`, keeping per-span ingestion synchronous/non-blocking on the `poll_once` hot path and performing async `ProvenanceResolver` enrichment at DAG emission time via a `DagEmitSink` callback protocol (marking contributing offsets durable via `OffsetManager.mark_durable` until Phase 3's `batch_buffer.py` is wired in).
- Wire `KafkaSpanConsumer`'s partition revocation hook to `DagStateMachine.on_partitions_revoked`.
- Add periodic `timewheel_tick_ms` (`50ms`) tick loop and graceful `SIGTERM`/`SIGINT` drain (flushing already-completed/expired DAGs within `graceful_drain_timeout_seconds` and leaving still-open DAGs uncommitted for Kafka replay per ADR-001).

### 5. Unit Tests, Docs & Git Commits
- **`test/unit/`**:
  - `test_coordinate_mapper.py`: `[0.0, 1.0]` clamping, pixel-to-unit normalization, `NaN`/`inf`/inverted bounds.
  - `test_rank_tracker.py`: Rank delta calculations (`Dense -> RRF -> Cross-Encoder`) and `-1` sentinel handling.
  - `test_lru_cache.py`: `50,000` capacity bound, LRU eviction order, hit/miss ratio stats, thread safety.
  - `test_redis_client.py`: `{doc_id}:{chunk_id}` key formatting, multi-`doc_id` slot-grouped batching (no `CROSSSLOT`), MsgPack (including `page_width`/`page_height`) & JSON decoding, L1->L2 fallback with mocked Redis Cluster, missing key graceful fallback.
  - `test_timewheel_dag.py`: Out-of-order span correlation, cross-span `CandidateTrace` merging, early completion on `is_terminal` / `expected_span_count`, exact deadline eviction with `is_partial = True`, `timewheel_max_open_traces` oldest-trace force-eviction, late/duplicate span offset durability, `on_partitions_revoked` cleanup, surviving spans emitted cleanly without `NoneType` errors, and ADR-001 offset tracking.
- **Docs & Commits**:
  - Sync `.ai/doc-api.md` (including Redis MsgPack payload field names) and `api/openapi/openapi.yaml` if any contract details change.
  - Update `.ai/progress-tracker.md` upon completion.
  - Commit incrementally using Conventional Commits ($\ge 2$ commits).

---

## Verification Plan

1. `ruff format --check .` and `ruff check .`
2. `mypy --strict cmd internal`
3. `bandit -r cmd internal -ll -ii` and `pip-audit`
4. `pytest test/unit -v`

---

## Confirmed Decisions (Human-in-the-Loop)

1. **Rank Delta Sign Convention**: `dense_to_rrf_delta = dense_rank - rrf_rank` (positive = rank improvement, e.g., rank `10 -> 2` yields `+8`, and `0` when either stage is `-1`).
2. **Missing Provenance Fallback**: When a chunk is absent from both L1 LRU and L2 Redis Cluster, emit the `EnrichedTraceNode` with default provenance (`doc_version=0`, `parent_container_hash=""`, `page_index=0`, `bbox=(0.0, 0.0, 0.0, 0.0)`) and record a resolution miss rather than dropping the trace.
3. **Time-Wheel Tick Granularity**: `timewheel_tick_ms = 50` (with `timewheel_expiry_seconds = 1.5` default) for deterministic expiration without timer bloat.
4. **Overflow & Revocation Semantics**: `timewheel_max_open_traces` (`25_000` per process) force-evicts the oldest trace as `is_partial = True`; partition revocation drops in-memory open traces for revoked partitions without emitting so Kafka replays cleanly per ADR-001.
