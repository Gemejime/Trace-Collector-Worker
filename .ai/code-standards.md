# Code Standards - RAG Trace Collector Worker (`rag-trace-collector-worker`)

## General Principles

- **Decoupled Asynchronous Boundary**: Keep the worker strictly focused on Kafka span ingestion, time-wheel DAG assembly, L1/L2 provenance hydration, and ClickHouse native TCP micro-batching. Never invoke Temporal gRPC APIs, online search endpoints, or on-the-fly evaluation triplet generation.
- **Fail-Fast on Corrupt Payloads, Resilient Backpressure on Downstream Outages**: Reject or quarantine malformed Protobuf records cleanly without crashing the worker process. On ClickHouse outages, pause Kafka offset advancement and spool micro-batches to the local disk WAL (`disk_wal.py`).
- **Root-Cause Focus**: Never mask race conditions or event-loop stalls with arbitrary `time.sleep` calls; use bounded `asyncio` timeouts, `uvloop` event loops, process isolation, and deterministic time-wheel ticks.

---

## Python 3.12+ & Code Quality Standards

- **Runtime Target**: Python 3.12+ with `uvloop` (gated via `sys_platform != "win32"` in `pyproject.toml` with `asyncio` fallback for Windows local tests) and `jemalloc` (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`, `MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:2000`; `jemalloc_stats.py` degrades cleanly when `libjemalloc.so` is absent).
- **Linter, Formatter & Typechecker**:
  - Formatter & Linter: `ruff format --check .` and `ruff check .`
  - Strict Type Checking: `mypy --strict cmd internal` (all functions, methods, and dataclasses/models must have explicit type annotations; Protobuf codegen uses `mypy-protobuf` `.pyi` stubs via `Makefile`)
- **Clean Architecture & Dependency Wiring**:
  - Keep domain logic (`internal/graph/`, `internal/provenance/coordinate_mapper.py`) pure Python with zero network I/O dependencies.
  - Wire infrastructure clients (`kafka_consumer.py`, `redis_client.py`, `native_client.py`, `disk_wal.py`) explicitly in `cmd/worker/process_runner.py`.
  - Use `dataclasses(slots=True)` or `msgspec`/`pydantic` models with `__slots__` on hot-path span/DAG structures to minimize per-object heap overhead.

---

## Multi-Process, Memory & Micro-Batching Invariants

1. **Multi-Process Worker Pool (`cmd/worker/`)**:
   - Supervisor (`main.py`) spawns a fixed pool of `3–4` worker subprocesses matching container CPU limits (`2,000m / 4,000m`).
   - Each subprocess runs an isolated `uvloop` event loop bound to a subset of the 32 `rag-diagnostic-traces` partitions.
   - If a worker subprocess exits abnormally, the supervisor immediately respawns a replacement without disturbing sibling processes.
2. **Dual-Tier Provenance Resolution (`internal/provenance/`)**:
   - L1: Process-local LRU cache capped at `50,000` hot chunk coordinate entries (`lru_cache.py`).
   - L2: Pipelined Redis 7.2 Cluster lookups using `{doc_id}:chunk_id` hash tags (`redis_client.py`).
   - Coordinate Normalization: `coordinate_mapper.py` MUST clamp and normalize all bounding box inputs `[x1, y1, x2, y2]` into unit floats `[0.0, 1.0]`.
3. **Bucketed Time-Wheel DAG Assembly (`internal/graph/`)**:
   - Replace per-trace `asyncio.TimerHandle` objects with a fixed-capacity bucketed time-wheel ring buffer (`timewheel.py`).
   - On bucket expiration, `state_machine.py` MUST flag incomplete graphs with `is_partial = True` and emit surviving spans to the batch buffer without raising `AttributeError` or `NoneType` errors.
   - `rank_tracker.py` computes deterministic rank transition deltas ($\text{Dense/Sparse} \rightarrow \text{RRF} \rightarrow \text{Cross-Encoder}$).
4. **Two-Tier ClickHouse Micro-Batching & Atomic Offsets (`internal/storage/`, `internal/consumer/`)**:
   - `batch_buffer.py` flushes via `native_client.py` (`asynch` / `clickhouse-driver` native TCP protocol with `insert_deduplicate=1`, never HTTP or single-row inserts) upon hitting `50,000` records OR `2.0` seconds, whichever occurs first.
   - `offset_manager.py` commits Kafka offsets **if and only after** ClickHouse confirms micro-batch writes to disk. During ClickHouse outages, `disk_wal.py` spools micro-batches and `kafka_consumer.py` pauses partition consumption while continuing `poll()` heartbeats within `max.poll.interval.ms` to prevent rebalance storms.

---

## Secured Logging Invariants

- **Structured JSON Logging**: All logs MUST be emitted as machine-parseable JSON lines via `internal/telemetry/logger.py`. Never use `print()` in `cmd/` or `internal/`.
- **Mandatory Correlation IDs**: Every log record must include `trace_id`, `span_id`, `tenant_id`, `worker_pid`, and `partition` (plus `duration_ms` where applicable).
- **Strict Redaction**:
  - Never log Kafka SASL/SCRAM secrets, Redis Cluster passwords, ClickHouse credentials, or connection DSNs.
  - Never log raw chunk text, prompt-context content, or unredacted tenant PII/PHI; log only `chunk_id`, `doc_id`, `doc_version`, and coordinate bounds.
  - Sanitize error messages in `/ready` and external responses (no internal filesystem paths or stack traces).

---

## Doc API & OpenAPI Mandatory Maintenance

- **Continuous Synchronization**: Whenever any endpoint, request/response field, Protobuf message, Kafka topic/partition contract, Redis key format, ClickHouse schema, or Prometheus metric is added or modified—no matter how small—[`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` **MUST** be updated immediately in the same change step.

---

## Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
