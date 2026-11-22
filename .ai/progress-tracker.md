# Progress Tracker & Implementation Roadmap - RAG Trace Collector Worker (`rag-trace-collector-worker`)

## 1. Calibrated Implementation Phases

This roadmap tracks the implementation milestones detailed in `tasks/RAG Trace Collector Worker Implementation Plan.pdf`:

| Phase | Focus Area | Deliverables & Exit Gates | Status |
| :--- | :--- | :--- | :--- |
| **Phase 0: Repository & AI Spec Alignment** | Governance, CI/CD & Contracts | - `.gitignore` excluding `tasks/`, `.spectral.yaml`, `api/openapi/openapi.yaml`, and GitHub Actions CI (`lint-and-typecheck`, `security-and-logging-audit`, `OpenAPI 3.1 & Doc API Contract Sync`, `test-suites`).<br>- Updated `.ai.md`, `.ai/*`, and `GEMINI.md` aligned with `rag-trace-collector-worker` and `Production Grade Backend Concepts.pdf`. | Completed |
| **Phase 1: Ingress & Multi-Process Architecture** | Stream Contracts & Concurrency | - `pyproject.toml`, `Makefile`, `api/proto/v1/{trace_record,provenance_graph}.proto`.<br>- `internal/config/settings.py`, `internal/telemetry/logger.py` (Secured JSON logging).<br>- `cmd/worker/{main.py, process_runner.py}` supervisor + isolated `uvloop` worker pool.<br>- `internal/consumer/{kafka_consumer.py, offset_manager.py}` consuming `rag-diagnostic-traces` partitioned by `hash(trace_id)`. | Pending |
| **Phase 2: Provenance Resolution & Time-Wheel DAG** | Graph Assembly & Multi-Tier Cache | - `internal/graph/{timewheel.py, state_machine.py, rank_tracker.py}` handling out-of-order spans, eviction deadlines, and `is_partial = True` persistence.<br>- `internal/provenance/{lru_cache.py, redis_client.py, coordinate_mapper.py}` (50k hot chunk LRU + pipelined Redis Cluster `{doc_id}:chunk_id` + `[0.0, 1.0]` bounding box normalization).<br>- Unit tests (`test/unit/`) verifying time-wheel expiration, coordinate bounds, and process isolation. | Pending |
| **Phase 3: High-Throughput ClickHouse Native Ingestion** | Storage Micro-Batching & WAL | - `internal/storage/clickhouse/{native_client.py, batch_buffer.py, schemas.py}` (two-tier `50,000` records or `2.0s` native TCP flushes, `ReplicatedMergeTree` DDL & materialized views).<br>- `internal/storage/spillover/disk_wal.py` local disk WAL for downstream ClickHouse outages.<br>- `internal/telemetry/{metrics.py, jemalloc_stats.py, health.py}` (`/healthz`, `/ready`, `/metrics`, `mallctl` scraper).<br>- Integration tests (`test/integration/`) verifying L1/L2 cache latencies, two-tier batching, and offset commit atomicity. | Pending |
| **Phase 4: Chaos Hardening & Soak Validation** | Resilience & Resource Profiling | - `test/performance/` (8-hour soak test under `12,000 spans/sec` verifying RSS $\le 2.0\text{ GiB}$ via `jemalloc` counters).<br>- `test/chaos/` (60s ClickHouse outage + WAL drain, Kafka rebalance storms, Redis latency spikes).<br>- `deployments/docker/Dockerfile` (`jemalloc` preloaded) & `deployments/helm/` (`values.yaml`, `deployment.yaml`, `scaledobject.yaml` KEDA lag scaler, 4–16 replicas, ArgoCD progressive rollout). | Pending |

---

## 2. Architecture Decision Records (ADRs)

- **ADR-001: Partition Key Anchored Strictly to `trace_id`**: Mandates partitioning the `rag-diagnostic-traces` topic by `hash(trace_id)` rather than `tenant_id`, guaranteeing all spans for a query execution tree land on the same consumer worker process to enable deterministic in-memory DAG assembly.
- **ADR-002: Multi-Process Execution Architecture**: Adopts a multi-process supervisor model per pod (`3–4` processes) over single-threaded `asyncio`, enabling horizontal core saturation (`2,000m–4,000m` CPU) and delivering $\ge 12,000\text{ spans/sec}$ without GIL bottlenecks.
- **ADR-003: Decoupled Evaluation Triggering**: Prunes direct Temporal workflow dispatching from the trace collector. The worker writes normalized execution records to ClickHouse, allowing downstream evaluation services (`rag-eval-worker`) to query and batch golden datasets offline without external gRPC dependencies.
- **ADR-004: Bucketed Time-Wheel DAG Aggregation**: Replaces per-trace timer objects with a low-overhead bucketed time-wheel ring buffer, bounding memory overhead during high-volume trace span correlation.
- **ADR-005: Dual-Tier Local LRU and Pipelined Redis Resolution**: Protects Redis Cluster network capacity by fronting metadata lookups with a local in-process LRU cache (`50,000` entries) for frequently retrieved chunks.

---

## 3. Operational Runbooks

### Runbook 1: Kafka Consumer Lag Spike Mitigation
1. Query Prometheus metric `trace_worker_kafka_consumer_lag` grouped by `partition`.
2. Check ClickHouse write duration via `trace_worker_clickhouse_flush_duration_seconds`.
3. If ClickHouse write latency is normal, verify if KEDA has scaled pod replicas up to the maximum threshold (`16`).
4. If scaling is capped, increase `maxReplicas` in Helm values or add partitions to the `rag-diagnostic-traces` Kafka topic if partition count (`32`) is saturated.

### Runbook 2: Diagnosing Elevated DAG Assembly Timeouts
1. Inspect `trace_worker_dag_evictions_total{status="partial"}`.
2. If evictions spike, inspect the upstream `rag-search-orchestrator` trace producer logs to identify whether specific downstream span emitters (e.g., Cross-Encoder or Redis `MGET`) are dropping spans under duress.
3. If search latencies have genuinely increased, adjust `TIMEWHEEL_EXPIRY_SECONDS` via the configuration map to allow wider span collection windows.

### Runbook 3: ClickHouse Backpressure & WAL Spooling
1. Check `trace_worker_spillover_active` and `trace_worker_clickhouse_errors_total`.
2. If workers are spooling to local disk, check ClickHouse system tables (`system.parts`) for active merge pressure or disk saturation.
3. Temporarily enable `async_insert=1` in the worker connection settings to shift micro-batch buffering directly onto ClickHouse server memory.

---

## 4. Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
