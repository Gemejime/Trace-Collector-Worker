# Architecture Context - RAG Trace Collector Worker (`rag-trace-collector-worker`)

## System Stack & Specifications

| Layer | Technology | Role & Operational Characteristics |
| :--- | :--- | :--- |
| **Language Runtime** | Python 3.12+ (Multi-Process + `uvloop`) | 3–4 dedicated worker subprocesses per pod matching CPU limits, bypassing GIL bottlenecks |
| **Memory Allocator** | `jemalloc` (`LD_PRELOAD`) | `MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:2000`; `mallctl` metrics |
| **Ingress Broker** | Apache Kafka 3.7+ (`aiokafka`) | Topic `rag-diagnostic-traces` (32 partitions, partition key `hash(trace_id)`), Protobuf v3 payloads |
| **L1 Provenance Cache** | Process-Local In-Memory LRU | `50,000` hot chunk coordinate hashes per worker process (zero network hops, $<0.05\text{ ms}$) |
| **L2 Provenance Cache** | Redis 7.2 Cluster | Pipelined multi-node reads using hash tags `{doc_id}:chunk_id` ($<1.5\text{ ms}$ batch fallback) |
| **DAG Correlation** | Bucketed Time-Wheel Ring Buffer | Low-overhead TTL eviction for out-of-order spans; flags `is_partial = true` on expiry |
| **Analytical Sink** | ClickHouse 24.3+ (`ReplicatedMergeTree`) | Native TCP micro-batches (`50,000` rows or `2.0s`), partitioned `toYYYYMM(event_date)`, `async_insert=1` burst fallback |
| **Outage Resilience** | Local Disk WAL (`disk_wal.py`) | Pauses Kafka offset advancement and spools micro-batches to disk during ClickHouse outages |
| **Observability HTTP** | HTTP Server (`:8080`) + Prometheus | `/healthz`, `/ready`, `/metrics` (`PROMETHEUS_MULTIPROC_DIR`), `/openapi.yaml`, `/docs` |
| **Autoscaling** | Kubernetes + KEDA `ScaledObject` | Min 4 / Max 16 replicas driven by Kafka Consumer Group lag ($\ge 5,000$ messages/process); note 32 partitions cap active consumers to 2/pod at 16 replicas unless topic partitions expand to 64 |

---

## Production-Grade 5-Layer Backend Architecture

Aligned with `tasks/Production Grade Backend Concepts.pdf`:

### Layer 1: API & Routing
- **Entry Points (`cmd/worker/main.py`, `cmd/worker/process_runner.py`)**:
  - `main.py` acts as the supervisor entrypoint: loads 12-factor configuration, spawns and monitors the fixed pool of worker subprocesses, starts the HTTP telemetry/health server (`:8080`) using `prometheus_client.multiprocess.MultiProcessCollector` (`PROMETHEUS_MULTIPROC_DIR=/tmp/prom-multiproc`) and worker IPC health heartbeats, and handles `SIGINT`/`SIGTERM` graceful shutdown.
  - `process_runner.py` initializes each subprocess's isolated `uvloop` event loop, binds its assigned Kafka partitions, warms up Redis Cluster and ClickHouse native TCP pools, reports dependency readiness to the supervisor over IPC, and drains in-flight batches on shutdown.
- **Endpoints & Routing (`internal/telemetry/health.py`, `internal/telemetry/metrics.py`)**:
  - HTTP Observability & Control Surface (`:8080`): `GET /healthz`, `GET /ready`, `GET /metrics`, `GET /openapi.yaml`, `GET /docs`.
  - Stream Ingress Contract: Kafka topic `rag-diagnostic-traces` consuming Protobuf v3 `TraceSpanRecord` envelopes.
- **Middleware & Cross-Cutting Interceptors**:
  - Subprocess crash isolation & automatic respawn, `trace_id`/`span_id`/`tenant_id` correlation binding in structured JSON logs, and Prometheus counter/histogram instrumentation.
- **Input Validation & Sanitization**:
  - Strict Protobuf schema validation on deserialization; `coordinate_mapper.py` clamps and normalizes bounding box coordinates to unit floats $[0.0, 1.0]$; corrupt/unparseable messages are counted, logged safely (without raw payloads), and quarantined/skipped without crashing sibling workers.
- **Rate Limiting & Backpressure**:
  - Consumer poll backpressure tied to ClickHouse batch buffer depth and disk WAL high-water marks; pauses Kafka partition consumption rather than exhausting pod RSS beyond $2.0\text{ GiB}$.
- **Idempotency & Contiguous Offset Durability**:
  - `offset_manager.py` enforces **contiguous durability independently per partition**: it tracks pending offsets per partition and advances Kafka commits **only** through the highest consecutive offset confirmed durable by ClickHouse flush ACKs, stopping strictly before the earliest unresolved or unflushed offset (a later offset's flush ACK never permits committing past an earlier unresolved record, such as an open DAG in the time-wheel).
  - Duplicate rows are identified by deterministic row identity `(tenant_id, event_date, trace_id)`. Neither the `ReplicatedMergeTree` sorting key `(tenant_id, event_date, trace_id)` nor block-level `insert_deduplicate=1` deduplicates rows when a replay regroups rows into different insert batches. Therefore, at-least-once replay safety relies on row-/query-level deduplication (e.g., `ReplacingMergeTree` with a monotonic version/timestamp or deterministic query-time `GROUP BY (tenant_id, event_date, trace_id)` / `LIMIT 1 BY tenant_id, event_date, trace_id` / `FINAL`). Downstream materialized views (`rag_eval_candidates_mv`) must target a deduplicating table or be queried with the same `(tenant_id, event_date, trace_id)` deduplication by `rag-eval-worker` so replayed inserts do not trigger duplicate evaluation runs.

### Layer 2: Data Persistence & State
- **Database Migrations & Schemas (`internal/storage/clickhouse/schemas.py`)**:
  - Version-controlled DDL for ClickHouse `ReplicatedMergeTree` tables partitioned by `toYYYYMM(event_date)` and ordered by `(tenant_id, event_date, trace_id)`, plus Materialized Views for offline `rag-eval-worker` candidate batch queries.
- **Atomic Offset & WAL Persistence (`internal/consumer/offset_manager.py`, `internal/storage/spillover/disk_wal.py`)**:
  - Ties Kafka consumer offset commits strictly to ClickHouse flush ACKs; spools uncommitted micro-batches to local disk WAL during ClickHouse outages and drains safely upon reconnect.
- **Connection Pooling (`internal/provenance/redis_client.py`, `internal/storage/clickhouse/native_client.py`)**:
  - Persistent pipelined Redis 7.2 Cluster connection pools per worker process and native TCP ClickHouse connection pools (zero HTTP/single-insert operations).
- **Multi-Tier Caching (`internal/provenance/lru_cache.py`, `redis_client.py`)**:
  - L1: Thread-safe/process-local LRU cache (`50,000` hot chunk coordinate hashes, $<0.05\text{ ms}$).
  - L2: Pipelined Redis 7.2 Cluster `MGET` / slot-grouped fetches (`{doc_id}:chunk_id`, $<1.5\text{ ms}$).
- **Batching & Memory Limits**:
  - Two-tier micro-batch accumulator (`50,000` records or `2.0s`) prevents `TOO_MANY_PARTS` errors in ClickHouse and bounds in-memory buffer size.
- **Resource Identification**:
  - Content-addressed `chunk_id` hashes, monotonic document versions ($v_1 \dots v_n$), `parent_container_hash`, and high-entropy `trace_id`/`span_id` identifiers.

### Layer 3: Architecture & Scale
- **Asynchronous Task Workers**:
  - Completely offloads trace DAG assembly and analytical persistence from `rag-search-orchestrator`'s critical path via Kafka.
- **Concurrency Control**:
  - Multi-process isolation eliminates Python GIL contention and cross-worker lock contention; `hash(trace_id)` Kafka partitioning guarantees all spans of a trace route to a single worker process—zero distributed locking required for DAG assembly.
- **High Availability & Redundancy**:
  - Minimum 4 replicas across AZs (KEDA scales 4 to 16 replicas across 32 Kafka partitions) on compute-optimized nodes (`c6i.xlarge` / `c7g.xlarge`).
  - Automatic subprocess supervision respawns crashed worker processes without interrupting sibling partition consumers.
- **Creational & Lifecycle Patterns**:
  - Explicit dependency wiring in `process_runner.py` with per-process singleton lifecycle for logger, `jemalloc` stats scraper, LRU cache, Redis client, and ClickHouse native pool.

### Layer 4: Security & Identity
- **Authentication & Authorization (AuthN / AuthZ)**:
  - SASL/SCRAM + TLS for Kafka consumer connections; authenticated TLS connections to Redis 7.2 Cluster and ClickHouse native TCP; strict `tenant_id` column scoping in all ClickHouse schemas.
- **Configuration & Secrets Management**:
  - 12-factor environment configuration (`internal/config/settings.py`) with credentials injected via Kubernetes Secrets / external secret stores (never committed to source control).
- **Data Protection & Secured Logging**:
  - TLS in transit; structured JSON logging with strict redaction of credentials, DSNs, raw chunk text, and PII/PHI.
- **Container Hardening**:
  - Multi-stage `Dockerfile` running as non-root user with architecture-matched `jemalloc` preloading (`/usr/lib/x86_64-linux-gnu/libjemalloc.so` for `amd64` / `c6i.xlarge` and `/usr/lib/aarch64-linux-gnu/libjemalloc.so` for `arm64` / `c7g.xlarge`, e.g., selected via `TARGETARCH` during image build), `readOnlyRootFilesystem: true` (with dedicated writable mounts only for `/var/spool/trace-wal` and `/tmp/prom-multiproc`), and dropped Linux capabilities.
- **Per-Process Memory Budget**:
  - With 3–4 subprocesses per pod, each subprocess targets $\le 450\text{ MiB}$ RSS (50k LRU + time-wheel + 50k batch buffer) so total pod/container working-set memory (including the supervisor process and all worker subprocesses, measured via container cgroup / `container_memory_working_set_bytes`) remains strictly $\le 2.0\text{ GiB}$. `trace_worker_jemalloc_resident_bytes` is retained as a per-worker allocator diagnostic, not as the pod memory ceiling measurement.

### Layer 5: Observability & Quality
- **Structured Logging**: JSON logs with `trace_id`, `span_id`, `tenant_id`, `worker_pid`, `partition`, `stage`, and `duration_ms`.
- **Primary Prometheus Telemetry Metrics**:
  - `trace_worker_spans_consumed_total` (Counter, per partition)
  - `trace_worker_dags_assembled_total` (Counter)
  - `trace_worker_dag_evictions_total` (Counter, e.g., `status="partial"`)
  - `trace_worker_provenance_resolution_ratio` (Gauge)
  - `trace_worker_lru_hit_ratio` (Gauge)
  - `trace_worker_clickhouse_flush_duration_seconds` (Histogram)
  - `trace_worker_jemalloc_resident_bytes` (Gauge)
  - `trace_worker_kafka_consumer_lag` (Gauge, per partition)
  - `trace_worker_spillover_active` (Gauge)
  - `trace_worker_clickhouse_errors_total` (Counter)
- **Production Alert Rules**:
  - *Elevated Consumer Partition Lag (Warning)*: Kafka lag $> 50,000$ records across consumer group for $> 3\text{ min}$.
  - *Cache Resolution Degradation (Warning / SLO Breach)*: Provenance resolution ratio $< 99.9\%$ (SLO breach warning) or $< 95\%$ (critical degradation) over 5-minute window.
  - *ClickHouse Native Ingestion Error (Page)*: `rate(trace_worker_clickhouse_errors_total[2m]) > 0` for $> 1\text{ min}$.
  - *Native Memory Fragmentation Breach (Critical)*: `sum(trace_worker_jemalloc_resident_bytes)` $> 85\%$ of container memory limit (`2.5 GiB`) for $> 2\text{ min}$.
- **Health Checks & Graceful Error Handling**:
  - `/healthz` (liveness) and `/ready` (readiness probing Kafka, Redis, ClickHouse, and disk WAL); graceful connection draining on `SIGTERM`.

---

## Adjusted Folder Structure

```text
rag-trace-collector-worker/
├── cmd/
│   └── worker/
│       ├── main.py                     # Supervisor entrypoint: spawns & monitors worker processes
│       └── process_runner.py           # Per-process uvloop, partition binding & shutdown signals
├── api/
│   ├── proto/v1/
│   │   ├── trace_record.proto          # Incoming diagnostic span schemas (partition key: trace_id)
│   │   └── provenance_graph.proto      # ClickHouse structured DAG & coordinate definitions
│   └── openapi/
│       └── openapi.yaml                # OpenAPI 3.1 spec for HTTP health, metrics & docs endpoints
├── internal/
│   ├── config/
│   │   └── settings.py                 # Worker process counts, buffer thresholds, connection pools
│   ├── consumer/
│   │   ├── kafka_consumer.py           # Process-isolated aiokafka consumer
│   │   └── offset_manager.py           # Batched offset committer linked to ClickHouse flush ACKs
│   ├── graph/
│   │   ├── timewheel.py                # Bucketed time-wheel DAG aggregator for low-overhead TTLs
│   │   ├── state_machine.py            # Span correlation engine; flags partial/incomplete DAGs
│   │   └── rank_tracker.py             # Computes delta shifts: Dense/Sparse -> RRF -> Cross-Encoder
│   ├── provenance/
│   │   ├── lru_cache.py                # Thread-safe/process-local LRU for 50k hot chunk metadata
│   │   ├── redis_client.py             # Pipelined Redis Cluster client handling cluster hash slots
│   │   └── coordinate_mapper.py        # Normalizes bounding boxes to unit floats [0.0, 1.0]
│   ├── storage/
│   │   ├── clickhouse/
│   │   │   ├── native_client.py        # Native TCP protocol connection pool
│   │   │   ├── batch_buffer.py         # Two-tier micro-batcher (50,000 records or 2.0s)
│   │   │   └── schemas.py              # ClickHouse table DDL, partitions, and codec mappings
│   │   └── spillover/
│   │       └── disk_wal.py             # Local disk-backed spooler for downstream ClickHouse outages
│   └── telemetry/
│       ├── metrics.py                  # Prometheus metrics (spans/sec, lag, buffer depth, etc.)
│       ├── jemalloc_stats.py           # Scrapes mallctl counters (allocated, resident, mapped)
│       ├── health.py                   # HTTP health check server for K8s probes (/healthz, /ready)
│       └── logger.py                   # Structured JSON logger with strict credential/PII redaction
├── deployments/
│   ├── docker/
│   │   └── Dockerfile                  # Multi-stage image with jemalloc preloaded
│   └── helm/
│       ├── values.yaml                 # Multi-core pod resource quotas, KEDA lag scaler
│       └── templates/
│           ├── deployment.yaml
│           └── scaledobject.yaml       # KEDA ScaledObject driven by Kafka lag per partition
├── test/
│   ├── unit/                           # Time-wheel eviction, coordinate mapping, rank deltas
│   ├── integration/                    # Multi-process Kafka consumption, Redis pipelines, ClickHouse
│   ├── chaos/                          # ClickHouse drops, broker restarts, missing root spans
│   ├── performance/                    # 12k spans/sec benchmark, jemalloc heap soak testing
│   └── fixtures/                       # Normalized test spans, mock chunk coordinates
├── pyproject.toml
├── Makefile
└── README.md
```
