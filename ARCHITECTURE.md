# Architecture — `rag-trace-collector-worker`

## 1. System Overview

`rag-trace-collector-worker` decouples diagnostic trace assembly and analytical persistence from the online search path (`rag-search-orchestrator`). It runs as a multi-process Python 3.12+ service (`uvloop` + `jemalloc`) that ingests Protobuf v3 spans from Kafka, hydrates chunk lineage from a two-tier cache (process-local 50k LRU + pipelined Redis 7.2 Cluster), correlates out-of-order spans in a bucketed time-wheel, and streams micro-batched `ProvenanceGraph` rows into ClickHouse via native TCP.

```text
[ rag-search-orchestrator ] ──► (Non-blocking Out-of-Band Producer)
                                      │
                                      ▼ (Partition Key: hash(trace_id))
               [ Kafka Topic: rag-diagnostic-traces (32 Partitions) ]
                                      │
           ┌──────────────────────────┴──────────────────────────┐
           ▼                                                     ▼
[ Worker Pod: Subprocess 0..N-1 ]                     [ Supervisor (main.py) ]
├── Protobuf Deserialization (aiokafka)               ├── IPC Heartbeat Monitor & Respawn
├── L1 In-Memory LRU (50,000 chunks)                  └── HTTP Server (:8080)
│   └── L2 Pipelined Redis 7.2 Cluster                    ├── /healthz, /ready
├── Bucketed Time-Wheel DAG Assembler                     ├── /metrics (multiproc)
├── Native TCP Micro-Batcher (50,000 rows or 2.0s)        └── /openapi.yaml, /docs
└── Local Disk WAL Spillover (disk_wal.py)
           │
           ▼ Native TCP (port 9440, TLS, insert_deduplicate=1)
[ ClickHouse (SharedMergeTree / ReplicatedMergeTree) ]
           │
           ▼ Offline Query / Materialized View
[ rag-eval-worker / Temporal ]
```

---

## 2. Core Architectural Components

### Layer 1: Multi-Process Ingress & Supervision (`cmd/worker/`, `internal/consumer/`)
- **Process Isolation (`cmd/worker/main.py`, `process_runner.py`)**: A supervisor spawns 3–4 isolated worker subprocesses matching CPU limits (`2000m` / `4000m`), each running its own `uvloop` event loop to eliminate GIL contention. Crashed workers are automatically respawned with bounded exponential backoff without disrupting sibling partition consumers.
- **Kafka Consumption (`internal/consumer/kafka_consumer.py`)**: Consumes `TraceSpanRecord` Protobuf v3 payloads from `rag-diagnostic-traces` (partitioned by `hash(trace_id)` so all spans for a trace route to the same worker process without distributed locking). Corrupt or oversized payloads are quarantined and counted without crashing the worker.
- **Contiguous Offset Durability (`internal/consumer/offset_manager.py`)**: Tracks pending offsets per partition and commits to Kafka **only** through the highest contiguous offset confirmed durable by ClickHouse flush ACKs.

### Layer 2: Dual-Tier Provenance Resolution (`internal/provenance/`)
- **L1 Cache (`lru_cache.py`)**: Thread-safe, process-local 50,000-entry LRU cache (`<0.05 ms` lookup).
- **L2 Cache (`redis_client.py`)**: Pipelined `RedisCluster` client using `{doc_id}:chunk_id` hash tags so all chunks of a document map to the same Redis hash slot (`<1.5 ms` batch fallback). Backed by an in-cluster 6-node Bitnami Redis 7.2 Cluster (3 primaries + 3 replicas).
- **Coordinate Normalization (`coordinate_mapper.py`)**: Clamps and normalizes visual bounding boxes to unit floats $[0.0, 1.0]$.

### Layer 3: Bucketed Time-Wheel DAG Assembly (`internal/graph/`)
- **Time-Wheel Ring Buffer (`timewheel.py`)**: Correlates out-of-order spans with $O(1)$ bucket insertion and tick-based TTL expiration (`1.5s` default).
- **State Machine & Rank Tracking (`state_machine.py`, `rank_tracker.py`)**: Merges candidate nodes across retrieval stages, computes rank transition deltas ($\text{Dense/Sparse} \rightarrow \text{RRF} \rightarrow \text{Cross-Encoder}$), and flags expired incomplete traces with `is_partial = True`.

### Layer 4: Micro-Batch Storage & Outage Resilience (`internal/storage/`)
- **Two-Tier Batch Buffer (`clickhouse/batch_buffer.py`)**: Accumulates assembled DAGs and flushes via native TCP (`clickhouse-driver` with `insert_deduplicate=1`, `async_insert=0`) upon reaching `50,000` rows or `2.0s`.
- **ClickHouse Schemas (`clickhouse/schemas.py`)**: `rag_trace_dags` partitioned by `toYYYYMM(event_date)` and ordered by `(tenant_id, event_date, trace_id)` with `ZSTD`/`Delta` codecs and materialized views (`rag_eval_candidates_mv`) for offline evaluation.
- **Local Disk WAL (`spillover/disk_wal.py`)**: During ClickHouse outages, pauses Kafka offset advancement, spools micro-batches to `/var/spool/trace-wal` (bounded by `DISK_WAL_MAX_BYTES`), triggers consumer backpressure, and drains FIFO on recovery.

### Layer 5: Memory Discipline & Observability (`internal/telemetry/`)
- **Allocator Tuning (`jemalloc_stats.py`)**: Preloads `libjemalloc.so.2` (`MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:2000`) and scrapes `mallctl` counters (`allocated`, `resident`, `mapped`).
- **Telemetry & Probes (`health.py`, `metrics.py`, `logger.py`)**: Exposes `/healthz`, `/ready`, `/metrics` (via `PROMETHEUS_MULTIPROC_DIR`), `/openapi.yaml`, and `/docs` on `:8080`, and emits structured JSON logs with strict credential, DSN, chunk-text, and PII/PHI redaction.

---

## 3. Deployment, Configuration & Security Architecture

- **12-Factor Config & Drift Enforcement**:
  - `internal/config/settings.py` (`WorkerSettings`) loads `.env` locally and environment variables in Kubernetes.
  - `.env.prod` (gitignored) syncs non-sensitive configuration into `deployments/helm/values.yaml` via `scripts/sync_env_to_deployments.py --sync` (enforced by `.git/hooks/pre-commit`).
  - `deployments/helm/templates/configmap.yaml` renders `.Values.env` dynamically, and `checksum/config` annotations in `deployment.yaml` and `rollout.yaml` trigger rolling restarts on config changes.
- **Secret Isolation**:
  - `PASSWORD`, `SECRET`, `TOKEN`, and `KEY` variables are stripped from `values.yaml` / `ConfigMap` and injected into Kubernetes Secret `rag-trace-collector-worker-secret` via `.github/workflows/deploy.yml` (see `deployments/SECRET_MANAGEMENT.md`).
- **Autoscaling & Hardening**:
  - KEDA `ScaledObject` scales pods from `4` to `16` based on Kafka consumer lag.
  - Non-root container (`10001:10001`), `readOnlyRootFilesystem: true` (writable mounts restricted to `/var/spool/trace-wal` and `/tmp/prom-multiproc`), and all Linux capabilities dropped.
