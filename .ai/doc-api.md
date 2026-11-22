# Documented APIs & Contracts - RAG Trace Collector Worker (`rag-trace-collector-worker`)

This document is the authoritative specification for all HTTP, Protobuf v3, Kafka, Redis Cluster, ClickHouse, and Prometheus contracts of the `rag-trace-collector-worker` microservice.

> [!IMPORTANT]
> **Continuous Doc API & OpenAPI 3.1 Updates**: This file and `api/openapi/openapi.yaml` MUST be kept synchronized on **every code change or addition**, regardless of how small the modification is.

---

## 1. OpenAPI 3.1 HTTP Telemetry & Probe Specification

### Base URL
`http://localhost:8080` (Internal Kubernetes Pod Telemetry & Probe Server)

### Endpoints Overview

| Method | Endpoint | Description | Auth Scope |
| :--- | :--- | :--- | :--- |
| `GET` | `/healthz` | Kubernetes Liveness Probe (supervisor & worker pool alive) | Public |
| `GET` | `/ready` | Kubernetes Readiness Probe (checks Kafka, Redis Cluster, ClickHouse native TCP, Disk WAL, drain state) | Public |
| `GET` | `/metrics` | Prometheus Metrics Scrape Endpoint | Public / Ops |
| `GET` | `/openapi.yaml` | Raw OpenAPI 3.1 YAML specification | Public |
| `GET` | `/docs` | Swagger UI documentation interface | Public |

---

### Detailed Endpoint Contracts

#### 1. `GET /healthz`
Returns `200 OK` if the supervisor process and worker subprocess pool are alive.
- **Response `200 OK`**:
```json
{
  "status": "alive",
  "active_workers": 4
}
```

#### 2. `GET /ready`
Verifies downstream connectivity and drain state before Kubernetes routes scrape/lifecycle traffic.
- **Response `200 OK`**:
```json
{
  "status": "ready",
  "checks": {
    "kafka": "ok",
    "redis": "ok",
    "clickhouse": "ok",
    "disk_wal": "ok"
  }
}
```
- **Response `503 Service Unavailable`** (when draining on `SIGTERM` or a dependency check fails):
```json
{
  "status": "not_ready",
  "checks": {
    "kafka": "ok",
    "redis": "ok",
    "clickhouse": "error",
    "disk_wal": "ok"
  }
}
```
- **Response `503 Service Unavailable`** (during graceful `SIGTERM` drain):
```json
{
  "status": "draining",
  "checks": {
    "kafka": "draining",
    "redis": "ok",
    "clickhouse": "ok",
    "disk_wal": "ok"
  }
}
```

#### 3. `GET /metrics`
- **Response `200 OK`**: Prometheus text exposition format (`text/plain; version=0.0.4`).

#### 4. `GET /openapi.yaml` & `GET /docs`
- `GET /openapi.yaml` $\rightarrow$ `200 OK` (`application/yaml`) serving `api/openapi/openapi.yaml`.
- `GET /docs` $\rightarrow$ `200 OK` (`text/html`) serving Swagger UI.

---

## 2. Kafka Ingress & Protobuf v3 Contracts (`api/proto/v1/`)

### Kafka Topic Contract
- **Topic**: `rag-diagnostic-traces`
- **Partitions**: `32`
- **Partition Key**: `hash(trace_id)` *(strictly anchored to `trace_id` per ADR-001 so all spans of an execution DAG land on the same worker subprocess)*
- **Payload Format**: Serialized Protobuf v3 `TraceSpanRecord` bytes.

### `api/proto/v1/trace_record.proto`
Incoming diagnostic span schema emitted out-of-band by `rag-search-orchestrator`:
- `string trace_id`
- `string span_id`
- `string parent_span_id`
- `string tenant_id`
- `string stage` *(e.g., `intent_classify`, `dense_retrieval`, `sparse_retrieval`, `rrf_blend`, `acl_filter`, `cross_encoder`, `parent_hydrate`)*
- `int64 start_time_ns`
- `int64 end_time_ns`
- `bool is_terminal` *(marks the final orchestrator span for early DAG completion before time-wheel expiry)*
- `uint32 expected_span_count` *(optional root-span hint; `0` if unknown, falling back to `TIMEWHEEL_EXPIRY_SECONDS`)*
- `repeated CandidateTrace candidates` *(contains `uint32 doc_id`, `string chunk_id`, `int32 dense_rank`, `int32 sparse_rank`, `int32 rrf_rank`, `int32 rerank_rank`, `double score`, `double logit`; sentinel `-1` indicates absence from a ranking stage)*
- `map<string, string> eval_tags` *(evaluation candidate markers for downstream `rag-eval-worker` / `rag-eval-governance-worker`)*

### `api/proto/v1/provenance_graph.proto`
Assembled execution DAG and coordinate-enriched record schema for ClickHouse persistence:
- `string trace_id`, `string tenant_id`, `string event_date`
- `bool is_partial` *(set `true` when evicted by the bucketed time-wheel before `is_terminal` or `expected_span_count` is satisfied)*
- `double total_duration_ms`
- `repeated EnrichedTraceNode nodes` *(includes `chunk_id`, `doc_id`, `doc_version`, `parent_container_hash`, `page_index`, normalized `bbox [x1, y1, x2, y2]` in $[0.0, 1.0]$, and rank transition deltas `dense -> rrf -> cross_encoder`)*

---

## 3. Redis 7.2 Cluster Provenance Cache Contract

- **L1 Cache**: Process-local thread-safe LRU holding `50,000` hot chunk coordinate hashes (`<0.05ms` hit latency).
- **L2 Cache (Redis 7.2 Cluster)**:
  - **Key Format**: `{doc_id}:{chunk_id}` *(Redis Cluster hash tag on `{doc_id}` enables slot-grouped multi-node pipelined `MGET` fetches)*
  - **Value Encoding**: MsgPack-serialized provenance record (with JSON fallback in tests) containing `doc_version` (`uint32`), `parent_container_hash` (`string`), `page_index` (`uint32`), `bbox` (`[x1, y1, x2, y2]`), and optional `page_width` / `page_height` (`float`) normalized by `coordinate_mapper.py` to `[0.0, 1.0]`. Missing keys fall back to default provenance (`doc_version=0`, `parent_container_hash=""`, `page_index=0`, `bbox=(0.0, 0.0, 0.0, 0.0)`) and record a resolution miss.
  - **Target Latency**: $<1.5\text{ ms}$ aggregate per pipelined batch.

---

## 4. ClickHouse 24.3+ Analytical Sink Contract (`internal/storage/clickhouse/schemas.py`)

- **Protocol**: Native TCP Protocol (`native_client.py` via `asynch` / `clickhouse-driver`), zero HTTP/single-insert operations.
- **Two-Tier Micro-Batch Flush**: Flush synchronously via native TCP strictly upon reaching **`50,000` records** OR **`2.0` seconds** (whichever occurs first); do not use `async_insert=1` unless a ClickHouse 24.3+ async deduplication (`async_insert_deduplicate=1`, `wait_for_async_insert=1`) and recovery contract is explicitly established. Enable `insert_deduplicate=1` and require WAL/restart recovery to preserve each block's exact contents, boundaries, and row order; `insert_deduplicate=1` alone does not suppress duplicates when replayed rows are regrouped into different batches, so downstream queries must also deduplicate by `(tenant_id, event_date, trace_id)`.
- **Engine, Partitioning & Tables**:
  - **Primary Table (`rag_trace_dags`)**: `ReplicatedMergeTree` (with `ZSTD(1)` / `Delta` / `T64` / `Gorilla` column codecs and `SETTINGS index_granularity = 8192`)
  - **Partition By**: `toYYYYMM(event_date)`
  - **Order By**: `(tenant_id, event_date, trace_id)`
  - **Core Columns**: `tenant_id LowCardinality(String)`, `event_date Date`, `trace_id String`, `is_partial UInt8`, `total_duration_ms Float64`, `nodes Nested(...)`, `eval_tags Map(String, String)`, `created_at DateTime64(3, 'UTC')`.
  - **Evaluation Candidates Table (`rag_eval_candidates`)**: `ReplicatedReplacingMergeTree(created_at)` partitioned by `toYYYYMM(event_date)`, ordered by `(tenant_id, event_date, trace_id)`.
- **Downstream Handoff**: Materialized view (`rag_eval_candidates_mv`) projects evaluation-tagged traces (`WHERE notEmpty(eval_tags)`) into `rag_eval_candidates` for `rag-eval-worker` / `rag-eval-governance-worker` offline polling.
- **Tenant Isolation**: Shared-table reads are restricted via ClickHouse Row Policies (`CREATE ROW POLICY ... ON rag_trace_dags FOR SELECT USING tenant_id = '<tenant_id>' TO <role>`).

---

## 5. Prometheus Metrics Contract (`internal/telemetry/metrics.py`)

| Metric Name | Type | Labels | Purpose |
| :--- | :--- | :--- | :--- |
| `trace_worker_spans_consumed_total` | Counter | `partition`, `worker_id` | Raw spans consumed per Kafka partition |
| `trace_worker_dags_assembled_total` | Counter | `worker_id` | Completed multi-stage execution DAGs |
| `trace_worker_dag_evictions_total` | Counter | `status` | Time-wheel evictions (e.g., `status="partial"`) |
| `trace_worker_provenance_resolution_ratio` | Gauge | `worker_id` | Ratio of chunk hashes successfully mapped to lineage metadata |
| `trace_worker_lru_hit_ratio` | Gauge | `worker_id` | Local 50k LRU cache hit vs. miss ratio |
| `trace_worker_clickhouse_flush_duration_seconds` | Histogram | `worker_id` | Micro-batch native TCP insert latencies into ClickHouse |
| `trace_worker_jemalloc_resident_bytes` | Gauge | `worker_id` | Resident native heap allocated by `jemalloc` (`mallctl`) |
| `trace_worker_kafka_consumer_lag` | Gauge | `partition` | Kafka consumer group lag per partition (KEDA scaling signal) |
| `trace_worker_spillover_active` | Gauge | `worker_id` | `1` when spooling micro-batches to local disk WAL |
| `trace_worker_clickhouse_errors_total` | Counter | `error_type` | ClickHouse native TCP write/connection errors |
