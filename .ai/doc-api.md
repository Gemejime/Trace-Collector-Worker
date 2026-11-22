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
- `repeated CandidateTrace candidates` *(contains `uint32 doc_id`, `string chunk_id`, `int32 dense_rank`, `int32 sparse_rank`, `int32 rrf_rank`, `int32 rerank_rank`, `double score`, `double logit`)*
- `map<string, string> eval_tags` *(evaluation candidate markers for downstream `rag-eval-worker`)*

### `api/proto/v1/provenance_graph.proto`
Assembled execution DAG and coordinate-enriched record schema for ClickHouse persistence:
- `string trace_id`, `string tenant_id`, `string event_date`
- `bool is_partial` *(set `true` when evicted by the bucketed time-wheel before all expected spans arrive)*
- `double total_duration_ms`
- `repeated EnrichedTraceNode nodes` *(includes `chunk_id`, `doc_id`, `doc_version`, `parent_container_hash`, `page_index`, normalized `bbox [x1, y1, x2, y2]` in $[0.0, 1.0]$, and rank transition deltas `dense -> rrf -> cross_encoder`)*

---

## 3. Redis 7.2 Cluster Provenance Cache Contract

- **L1 Cache**: Process-local thread-safe LRU holding `50,000` hot chunk coordinate hashes (`<0.05ms` hit latency).
- **L2 Cache (Redis 7.2 Cluster)**:
  - **Key Format**: `{doc_id}:chunk_id` *(Redis Cluster hash tag on `{doc_id}` enables slot-grouped multi-node pipelined fetches)*
  - **Payload**: Document version ($v_1 \dots v_n$), `parent_container_hash`, `page_index`, and raw bounding box coordinates normalized by `coordinate_mapper.py` to $[0.0, 1.0]$.
  - **Target Latency**: $<1.5\text{ ms}$ aggregate per pipelined batch.

---

## 4. ClickHouse 24.3+ Analytical Sink Contract (`internal/storage/clickhouse/schemas.py`)

- **Protocol**: Native TCP Protocol (`native_client.py`), zero HTTP/single-insert operations.
- **Two-Tier Micro-Batch Flush**: Flush strictly upon reaching **`50,000` records** OR **`2.0` seconds** (whichever occurs first). Fallback to `async_insert=1` only during extreme write bursts.
- **Engine & Partitioning**:
  - Engine: `ReplicatedMergeTree`
  - Partition By: `toYYYYMM(event_date)`
  - Order By: `(tenant_id, event_date, trace_id)`
- **Downstream Handoff**: Materialized views expose candidate trace batches for `rag-eval-worker` / `rag-eval-governance-worker` offline polling.

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
