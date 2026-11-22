# RAG Trace Collector Worker (`rag-trace-collector-worker`) - Project Overview

## Overview

The **`rag-trace-collector-worker`** is an asynchronous pipeline microservice designed to collect, assemble, and enrich execution trace DAGs from search retrieval operations without impacting online search latency. Implemented in Python 3.12+ with a multi-process `uvloop` runtime and `jemalloc` memory tuning, it ingests Protobuf diagnostic spans from Kafka, hydrates chunk provenance and normalized bounding box coordinates via a process-local 50,000-entry LRU cache and pipelined Redis 7.2 Cluster lookups, assembles execution DAGs in a bucketed time-wheel, and streams micro-batched analytical records directly into ClickHouse via native TCP for monitoring and offline downstream evaluation.

```text
REVISED WORKER ARCHITECTURE
[ rag-search-orchestrator ] ──► (Non-blocking Out-of-Band Producer)
                                      │
                                      ▼ (Partition Key: hash(trace_id))
               [ Kafka Topic: rag-diagnostic-traces (32 Partitions) ]
                                      │
           ┌──────────────────────────┴──────────────────────────┐
           ▼                                                     ▼
[ Worker Pod 1: Subprocess 1 ]                        [ Worker Pod 1: Subprocess 2 ]
├── Protobuf Byte Deserialization                     ├── Protobuf Byte Deserialization
├── In-Memory LRU (50k Chunks)                        ├── In-Memory LRU (50k Chunks)
│   └── Redis Cluster Pipeline Fallback               │   └── Redis Cluster Pipeline Fallback
├── Bucketed Time-Wheel Assembler                     ├── Bucketed Time-Wheel Assembler
└── Native Micro-Batch Accumulator                    └── Native Micro-Batch Accumulator
           │                                                     │
           └──────────────────────────┬──────────────────────────┘
                                      │ TCP / Native Protocol (50k rows or 2.0s)
                                      ▼
                     [ ClickHouse: ReplicatedMergeTree ]
                                      │
                                      ▼ (Scheduled Poller / Materialized View)
                     [ rag-eval-worker / Temporal Queue ]
```

---

## Strategic Objectives

1. **Asynchronous Provenance Graph Assembly**: Ingest diagnostic spans and retrieval records emitted out-of-band by `rag-search-orchestrator` and telemetry collectors, reconciling execution DAGs across query intent classification, dense/sparse retrieval fan-out, RRF blending, cross-encoder reranking, and parent container hydration.
2. **Deep Coordinate & Version Mapping**: Stitch precise document lineage into every trace node by binding content-addressed chunk identities, monotonic document versions ($v_1 \dots v_n$), parent container hashes, and normalized visual bounding box coordinates ($0.0 \le x, y \le 1.0$).
3. **Normalized Analytical Persistence**: Micro-batch enriched trace graphs, stage latency waterfalls, and rank transition deltas ($\text{Dense/Sparse} \rightarrow \text{RRF} \rightarrow \text{Cross-Encoder}$) directly into ClickHouse without transactional overhead on operational databases.
4. **Evaluation Candidate Tagging**: Store evaluation metadata tags and interaction markers directly within the ClickHouse trace schemas, allowing downstream evaluation engines (`rag-eval-worker` / `rag-eval-governance-worker`) to query candidate batches offline without active worker orchestration.
5. **Continuous Documentation & Contract Synchronization**: Ensure OpenAPI 3.1 specifications (`api/openapi/openapi.yaml`) and [`.ai/doc-api.md`](doc-api.md) are continuously synchronized with every code change or addition, no matter how small.

---

## Calibrated Quantitative SLOs & SLIs

- **Graph Assembly Latency**: **P95 $\le 2.5\text{ seconds}$** from Kafka trace span receipt to final ClickHouse ingestion.
- **Sustained Processing Throughput**: **$\ge 12,000\text{ spans/sec}$** aggregate per pod (achieved via multi-process consumer scaling pinned to available CPU cores).
- **Lineage Resolution Rate**: **$\ge 99.9\%$** of retrieved chunk spans successfully resolved and joined to their parent document version and bounding box coordinates.
- **Memory Footprint & Allocation Invariants**: **RSS strictly $\le 2.0\text{ GiB}$ per pod** enforced under prolonged high-concurrency bursts using `jemalloc` tuning, monitored via runtime `mallctl` allocator counters (`jemalloc.allocated` vs. `jemalloc.resident`).
- **Upstream Architecture Invariant (External)**: Non-blocking execution of trace emission by `rag-search-orchestrator` (guaranteed upstream via ring buffer emitters; zero latency overhead on search critical paths).
- **Per-Pod Resource Ceilings**:
  - CPU Request / Limit: `2,000m` / `4,000m` (3–4 dedicated worker subprocesses)
  - Memory Request / Limit: `1.5 GiB` / `2.5 GiB` (RSS ceiling $\le 2.0\text{ GiB}$)

---

## Scope & Exclusions

### In-Scope
- **Stream Ingestion**: Consuming raw Protobuf diagnostic spans from Kafka topic `rag-diagnostic-traces` partitioned strictly by `hash(trace_id)`.
- **DAG Construction & Time-Wheel Eviction**: Correlating fragmented OpenTelemetry span IDs into execution DAGs using low-overhead bucketed time wheels for TTL enforcement and `is_partial = true` flagging on timeout.
- **Local L1 + Pipelined Redis Provenance Resolution**: Hydrating chunk identifiers with visual bounding boxes, page indexes, document versions, and parent boundaries via a local in-memory LRU cache (`50,000` entries per process) with fallback to pipelined Redis 7.2 Cluster reads.
- **Direct ClickHouse Sinking**: Micro-batching normalized trace waterfalls, partial/incomplete execution trees, and rank transition matrices directly into ClickHouse via native TCP (`50,000` rows or `2.0s`), with local disk WAL spillover during outages.

### Exclusions (Explicitly Out-of-Scope)
- **Direct Temporal Workflow Dispatch**: Worker does not call Temporal gRPC APIs or manage `eval-task-queue` executions (delegated to offline ClickHouse pollers or cron triggers).
- **On-the-Fly Evaluation Triplet Formulation**: Extracting complex prompt-context triplets and prompt evaluation criteria is handled downstream by materialized views or batch readers.
- **Custom Tenant Partition Rebalancing**: The consumer uses standard Kafka partition group rebalancing across `trace_id` hash partitions without custom tenant rebalance hooks.
- **Regulatory Access & Audit Trail Logging**: Permanent compliance logs (HIPAA, SOC 2) and immutable S3 WORM vaulting are strictly owned by `rag-telemetry-audit-daemon`.
- **Online Retrieval & Ranking**: In-flight query transformation, Roaring Bitmap filtering, and RRF calculations remain owned by `rag-search-orchestrator`.
