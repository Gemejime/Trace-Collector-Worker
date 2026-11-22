# Phase 4 Implementation Plan: Chaos Hardening & Soak Validation

## Objective

Implement **Phase 4: Chaos Hardening & Soak Validation** for `rag-trace-collector-worker` per [`.ai/progress-tracker.md`](../.ai/progress-tracker.md), [`.ai/architecture.md`](../.ai/architecture.md), [`.ai/doc-api.md`](../.ai/doc-api.md), [`.ai/code-standards.md`](../.ai/code-standards.md), and [`.ai/security.md`](../.ai/security.md).

---

## Scope & Deliverables

### 1. Hardened Multi-Stage Container (`deployments/docker/Dockerfile`)
- Multi-stage `python:3.12-slim` image build running as a non-root user (`runAsNonRoot: true`), invoking `rag-trace-worker` (`cmd.worker.main:main`) with `PYTHONUNBUFFERED=1` and `PYTHONDONTWRITEBYTECODE=1`.
- Install `libjemalloc2` and configure architecture-matched `LD_PRELOAD` using Docker `TARGETARCH`:
  - `amd64` (`c6i.xlarge`): `/usr/lib/x86_64-linux-gnu/libjemalloc.so.2` (also symlink `libjemalloc.so` -> `libjemalloc.so.2` since `libjemalloc2` on Debian omits the unversioned `.so` symlink).
  - `arm64` (`c7g.xlarge`): `/usr/lib/aarch64-linux-gnu/libjemalloc.so.2` (also symlink `libjemalloc.so` -> `libjemalloc.so.2`).
- Set `MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:2000` and `PROMETHEUS_MULTIPROC_DIR=/tmp/prom-multiproc`.
- Support `readOnlyRootFilesystem: true` with dedicated writable mounts for `/var/spool/trace-wal` (disk WAL spillover), `/tmp/prom-multiproc`, and `/tmp` (`emptyDir`) for Python `multiprocessing` (`spawn`) / `aiokafka` temp files.

### 2. Helm Chart, KEDA Autoscaling, Security & ArgoCD Progressive Rollout (`deployments/helm/`)
- **`deployments/helm/Chart.yaml`**, **`.helmignore`**, & **`deployments/helm/values.yaml`**:
  - Multi-core pod resource quotas matching compute-optimized nodes (`c6i.xlarge` / `c7g.xlarge`): CPU `2000m` request / `4000m` limit, Memory `1.5Gi` request / `2.5Gi` limit, `WORKER_PROCESS_COUNT=4`.
  - Telemetry & runtime env wiring (`TELEMETRY_HOST=0.0.0.0` overriding the `127.0.0.1` default in `settings.py`, `TELEMETRY_PORT=8080`, `PROMETHEUS_MULTIPROC_DIR=/tmp/prom-multiproc`, plus Kafka/Redis/ClickHouse/time-wheel `WorkerSettings` env vars).
  - KEDA scaling bounds: `minReplicaCount: 4`, `maxReplicaCount: 16`, Kafka consumer group lag threshold (`lagThreshold: "5000"` per partition with `activationLagThreshold` and optional `TriggerAuthentication`). Note: `rag-diagnostic-traces` has `32` partitions, so at `16` replicas × `4` processes (`64` processes), up to `32` worker processes sit idle; ensure `/ready` remains healthy when `0` partitions are assigned to an idle worker.
  - ArgoCD progressive rollout toggle (`rollout.enabled`) with canary steps, pause intervals, and Prometheus analysis gates.
- **`deployments/helm/templates/deployment.yaml`** & **`poddisruptionbudget.yaml`**:
  - Standard Kubernetes `Deployment` (active when `rollout.enabled=false`) with non-root `securityContext` (`runAsNonRoot: true`, `allowPrivilegeEscalation: false`, `capabilities: drop: ["ALL"]`, `readOnlyRootFilesystem: true`), `/healthz` and `/ready` HTTP probes on `:8080`, `emptyDir` mounts for `/tmp/prom-multiproc` and `/tmp`, and `/var/spool/trace-wal` volume mount.
  - `PodDisruptionBudget` (`minAvailable` / `maxUnavailable`) preserving multi-AZ redundancy (`minReplicaCount: 4`) during node drains.
- **`deployments/helm/templates/rollout.yaml`** & **`deployments/helm/argocd/application.yaml`**:
  - Argo Rollouts `Rollout` template (toggleable via `rollout.enabled=true`) alongside `deployment.yaml` for canary progressive delivery (`setWeight` + `pause` + `AnalysisTemplate` scoped to canary pods/revision checking `sum(rate(trace_worker_clickhouse_errors_total{...}[2m]))` and `avg(trace_worker_provenance_resolution_ratio{...})`), plus an ArgoCD `Application` manifest for GitOps sync.
- **`deployments/helm/templates/scaledobject.yaml`**:
  - KEDA `ScaledObject` dynamically targeting `apps/v1 Deployment` (`rollout.enabled=false`) or `argoproj.io/v1alpha1 Rollout` (`rollout.enabled=true`) driven by Kafka lag on `rag-diagnostic-traces` (`4–16` replicas).
- **`deployments/helm/templates/service.yaml`**, **`serviceaccount.yaml`**, **`networkpolicy.yaml`**, & **`prometheusrule.yaml`**:
  - Pod telemetry `Service` on `:8080` and dedicated `ServiceAccount`.
  - `NetworkPolicy` restricting ingress to Prometheus scraper (`/metrics`) and kubelet probes (`/healthz`, `/ready`) on `:8080`, and restricting egress strictly to configurable Kafka broker ports (e.g., `9092`/`9093`/`9094`), Redis 7.2 Cluster ports (`6379`/`6380`), ClickHouse native TCP ports (`9000`/`9440`), and DNS (`:53` UDP and TCP).
  - `PrometheusRule` encoding the 4 production alerts from [`.ai/architecture.md`](../.ai/architecture.md) with accurate multi-process label aggregations:
    1. Elevated Consumer Partition Lag (Warning): `sum(trace_worker_kafka_consumer_lag) > 50000` for `3m`.
    2. Cache Resolution Degradation (Warning `< 0.999` SLO Breach / Critical `< 0.95`): `min by (namespace, pod) (avg_over_time(trace_worker_provenance_resolution_ratio[5m]))`.
    3. ClickHouse Native Ingestion Error (Page): `sum(rate(trace_worker_clickhouse_errors_total[2m])) > 0` for `1m`.
    4. Native Memory Fragmentation Breach (Critical): `sum by (namespace, pod) (trace_worker_jemalloc_resident_bytes) > 0.85 * 2684354560` (85% of `2.5 GiB` container limit) for `2m` (paired with `container_memory_working_set_bytes` cAdvisor guard).

### 3. Observability Sync & Shared Test Fixtures (`internal/telemetry/metrics.py`, `test/fixtures/`)
- Expose `trace_worker_jemalloc_allocated_bytes` alongside `trace_worker_jemalloc_resident_bytes` in `internal/telemetry/metrics.py` (and synchronize `.ai/doc-api.md` in the same change) so Prometheus soak audits can track `jemalloc.allocated` vs. `jemalloc.resident` per `tasks/RAG Trace Collector Worker Implementation Plan.pdf`.
- Shared fixtures in `test/fixtures/`: normalized `TraceSpanRecord` Protobuf payload builders, multi-stage DAG fixtures (`intent_classify` → `dense_retrieval` / `sparse_retrieval` → `rrf_blend` → `cross_encoder` → `parent_hydrate`), and mock MsgPack chunk provenance coordinate payloads.

### 4. Chaos Test Suite (`test/chaos/`)
- Deterministic CI-safe fault-injection tests (plus `@pytest.mark.integration` live-cluster chaos tests where applicable):
  - **60s ClickHouse Outage & WAL Drain**: Simulate ClickHouse connection drops during sustained load; verify Kafka offset advancement pauses, micro-batches spool to `disk_wal.py`, `trace_worker_spillover_active` activates, consumer `poll()`/`getmany()` heartbeats continue within `kafka_max_poll_interval_ms` while paused, pod/worker memory remains bounded, and WAL segments drain preserving exact batch boundaries with `insert_deduplicate=1` (`clickhouse_async_insert=False`) upon reconnect.
  - **Disk WAL Exhaustion Backpressure**: Fill `disk_wal.py` to `disk_wal_max_bytes`; verify `is_full()` flips `/ready` to `503` and pauses Kafka consumption without crashing or exceeding disk quota.
  - **Kafka Rebalance Storms**: Trigger partition revocation and reassignment while DAGs are open in the time-wheel; verify `OffsetManager` contiguous durability, zero split-brain/corrupted DAG state, and clean replay from earliest uncommitted offsets.
  - **Redis Latency Spikes & Cache Degradation**: Inject Redis Cluster timeouts/errors; verify bounded `redis_socket_timeout_seconds`, L1 LRU isolation, `trace_worker_provenance_resolution_ratio` tracking, and zero event-loop stalls.
  - **Missing Root / Intermediate Spans, Corrupt Payloads & Worker Crash Recovery**: Ingest fragmented traces and malformed Protobuf bytes; verify time-wheel `is_partial=True` eviction, safe quarantine without sibling worker crashes, supervisor respawn with `mark_worker_process_dead` shard cleanup, and un-drained WAL recovery on worker restart.

### 5. Performance & 8-Hour Soak Validation (`test/performance/`)
- Parameterized throughput & soak benchmark (`--soak-seconds` pytest CLI option / `SOAK_DURATION_SECONDS` env var, plus `@pytest.mark.soak` for full-scale enforcement):
  - Defaults to a fast, deterministic CI smoke duration (`2s`–`5s`) verifying pipeline correctness and flat memory growth across iterations (gracefully handling `uvloop`/`libjemalloc` absence on Windows/macOS dev environments).
  - Supports full `28800s` (8-hour) multi-process execution (`pytest test/performance -v --soak-seconds=28800`) enforcing sustained `>= 12,000 spans/sec` aggregate throughput, `P95 <= 2.5s` assembly-to-flush latency, and bounded RSS / `jemalloc` (`allocated` & `resident`, `<= 2.0 GiB` ceiling) with flat heap growth.

### 6. CI, Documentation Sync & Git Commits
- Update `Makefile` (`test-chaos`, `test-perf`, `helm-lint`, `verify`) and `.github/workflows/ci.yml` to run deterministic `test/integration` (`-m "not integration"`), `test/chaos`, and `test/performance` smoke checks (keeping `@pytest.mark.integration` and `@pytest.mark.soak` / 8-hour runs gated) plus `helm lint` / `helm template` validation.
- Update `.ai/doc-api.md` and `api/openapi/openapi.yaml` if any telemetry/config surface changes (e.g., `trace_worker_jemalloc_allocated_bytes`).
- Update `.ai/progress-tracker.md` marking Phase 4 `Completed` upon verification, and create `>= 2` conventional git commits.

---

## Verification Plan

1. `ruff format --check .` and `ruff check .`
2. `mypy --strict cmd internal`
3. `bandit -r cmd internal -ll -ii` and `pip-audit`
4. OpenAPI 3.1 validation (`openapi-spec-validator`) & `.ai/doc-api.md` sync check
5. `pytest test/unit test/integration test/chaos test/performance -v -m "not integration and not soak"`
6. `helm lint deployments/helm` and `helm template deployments/helm` (both `rollout.enabled=false` and `rollout.enabled=true`)
