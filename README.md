# `rag-trace-collector-worker`

Asynchronous, memory-bounded Python 3.12+ multi-process microservice that consumes out-of-order Protobuf diagnostic spans from Kafka (`rag-diagnostic-traces`), resolves document chunk provenance via a 50k process-local LRU cache + pipelined Redis 7.2 Cluster lookups, assembles execution DAGs in a bucketed time-wheel, and streams micro-batches into ClickHouse via native TCP.

---

## Key Capabilities & SLOs

- **Graph Assembly Latency**: $\text{P95} \le 2.5\text{ seconds}$ from Kafka span receipt to ClickHouse persistence.
- **Throughput**: $\ge 12,000\text{ spans/sec}$ per pod across 3–4 isolated `uvloop` worker subprocesses.
- **Lineage Resolution**: $\ge 99.9\%$ chunk-to-document/bounding-box resolution (`L1` 50k LRU $<0.05\text{ ms}$ + `L2` pipelined Redis 7.2 Cluster $<1.5\text{ ms}$).
- **Memory Ceiling**: Pod RSS $\le 2.0\text{ GiB}$ with `jemalloc` preloading and backpressure-driven partition pausing.
- **Durability**: Contiguous per-partition Kafka offset commits strictly after ClickHouse flush ACKs, plus local disk WAL spillover (`disk_wal.py`) during downstream outages.

---

## Repository Layout

```text
rag-trace-collector-worker/
├── cmd/worker/                  # Supervisor (main.py) & isolated uvloop worker (process_runner.py)
├── internal/
│   ├── config/                  # 12-factor Pydantic WorkerSettings (.env + env vars)
│   ├── consumer/                # aiokafka consumer & contiguous per-partition offset manager
│   ├── graph/                   # Bucketed time-wheel, DAG state machine, rank delta tracker
│   ├── provenance/              # 50k LRU cache, Redis 7.2 Cluster client, [0,1] bbox mapper
│   ├── storage/                 # ClickHouse native TCP client, 50k/2s batch buffer, disk WAL
│   └── telemetry/               # /healthz, /ready, /metrics, /openapi.yaml, /docs, JSON logger
├── api/
│   ├── proto/v1/                # Protobuf v3 contracts (trace_record.proto, provenance_graph.proto)
│   └── openapi/                 # OpenAPI 3.1 specification (openapi.yaml)
├── deployments/
│   ├── argocd/                  # ArgoCD Application manifests (Redis Cluster)
│   ├── docker/                  # Hardened non-root Dockerfile & local Redis Compose
│   ├── helm/                    # Worker Helm chart, KEDA ScaledObject, Argo Rollout, Redis values
│   ├── redis.md                 # Self-hosted Bitnami Redis 7.2 Cluster guide & Day-2 runbooks
│   └── SECRET_MANAGEMENT.md     # Secret boundaries, Kubernetes Secret patterns & CI/CD injection
├── scripts/
│   ├── ensure_kafka_topic.py    # Idempotent Kafka topic & partition verifier
│   ├── sync_env_to_deployments.py # .env.prod -> deployments/helm/values.yaml sync & drift check
│   └── deploy_redis_cluster.*   # One-click Bash/PowerShell Bitnami Redis Cluster deployment
└── test/                        # Unit, integration, chaos, and performance/soak test suites
```

---

## Local Development

### 1. Environment Setup
```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows:     .venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

Copy `.env.example` to `.env` for local development (`.env` is gitignored and loaded automatically by `WorkerSettings`):
```bash
cp .env.example .env
```

### 2. Start Local Redis 7.2 (Optional)
```bash
docker compose -f deployments/docker/docker-compose.redis.yml up -d
```

### 3. Run the Worker
```bash
rag-trace-worker
```
Observability endpoints are served on `http://127.0.0.1:8080`:
- `GET /healthz` — Liveness probe
- `GET /ready` — Dependency readiness (Kafka, Redis, ClickHouse, Disk WAL)
- `GET /metrics` — Prometheus multiprocess metrics
- `GET /openapi.yaml` & `GET /docs` — OpenAPI 3.1 spec & Swagger UI

---

## Quality Gates & Testing

Run formatting, linting, strict typechecking, security scanning, and tests via `Makefile`:

```bash
make format-check
make lint
make typecheck
make security
make test-unit
make test-integration
make test-chaos
make test-perf
```

---

## Configuration & Deployment

- **Local vs. Production Env**:
  - `.env` is for **local development only**.
  - `.env.prod` (gitignored) is the single source of truth for production Helm `ConfigMap` values.
  - Sync `.env.prod` to `deployments/helm/values.yaml` (sensitive `PASSWORD`/`SECRET`/`TOKEN`/`KEY` keys are automatically excluded):
    ```bash
    python scripts/sync_env_to_deployments.py --sync
    python scripts/sync_env_to_deployments.py --check-only
    ```
  - A Git pre-commit hook (`.git/hooks/pre-commit`) enforces zero drift before commits.
- **Secrets & CI/CD**: See [`deployments/SECRET_MANAGEMENT.md`](deployments/SECRET_MANAGEMENT.md) and `.github/workflows/deploy.yml` for GitHub Actions Kubernetes Secret injection (`rag-trace-collector-worker-secret`).
- **Redis 7.2 Cluster**: See [`deployments/redis.md`](deployments/redis.md) for local Docker Compose, one-click Helm deployment, and ArgoCD GitOps setup.
