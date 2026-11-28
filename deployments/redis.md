# Self-Hosted Bitnami Redis 7.2 Cluster (`rag-trace-collector-worker`)

This document describes the dedicated in-cluster Redis 7.2 Cluster topology, local development setup, deployment workflows, and Day-2 operational runbooks for `rag-trace-collector-worker`.

---

## 1. Architecture & Topology

`rag-trace-collector-worker` uses Redis 7.2 Cluster as its **L2 provenance cache** (`internal/provenance/redis_client.py`) behind the process-local 50k LRU cache (`L1`). Keys use `{doc_id}:chunk_id` hash tags so all chunks for a document map to the same Redis hash slot, enabling pipelined slot-local `MGET` batches.

- **Topology**: 6-node cluster (`3` Primary shards + `3` Secondary replicas) via `bitnami/redis-cluster` (`v11.x`)
- **Namespace**: `rag-telemetry`
- **Helm Release**: `rag-trace-collector-worker-redis`
- **Shared Secret**: `rag-trace-collector-worker-secret` (key: `REDIS_PASSWORD`)
- **Persistence**: `5Gi` PVC per node with AOF (`appendonly yes`, `appendfsync everysec`) and RDB snapshots
- **Resources**: Requests `100m` CPU / `256Mi` memory; Limits `500m` CPU / `1Gi` memory; soft pod anti-affinity
- **Metrics**: Prometheus Redis Exporter on port `9121` (`prometheus.io/scrape: "true"`)

### Why Dedicated In-Cluster Redis?
Co-locating a dedicated 6-node Redis Cluster in the `rag-telemetry` namespace eliminates cross-VPC/NAT hops and noisy-neighbor contention, keeping L2 provenance lookups within **sub-millisecond ($P99 < 1\text{ms}$) network RTT**—comfortably inside the worker's `<1.5ms` L2 SLO.

---

## 2. Internal Headless DNS Routing

The worker connects to the cluster seed nodes using Kubernetes StatefulSet headless DNS:

```text
rag-trace-collector-worker-redis-0.rag-trace-collector-worker-redis-headless.rag-telemetry.svc.cluster.local:6379
rag-trace-collector-worker-redis-1.rag-trace-collector-worker-redis-headless.rag-telemetry.svc.cluster.local:6379
rag-trace-collector-worker-redis-2.rag-trace-collector-worker-redis-headless.rag-telemetry.svc.cluster.local:6379
```

Configured via `REDIS_CLUSTER_NODES` in `.env.prod` and synced to `deployments/helm/values.yaml` (`.Values.env.REDIS_CLUSTER_NODES`).

---

## 3. Local Development (Docker Compose)

For local workstation development, start the standalone Redis 7.2 container (`bitnami/redis:7.2-debian-12`):

```bash
docker compose -f deployments/docker/docker-compose.redis.yml up -d
```

- Exposes `localhost:6379` with AOF persistence (`redis_data` volume) and `REDIS_PASSWORD` authentication (defaults to `local-dev-redis-password`).
- Tear down with:
  ```bash
  docker compose -f deployments/docker/docker-compose.redis.yml down
  ```

---

## 4. Kubernetes Deployment & GitOps

### Option A: One-Click Script
Ensure `rag-trace-collector-worker-secret` (with `REDIS_PASSWORD`) exists in `rag-telemetry` (e.g., via `.github/workflows/deploy.yml`), then run:

- **Linux / macOS (Bash)**:
  ```bash
  bash scripts/deploy_redis_cluster.sh
  ```
- **Windows (PowerShell)**:
  ```powershell
  pwsh -File scripts/deploy_redis_cluster.ps1
  ```

### Option B: ArgoCD GitOps
Apply the declarative ArgoCD `Application` manifest:

```bash
kubectl apply -f deployments/argocd/redis-cluster-application.yaml
```

This tracks `bitnami/redis-cluster` (`11.*`) with `deployments/helm/redis-cluster/values.yaml`, automated pruning/self-healing, and exponential retry backoff.

---

## 5. Day-2 Operational Runbooks

### Cluster Health & Slot Inspection
```bash
kubectl exec -n rag-telemetry rag-trace-collector-worker-redis-0 -- \
  sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli cluster info'

kubectl exec -n rag-telemetry rag-trace-collector-worker-redis-0 -- \
  sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli cluster nodes'
```
Verify `cluster_state:ok` and `cluster_slots_assigned:16384`.

### Real-Time Latency Verification
```bash
kubectl exec -n rag-telemetry rag-trace-collector-worker-redis-0 -- \
  sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli --latency -i 1'
```

### Prometheus Metrics Scraping
Verify the exporter sidecar on port `9121`:
```bash
kubectl port-forward -n rag-telemetry svc/rag-trace-collector-worker-redis-metrics 9121:9121
curl -s http://localhost:9121/metrics | head -n 20
```
