# Secret Management — `rag-trace-collector-worker`

This document defines how non-sensitive configuration and sensitive credentials are separated, injected, and rotated for `rag-trace-collector-worker` across local development, CI/CD, and Kubernetes/Helm deployments.

---

## 1. Configuration vs. Secrets Boundary

| Layer | Source of Truth | Tracked in Git? | Synced to Helm `ConfigMap`? |
| --- | --- | --- | --- |
| **Local Development** | `.env` (copied from `.env.example`) | No (`.gitignore`) | No |
| **Non-Sensitive Deploy Config** | `.env.prod` | No (`.gitignore`) | Yes (`scripts/sync_env_to_deployments.py --sync` → `deployments/helm/values.yaml` → `templates/configmap.yaml`) |
| **Sensitive Credentials** | GitHub Environment Secrets / Cluster Secrets | Never | Never (excluded by `PASSWORD`, `SECRET`, `TOKEN`, `KEY` filter) |

`scripts/sync_env_to_deployments.py` (`is_sensitive_key`) performs a name-based filter that excludes any key whose name contains `PASSWORD`, `SECRET`, `TOKEN`, or `KEY` from `deployments/helm/values.yaml` and `ConfigMap`. Credentials with other variable names may still be written to `deployments/helm/values.yaml` unless renamed to match the filter or omitted from `.env.prod`.

---

## 2. Secret Environment Variables

The worker's `WorkerSettings` (`internal/config/settings.py`) expects the following optional/conditional `SecretStr` credentials:

| Variable | Component | When Required |
| --- | --- | --- |
| `KAFKA_SASL_USERNAME` | Kafka (`aiokafka`) | `KAFKA_SECURITY_PROTOCOL` is `SASL_PLAINTEXT` or `SASL_SSL` |
| `KAFKA_SASL_PASSWORD` | Kafka (`aiokafka`) | `KAFKA_SECURITY_PROTOCOL` is `SASL_PLAINTEXT` or `SASL_SSL` |
| `KAFKA_SSL_KEY_PASSWORD` | Kafka (`aiokafka`) | Encrypted client TLS private key (`SSL` / `SASL_SSL`) |
| `REDIS_PASSWORD` | Redis 7.2 Cluster | Redis Cluster authentication enabled |
| `CLICKHOUSE_PASSWORD` | ClickHouse 24.3+ | ClickHouse user password authentication enabled |

> **KEDA Autoscaling**: When `keda.triggerAuthentication.enabled: true` in `deployments/helm/values.yaml`, KEDA references a `TriggerAuthentication` named `rag-trace-kafka-trigger-auth` for Kafka lag polling.

---

## 3. Kubernetes Secret Injection Patterns

The Helm chart (`deployments/helm/`) supports two secret injection patterns in `templates/deployment.yaml` and `templates/rollout.yaml`:

### Option A: Unified Secret (`env.secretName`) — *Default for CI/CD*
1. Create a single Kubernetes Secret (e.g., `rag-trace-collector-worker-secret`) containing any needed keys (`KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`, `REDIS_PASSWORD`, `CLICKHOUSE_PASSWORD`, etc.).
2. Set `env.secretName: "rag-trace-collector-worker-secret"` in `deployments/helm/values.yaml` (preserved across `--sync` runs) or pass `--set env.secretName=rag-trace-collector-worker-secret`.
3. `deployment.yaml` and `rollout.yaml` mount it via `envFrom.secretRef`.

### Option B: Per-Service Secrets (`kafka.secretName`, `redis.secretName`, `clickhouse.secretName`)
Used when platform teams manage separate Secrets per backing service:
- `kafka.secretName` → injects `KAFKA_SASL_USERNAME` (`key: username`) and `KAFKA_SASL_PASSWORD` (`key: password`)
- `redis.secretName` → injects `REDIS_PASSWORD` (`key: password`)
- `clickhouse.secretName` → injects `CLICKHOUSE_PASSWORD` (`key: password`)

Rendered via `rag-trace-collector-worker.secretEnv` in `deployments/helm/templates/_helpers.tpl`.

---

## 4. CI/CD Secret Injection (GitHub Actions)

`.github/workflows/deploy.yml` implements automated, idempotent secret injection using **GitHub Environments** (`staging` / `production`):

1. **Configure GitHub Environment Secrets** under **Settings → Environments → `<env>`**:
   - `KUBECONFIG_B64` *(required)*: Base64-encoded kubeconfig with permissions to manage Secrets and Helm releases in the target namespace.
   - `KAFKA_SASL_USERNAME`
   - `KAFKA_SASL_PASSWORD`
   - `KAFKA_SSL_KEY_PASSWORD` *(optional)*
   - `REDIS_PASSWORD`
   - `CLICKHOUSE_PASSWORD`
2. **Idempotent Secret Apply (`inject-secrets` job)**:
   - Decodes `KUBECONFIG_B64` to `$RUNNER_TEMP/kubeconfig` (`chmod 600`).
   - Writes non-empty secret variables to `$RUNNER_TEMP/worker-secrets.env` (`chmod 600`) with shell tracing disabled (`set +x`).
   - Runs `kubectl create secret generic ... --dry-run=client -o yaml | kubectl apply -f -`.
   - Deletes temporary credential files in `if: always()`.
3. **Helm Deployment (`deploy-helm` job)**:
   - Runs `helm upgrade --install` with `--set env.secretName=rag-trace-collector-worker-secret`.

---

## 5. Rotation & Security Hygiene

- **ConfigMap changes** automatically trigger rolling restarts via the `checksum/config` annotation in `deployment.yaml` and `rollout.yaml`.
- **Secret rotation**: After updating a secret in GitHub Environments and re-running `.github/workflows/deploy.yml` (or applying via `kubectl`), trigger a rolling restart so pods pick up the rotated values:
  ```bash
  kubectl rollout restart deployment/rag-trace-worker-rag-trace-collector-worker -n rag-platform
  # Or for Argo Rollouts:
  kubectl argo rollouts restart rag-trace-worker-rag-trace-collector-worker -n rag-platform
  ```
- **Static Auditing**: `.github/workflows/ci.yml` runs `bandit` and a custom regex audit on `cmd/` and `internal/` to block hardcoded credentials, DSNs, and `print()` statements.
- **Runtime Redaction**: `internal/telemetry/logger.py` redacts credentials, DSNs, raw chunk text, and PII/PHI from all structured JSON logs, and `WorkerSettings` uses `hide_input_in_errors=True` with `SecretStr`.
