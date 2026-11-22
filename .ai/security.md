# Security Context - RAG Trace Collector Worker (`rag-trace-collector-worker`)

## Threat Model & Trust Boundaries

- **Untrusted / Malformed Stream Payloads**: Protobuf diagnostic spans arrive from Kafka topic `rag-diagnostic-traces`. Deserialization must enforce payload size bounds, schema validation, and coordinate bounds clamping (`[0.0, 1.0]`) so malformed or adversarial messages cannot trigger OOMs or crash worker subprocesses.
- **Tenant Isolation**: Every trace span, assembled DAG, and ClickHouse record carries `tenant_id`. ClickHouse `ReplicatedMergeTree` tables are ordered by `(tenant_id, event_date, trace_id)` to enforce logical tenant partitioning for downstream `rag-eval-worker` queries.
- **Local Disk WAL Protection**: During ClickHouse outages, `disk_wal.py` spools micro-batches to an isolated ephemeral/encrypted volume mount with strict size caps so disk saturation cannot starve the node.
- **Worker Runtime Hardening**: Container runs as non-root with `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so`, read-only root filesystem (writable mount restricted solely to the WAL spillover directory), and all Linux capabilities dropped.

---

## Secured Logging Invariants

1. **Structured JSON Telemetry Only**:
   - All logs emitted via `internal/telemetry/logger.py` in JSON format containing correlation metadata (`trace_id`, `span_id`, `tenant_id`, `worker_pid`, `partition`, `stage`, `duration_ms`).
   - `print()` statements are strictly forbidden in `cmd/` and `internal/`.
2. **Credentials & Connection String Redaction**:
   - Kafka SASL/SCRAM passwords, SSL key passphrases, Redis Cluster `AUTH` tokens, ClickHouse native TCP credentials, and full DSNs must be masked/stripped before logging.
3. **PII/PHI & Content Protection**:
   - Never log raw document chunk text, user query text, or tenant PII/PHI in worker stdout/stderr logs. Log only content-addressed identifiers (`chunk_id`, `doc_id`, `doc_version`, `parent_container_hash`) and coordinate metadata.
4. **Error Trace Sanitization**:
   - HTTP `/ready` and `/healthz` responses must never expose internal filesystem paths, stack traces, or credentialed hostnames.

---

## Network & Container Hardening

1. **Multi-Stage Non-Root Container (`deployments/docker/Dockerfile`)**:
   - Non-root UID (`runAsNonRoot: true`), `allowPrivilegeEscalation: false`, `capabilities: drop: ["ALL"]`.
   - `readOnlyRootFilesystem: true` with dedicated `/var/spool/trace-wal` mount for `disk_wal.py`.
2. **Kubernetes Network Policies**:
   - **Ingress**: Restricted to Prometheus scraper on `:8080` (`/metrics`) and kubelet probes (`/healthz`, `/ready`).
   - **Egress**: Allowed only to Apache Kafka 3.7+ brokers, Redis 7.2 Cluster (`:6379`), ClickHouse 24.3+ native TCP (`:9000` / `:9440` TLS), and cluster DNS (`:53`). General external internet egress and Temporal gRPC egress are strictly denied.
3. **Transport Security**:
   - TLS/mTLS for Kafka consumer connections, Redis Cluster, and ClickHouse native TCP in production.

---

## Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
