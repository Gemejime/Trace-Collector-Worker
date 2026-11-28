# GEMINI.md — RAG Trace Collector Worker (`rag-trace-collector-worker`)

This repository implements **`rag-trace-collector-worker`**, the asynchronous Python 3.12+ multi-process microservice that consumes diagnostic spans from Kafka (`rag-diagnostic-traces`), resolves chunk provenance via a 50k local LRU cache and pipelined Redis 7.2 Cluster lookups, assembles execution DAGs in a bucketed time-wheel, and streams micro-batches into ClickHouse via native TCP.

## Authoritative Context & Agent Instructions

Before planning, editing, or reviewing any code, read and follow the specifications in [`.ai.md`](.ai.md) and the `.ai/` directory—starting with [`.ai/agents.md`](.ai/agents.md):

1. **[`.ai/agents.md`](.ai/agents.md)** — Principal Distributed Systems & Python Backend Engineer persona, system role, core operational invariants, and implementation workflow.
2. **[`.ai/project-overview.md`](.ai/project-overview.md)** — Objectives, calibrated SLOs/SLIs ($\text{P95} \le 2.5\text{s}$, $\ge 12,000\text{ spans/sec}$, $\ge 99.9\%$ lineage resolution, $\text{RSS} \le 2.0\text{ GiB}$), and scope exclusions.
3. **[`.ai/architecture.md`](.ai/architecture.md)** — Production-grade 5-layer backend architecture, `uvloop` + `jemalloc` runtime, folder structure, Kubernetes/KEDA sizing, and Prometheus alerting matrix.
4. **[`.ai/doc-api.md`](.ai/doc-api.md)** — Authoritative contract reference for HTTP (`/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, `/docs`), Protobuf v3 (`api/proto/v1/`), Kafka, Redis Cluster, ClickHouse `ReplicatedMergeTree`, and Prometheus metrics.
5. **[`.ai/code-standards.md`](.ai/code-standards.md)** — Python 3.12+ (`ruff`, `mypy --strict`), multi-process `uvloop` isolation, bucketed time-wheel, two-tier micro-batching (`50,000` rows / `2.0s`), and atomic Kafka offset commits.
6. **[`.ai/security.md`](.ai/security.md)** — Threat model, tenant isolation, non-root container hardening, WAL safety, and secured logging invariants.
7. **[`.ai/ai-workflow-rules.md`](.ai/ai-workflow-rules.md)** — Spec-driven development rules, human-in-the-loop guards, and pre-commit verification checklist.
8. **[`.ai/progress-tracker.md`](.ai/progress-tracker.md)** — Phased implementation roadmap, ADRs (ADR-001 to ADR-005), and operational runbooks.

---

## Mandatory Engineering Invariants

- **Continuous Doc API & OpenAPI Sync**: Whenever any endpoint, schema, Protobuf message, Kafka/Redis/ClickHouse contract, or metric is created or modified—no matter how small—update [`.ai/doc-api.md`](.ai/doc-api.md) and [`api/openapi/openapi.yaml`](api/openapi/openapi.yaml) in the same change.
- **Secured Logging**: Emit structured JSON logs only (`internal/telemetry/logger.py`). Never use `print()` in `cmd/` or `internal/`, and strictly redact credentials, DSNs, raw chunk text, and PII/PHI.
- **Quality Gates**: Ensure `ruff check .`, `ruff format --check .`, `mypy --strict cmd internal`, `bandit -r cmd internal -ll -ii`, OpenAPI 3.1 validation, and `pytest` pass before committing.

## Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
