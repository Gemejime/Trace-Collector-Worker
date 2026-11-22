# AI Workflow Rules - RAG Trace Collector Worker (`rag-trace-collector-worker`)

## Development Approach

Build this microservice incrementally following a spec-driven workflow:
- The specification documents in `tasks/` (specifically `RAG Trace Collector Worker Implementation Plan.pdf`) and context files in [`.ai/`](.) define system invariants, data structures, and operational boundaries.
- Adhere strictly to the 5-layer backend architecture documented in [`.ai/architecture.md`](architecture.md) aligned with `tasks/Production Grade Backend Concepts.pdf`.
- Implement against these specs without inferring undocumented dependencies or deviating from architectural decision records (ADR-001 through ADR-005).

---

## Human-in-the-Loop Rules

- **Plan First**: Propose an implementation plan in `prompts/<task-name>.md` (or concisely in response) before making substantial code changes, and await confirmation when requirements are ambiguous.
- **Ambiguity Escalation**: If ClickHouse table column types, Kafka SASL auth modes, Redis key schemas, or `TIMEWHEEL_EXPIRY_SECONDS` defaults are ambiguous, prompt for clarification before proceeding.
- **Destructive Action Guards**: Always request human confirmation before running destructive operations, breaking published Protobuf v3 contracts, or removing production Helm/ArgoCD manifests.
- **Demonstrate & Verify**: Provide clear verification steps, unit/integration/benchmark test output, and linting/security checks after completing any unit.

---

## Non-Negotiable Invariants

1. **Continuous Doc API & OpenAPI Maintenance**: [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` MUST be updated on **every code change or addition**, regardless of how small the modification is, whenever any endpoint, schema, proto, Kafka/Redis/ClickHouse contract, or metric changes.
2. **Secured Logging**: All logs emitted by `rag-trace-collector-worker` must be structured JSON (`internal/telemetry/logger.py`). Strictly redact Kafka/Redis/ClickHouse credentials, DSNs, raw chunk text, and tenant PII/PHI before emission. Never use `print()` in `cmd/` or `internal/`.
3. **Partition Key Anchored to `trace_id`**: Strictly preserve `hash(trace_id)` partitioning on `rag-diagnostic-traces` (ADR-001); never introduce custom tenant rebalance hooks.
4. **Memory & Allocator Discipline**: Enforce `jemalloc` preloading, 50k-entry process-local LRU bounds, and bucketed time-wheel ring buffers so pod RSS stays $\le 2.0\text{ GiB}$ at $\ge 12,000\text{ spans/sec}$.
5. **Atomic Offset & WAL Guarantees**: Never commit Kafka offsets before ClickHouse confirms micro-batch disk persistence; spool to `disk_wal.py` on ClickHouse outages.
6. **Git Commit Standards**:
   - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
   - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
     - `feat:` for new features 
     - `fix:` for bug fixes
     - `refactor:` for code refactoring
     - `docs:` for documentation updates
     - `tests:` for tests addition and modification
     - `chore:` for maintenance tasks and environment setup
7. **Quality Gates**: `ruff check`, `ruff format --check`, `mypy --strict`, `bandit`, `pip-audit`, Spectral OpenAPI validation, and `pytest` must pass cleanly before any commit.

---

## Pre-Commit Verification Checklist

Before committing code, submitting changes, or marking a task complete:

1. **Formatting & Linting**: Run `ruff format --check .` and `ruff check .`.
2. **Strict Type Checking**: Run `mypy --strict cmd internal`.
3. **Security & Secured Logging Audit**: Run `bandit -r cmd internal -ll -ii`, `pip-audit`, and verify zero `print()` calls, credentials, DSNs, raw chunk text, or PII/PHI in logs.
4. **Automated Tests & Benchmarks**: Ensure `pytest test/unit -v` passes and time-wheel / coordinate mapping / micro-batching invariants hold.
5. **Doc API & OpenAPI Sync**: Confirm [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` reflect any new or modified endpoints, schemas, protos, Kafka/Redis/ClickHouse contracts, or metrics.
6. **Progress Tracker**: Update [`.ai/progress-tracker.md`](progress-tracker.md) with session notes and completed items.
