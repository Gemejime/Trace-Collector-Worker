# Phase 1 Implementation Plan: Ingress & Multi-Process Architecture

## Objective

Implement **Phase 1: Ingress & Multi-Process Architecture** for `rag-trace-collector-worker` per [`.ai/progress-tracker.md`](../.ai/progress-tracker.md), [`.ai/architecture.md`](../.ai/architecture.md), [`.ai/doc-api.md`](../.ai/doc-api.md), [`.ai/code-standards.md`](../.ai/code-standards.md), and [`.ai/security.md`](../.ai/security.md).

---

## Scope & Deliverables

### 1. Build, Packaging & Tooling
- **`pyproject.toml`**:
  - Python `>=3.12` project metadata and runtime dependencies: `aiokafka`, `protobuf`, `pydantic-settings`, `structlog`, and `uvloop` (environment-marker gated `sys_platform != "win32"` with `asyncio` fallback on Windows for local tests).
  - `[project.optional-dependencies].dev`: `ruff`, `mypy`, `bandit`, `pip-audit`, `pytest`, `pytest-asyncio`, `pytest-cov`, `grpcio-tools`, `mypy-protobuf`, `pyyaml`, `openapi-spec-validator`.
  - Tool configurations for `ruff` (lint + format), `mypy --strict`, `pytest`, and `bandit`.
- **`Makefile`**:
  - Targets: `proto` (generate `_pb2.py` and `.pyi` stubs via `python -m grpc_tools.protoc` + `mypy-protobuf`), `lint`, `format-check`, `typecheck`, `security`, `test-unit`, `verify`.

### 2. Protobuf v3 Stream & DAG Contracts (`api/proto/v1/`)
- **`api/proto/v1/trace_record.proto`**:
  - `TraceSpanRecord`: `trace_id`, `span_id`, `parent_span_id`, `tenant_id`, `stage`, `start_time_ns`, `end_time_ns`, `is_terminal`, `expected_span_count`, `repeated CandidateTrace candidates`, `map<string, string> eval_tags`.
  - `CandidateTrace`: `doc_id` (`uint32`), `chunk_id` (`string`), `dense_rank` (`int32`), `sparse_rank` (`int32`), `rrf_rank` (`int32`), `rerank_rank` (`int32`), `score` (`double`), `logit` (`double`), with `-1` sentinel for stage absence.
- **`api/proto/v1/provenance_graph.proto`**:
  - `ProvenanceGraph`: `trace_id`, `tenant_id`, `event_date`, `is_partial`, `total_duration_ms`, `repeated EnrichedTraceNode nodes`, `map<string, string> eval_tags`.
  - `EnrichedTraceNode`: `chunk_id`, `doc_id` (`uint32`), `doc_version` (`uint32`), `parent_container_hash`, `page_index` (`uint32`), normalized `BoundingBox bbox` (`x1`, `y1`, `x2`, `y2` in `[0.0, 1.0]`), per-stage ranks/scores (`dense_rank`, `sparse_rank`, `rrf_rank`, `rerank_rank`, `score`, `logit`), and rank transition deltas (`dense_to_rrf_delta`, `rrf_to_rerank_delta`, `dense_to_rerank_delta`).
- Generated `_pb2.py` and `_pb2.pyi` stubs (and `__init__.py`) **must be committed** in `api/proto/v1/` so CI (`mypy --strict` and `pytest`) runs without requiring a system `protoc` binary.

### 3. Configuration & Secured JSON Logging
- **`internal/config/settings.py`**:
  - 12-factor `pydantic-settings` `WorkerSettings` covering:
    - Process pool (`WORKER_PROCESS_COUNT` default `4`, `GRACEFUL_DRAIN_TIMEOUT_SECONDS` e.g. `20.0`, crash-loop respawn backoff & max-restart bounds).
    - Kafka (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC="rag-diagnostic-traces"`, `KAFKA_TOPIC_PARTITIONS=32`, `KAFKA_GROUP_ID`, security protocol `PLAINTEXT` / `SSL` / `SASL_PLAINTEXT` / `SASL_SSL`, SASL mechanism `SCRAM-SHA-256` / `SCRAM-SHA-512` & credentials with `SecretStr` masking, TLS CA/cert/key paths + `ssl_key_password: SecretStr`, `max_poll_interval_ms`, `max_payload_bytes`, and bounded memory fetch limits: `max_poll_records`, `fetch_max_bytes`, `max_partition_fetch_bytes`, `max_in_flight_offsets`).
    - Redis & ClickHouse connection settings (with `SecretStr` for passwords/tokens and TLS flags).
    - `TIMEWHEEL_EXPIRY_SECONDS=1.5` (calibrated for the `P95 <= 2.5s` SLO alongside `2.0s` ClickHouse batch flush), `BATCH_MAX_ROWS=50000`, `BATCH_FLUSH_INTERVAL_SECONDS=2.0`, `DISK_WAL_DIR`.
- **`internal/telemetry/logger.py`**:
  - Structured JSON logger (`structlog` / JSON formatter) emitting `trace_id`, `span_id`, `tenant_id`, `worker_pid`, `partition`, `stage`, `duration_ms` with safe default bindings for supervisor/lifecycle logs outside span context.
  - Strict redaction processor stripping Kafka/Redis/ClickHouse credentials, SSL passphrases, DSNs, bearer tokens, raw chunk text, and PII/PHI. Zero `print()` calls and zero credential/DSN example literals (including inside docstrings) in `cmd/` or `internal/` per CI hygiene checks.

### 4. Kafka Ingress, Topic Contract & Contiguous Offset Durability (`internal/consumer/`)
- **`internal/consumer/offset_manager.py`**:
  - Implements **ADR-001** per-partition contiguous offset durability:
    - Tracks fetched/in-flight offsets per partition.
    - Marks offsets durable on ClickHouse flush ACK (`mark_durable`) **and** marks quarantined/corrupt offsets resolved (`mark_quarantined`) so a malformed record never permanently deadlocks the contiguous commit watermark.
    - Computes highest contiguous committable offset per partition (`OffsetAndMetadata(highest_contiguous_offset + 1, "")`), never advancing past an earlier unresolved/unflushed offset.
    - Handles partition assignment and `on_partitions_revoked` cleanly (committing durable contiguous offsets before clearing revoked partition state).
- **`internal/consumer/kafka_consumer.py`**:
  - Process-isolated `aiokafka.AIOKafkaConsumer` consuming `rag-diagnostic-traces` (32 partitions, partitioned upstream by `hash(trace_id)` with `tenant_id` payload metadata).
  - Validates topic partition metadata on startup (warns if partition count != `KAFKA_TOPIC_PARTITIONS`), enforces `max_payload_bytes` and strict Protobuf deserialization, and quarantines/counts corrupt records without crashing the worker.
  - Enforces bounded in-flight buffering (`max_in_flight_offsets` high-water mark) via `pause()` / `resume()` backpressure while maintaining consumer group heartbeats within `max_poll_interval_ms`.
- **Topic Provisioning Helper (Local Dev / Exit Gate)**:
  - Provide a lightweight topic-setup/verification script or admin helper (e.g., `scripts/ensure_kafka_topic.py` or `Makefile` target) creating/verifying `rag-diagnostic-traces` with `32` partitions (`hash(trace_id)` partition key contract) for local integration testing and Phase 1 exit-gate verification.

### 5. Multi-Process Supervisor & Worker Runtime (`cmd/worker/`)
- **`cmd/worker/process_runner.py`**:
  - Initializes per-process `uvloop` (with `asyncio` fallback on Windows), `WorkerSettings`, secured logger, `OffsetManager`, and `KafkaSpanConsumer`.
  - Handles `SIGTERM`/`SIGINT` graceful drain and sends periodic IPC heartbeats to the supervisor.
- **`cmd/worker/main.py`**:
  - Supervisor entrypoint using `multiprocessing.get_context("spawn")` to launch `WORKER_PROCESS_COUNT` (`3–4`) isolated worker subprocesses cleanly across Linux and Windows.
  - Monitors worker health/IPC heartbeats and exit codes, respawning crashed subprocesses with bounded exponential backoff / crash-loop protection without disrupting sibling workers, and exposing a dead-PID cleanup hook for Phase 3's `prometheus_client.multiprocess.mark_process_dead(pid)`.
  - Coordinates graceful shutdown on `SIGINT`/`SIGTERM` by forwarding termination signals, waiting up to `GRACEFUL_DRAIN_TIMEOUT_SECONDS`, and escalating to `terminate()`/`kill()` for hung workers.

### 6. Unit Tests, Docs & Git Commits
- **`test/unit/`** (all unit tests run in isolation with zero live Kafka broker dependency):
  - `test_settings.py`: Environment loading, `SecretStr` redaction in `repr`/logs, validation bounds (including fetch/memory bounds).
  - `test_logger.py`: JSON log format, mandatory/default correlation fields, credential/DSN/PII redaction.
  - `test_proto_contracts.py`: `TraceSpanRecord` and `ProvenanceGraph` (including `EnrichedTraceNode` ranks/scores/deltas) round-trip serialization.
  - `test_offset_manager.py`: Out-of-order flush ACKs, quarantined/corrupt offset resolution, `offset + 1` contiguous commit boundary, partition revocation.
  - `test_supervisor.py`: Worker subprocess spawn (`spawn` context) using a lightweight dummy worker target, automatic crash respawn isolation + backoff, and graceful shutdown.
- **Contract & Roadmap Sync**:
  - Update `.ai/doc-api.md` if any Protobuf field details or contract notes are refined.
  - Update `.ai/progress-tracker.md` upon completion.
  - Commit incrementally using Conventional Commits ($\ge 2$ commits).

---

## Verification Plan

1. `ruff format --check .` and `ruff check .`
2. `mypy --strict cmd internal`
3. `bandit -r cmd internal -ll -ii` and `pip-audit`
4. Secured logging audit (zero `print()` in `cmd/` or `internal/`, zero unredacted secrets/DSNs)
5. `pytest test/unit -v`

---

## Open Clarifications (Human-in-the-Loop)

1. **Kafka Auth Modes**: Confirm supporting `PLAINTEXT` (local dev/tests), `SSL` (mTLS), `SASL_PLAINTEXT`, and `SASL_SSL` (`SCRAM-SHA-256` / `SCRAM-SHA-512`) per `.ai/security.md`.
2. **`TIMEWHEEL_EXPIRY_SECONDS` Default**: Confirm `1.5` seconds as default (recommended over `2.0s` so `1.5s` time-wheel + `2.0s` ClickHouse batch flush preserves headroom for the `P95 <= 2.5s` SLO).
