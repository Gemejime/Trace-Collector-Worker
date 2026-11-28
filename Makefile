PYTHON ?= python
PROTO_DIR := api/proto/v1

.PHONY: proto lint format-check typecheck security test-unit test-integration test-chaos test-perf helm-lint ensure-topic verify

proto:
	$(PYTHON) -m grpc_tools.protoc \
		-I . \
		--python_out=. \
		--mypy_out=. \
		$(PROTO_DIR)/trace_record.proto \
		$(PROTO_DIR)/provenance_graph.proto

lint:
	ruff check .

format-check:
	ruff format --check .

typecheck:
	mypy --strict cmd internal

security:
	bandit -r cmd internal -ll -ii
	pip-audit

test-unit:
	pytest test/unit -v

test-integration:
	pytest test/integration -v -m "not integration and not soak"

test-chaos:
	pytest test/chaos -v -m "not integration"

test-perf:
	pytest test/performance -v -m "not soak"

helm-lint:
	helm lint deployments/helm
	helm template rag-trace-worker deployments/helm --set rollout.enabled=false > /dev/null
	helm template rag-trace-worker deployments/helm --set rollout.enabled=true > /dev/null

ensure-topic:
	$(PYTHON) scripts/ensure_kafka_topic.py

verify: format-check lint typecheck security test-unit test-integration test-chaos test-perf
