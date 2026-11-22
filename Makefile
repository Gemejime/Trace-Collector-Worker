PYTHON ?= python
PROTO_DIR := api/proto/v1

.PHONY: proto lint format-check typecheck security test-unit ensure-topic verify

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

ensure-topic:
	$(PYTHON) scripts/ensure_kafka_topic.py

verify: format-check lint typecheck security test-unit
