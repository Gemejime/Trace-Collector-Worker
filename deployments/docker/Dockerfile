# syntax=docker/dockerfile:1.7

FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY pyproject.toml ./
COPY api ./api
COPY cmd ./cmd
COPY internal ./internal

RUN pip install --upgrade pip \
    && pip install .


FROM python:3.12-slim AS runtime

ARG TARGETARCH=amd64

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}" \
    MALLOC_CONF="background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:2000" \
    LD_PRELOAD="/usr/local/lib/libjemalloc.so.2" \
    PROMETHEUS_MULTIPROC_DIR="/tmp/prom-multiproc" \
    TELEMETRY_HOST="0.0.0.0" \
    TELEMETRY_PORT="8080" \
    DISK_WAL_DIR="/var/spool/trace-wal"

RUN apt-get update && apt-get install -y --no-install-recommends \
        libjemalloc2 \
    && case "${TARGETARCH}" in \
        amd64) LIB_DIR="/usr/lib/x86_64-linux-gnu" ;; \
        arm64) LIB_DIR="/usr/lib/aarch64-linux-gnu" ;; \
        *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && ln -sf "${LIB_DIR}/libjemalloc.so.2" "${LIB_DIR}/libjemalloc.so" \
    && ln -sf "${LIB_DIR}/libjemalloc.so.2" /usr/local/lib/libjemalloc.so.2 \
    && ln -sf "${LIB_DIR}/libjemalloc.so.2" /usr/local/lib/libjemalloc.so \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 worker \
    && useradd --uid 10001 --gid 10001 --shell /usr/sbin/nologin --create-home worker \
    && mkdir -p /var/spool/trace-wal /tmp/prom-multiproc \
    && chown -R 10001:10001 /var/spool/trace-wal /tmp/prom-multiproc

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --chown=10001:10001 api/openapi/openapi.yaml /app/api/openapi/openapi.yaml

USER 10001:10001

EXPOSE 8080

ENTRYPOINT ["rag-trace-worker"]
