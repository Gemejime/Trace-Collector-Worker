#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-rag-telemetry}"
RELEASE_NAME="${RELEASE_NAME:-rag-trace-collector-worker-redis}"
SECRET_NAME="${SECRET_NAME:-rag-trace-collector-worker-secret}"
VALUES_FILE="${VALUES_FILE:-deployments/helm/redis-cluster/values.yaml}"

for cmd in helm kubectl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "ERROR: Required tool '$cmd' is not installed or not on PATH." >&2
    exit 1
  fi
done

if [ ! -f "$VALUES_FILE" ]; then
  echo "ERROR: Values file not found: $VALUES_FILE" >&2
  exit 1
fi

echo "Ensuring namespace '$NAMESPACE' exists..."
kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$NAMESPACE"

if ! kubectl get secret "$SECRET_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
  echo "ERROR: Shared secret '$SECRET_NAME' not found in namespace '$NAMESPACE'." >&2
  echo "Inject it via CI/CD (.github/workflows/deploy.yml) or create it with key REDIS_PASSWORD before deploying." >&2
  exit 1
fi

echo "Adding/updating Bitnami Helm repository..."
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update >/dev/null
helm repo update bitnami >/dev/null

echo "Deploying '$RELEASE_NAME' (bitnami/redis-cluster) into namespace '$NAMESPACE'..."
helm upgrade --install "$RELEASE_NAME" bitnami/redis-cluster \
  --namespace "$NAMESPACE" \
  -f "$VALUES_FILE" \
  --wait \
  --timeout 10m

echo "Waiting for all Redis Cluster StatefulSet pods to become Ready..."
kubectl wait --for=condition=Ready pod \
  -l "app.kubernetes.io/instance=${RELEASE_NAME}" \
  -n "$NAMESPACE" \
  --timeout=300s

echo "Verifying Redis Cluster slot assignment and health..."
CLUSTER_INFO="$(kubectl exec -n "$NAMESPACE" "${RELEASE_NAME}-0" -- sh -c \
  'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli cluster info')"
echo "$CLUSTER_INFO"

if ! echo "$CLUSTER_INFO" | grep -q "cluster_state:ok"; then
  echo "ERROR: Redis Cluster state is not 'ok'." >&2
  exit 1
fi
if ! echo "$CLUSTER_INFO" | grep -q "cluster_slots_assigned:16384"; then
  echo "ERROR: Not all 16,384 Redis Cluster slots are assigned." >&2
  exit 1
fi

echo "SUCCESS: Redis 7.2 Cluster '$RELEASE_NAME' is healthy with 16,384 slots assigned."
