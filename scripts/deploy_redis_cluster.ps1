[CmdletBinding()]
param(
    [string]$Namespace = "rag-telemetry",
    [string]$ReleaseName = "rag-trace-collector-worker-redis",
    [string]$SecretName = "rag-trace-collector-worker-secret",
    [string]$ValuesFile = "deployments/helm/redis-cluster/values.yaml"
)

$ErrorActionPreference = "Stop"

foreach ($tool in @("helm", "kubectl")) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        Write-Error "Required tool '$tool' is not installed or not on PATH."
        exit 1
    }
}

if (-not (Test-Path $ValuesFile)) {
    Write-Error "Values file not found: $ValuesFile"
    exit 1
}

Write-Host "Ensuring namespace '$Namespace' exists..."
kubectl get namespace $Namespace *> $null
if ($LASTEXITCODE -ne 0) {
    kubectl create namespace $Namespace
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

kubectl get secret $SecretName -n $Namespace *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Shared secret '$SecretName' not found in namespace '$Namespace'. Inject it via CI/CD or create it with key REDIS_PASSWORD before deploying."
    exit 1
}

Write-Host "Adding/updating Bitnami Helm repository..."
helm repo add bitnami https://charts.bitnami.com/bitnami --force-update | Out-Null
helm repo update bitnami | Out-Null

Write-Host "Deploying '$ReleaseName' (bitnami/redis-cluster) into namespace '$Namespace'..."
helm upgrade --install $ReleaseName bitnami/redis-cluster `
    --namespace $Namespace `
    -f $ValuesFile `
    --set "existingSecret=$SecretName" `
    --wait `
    --timeout 10m
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Waiting for all Redis Cluster StatefulSet pods to become Ready..."
kubectl wait --for=condition=Ready pod `
    -l "app.kubernetes.io/instance=$ReleaseName" `
    -n $Namespace `
    --timeout=300s
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Verifying Redis Cluster slot assignment and health..."
$clusterInfo = kubectl exec -n $Namespace "$ReleaseName-0" -- sh -c 'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli cluster info'
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$clusterInfo | Out-Host
$clusterInfoText = ($clusterInfo -join "`n")
if (($clusterInfoText -notmatch "cluster_state:ok") -or ($clusterInfoText -notmatch "cluster_slots_assigned:16384")) {
    Write-Error "Redis Cluster verification failed (expected cluster_state:ok and cluster_slots_assigned:16384)."
    exit 1
}

Write-Host "SUCCESS: Redis 7.2 Cluster '$ReleaseName' is healthy with 16,384 slots assigned."
