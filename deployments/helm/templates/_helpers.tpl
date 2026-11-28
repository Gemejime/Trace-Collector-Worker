{{- define "rag-trace-collector-worker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rag-trace-collector-worker.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "rag-trace-collector-worker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rag-trace-collector-worker.labels" -}}
helm.sh/chart: {{ include "rag-trace-collector-worker.chart" . }}
{{ include "rag-trace-collector-worker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "rag-trace-collector-worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rag-trace-collector-worker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "rag-trace-collector-worker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "rag-trace-collector-worker.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "rag-trace-collector-worker.telemetryPort" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.worker.telemetryPort (get $env "TELEMETRY_PORT") -}}
{{- end }}

{{- define "rag-trace-collector-worker.diskWalDir" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.worker.diskWalDir (get $env "DISK_WAL_DIR") -}}
{{- end }}

{{- define "rag-trace-collector-worker.prometheusMultiprocDir" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.worker.prometheusMultiprocDir (get $env "PROMETHEUS_MULTIPROC_DIR") -}}
{{- end }}

{{- define "rag-trace-collector-worker.kafkaBootstrapServers" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.kafka.bootstrapServers (get $env "KAFKA_BOOTSTRAP_SERVERS") -}}
{{- end }}

{{- define "rag-trace-collector-worker.kafkaTopic" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.kafka.topic (get $env "KAFKA_TOPIC") -}}
{{- end }}

{{- define "rag-trace-collector-worker.kafkaGroupId" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.kafka.groupId (get $env "KAFKA_GROUP_ID") -}}
{{- end }}

{{- define "rag-trace-collector-worker.kafkaAutoOffsetReset" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.kafka.autoOffsetReset (get $env "KAFKA_AUTO_OFFSET_RESET") -}}
{{- end }}

{{- define "rag-trace-collector-worker.kafkaSecurityProtocol" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.kafka.securityProtocol (get $env "KAFKA_SECURITY_PROTOCOL") -}}
{{- end }}

{{- define "rag-trace-collector-worker.kafkaSaslMechanism" -}}
{{- $env := .Values.env | default dict -}}
{{- default .Values.kafka.saslMechanism (get $env "KAFKA_SASL_MECHANISM") -}}
{{- end }}

{{- define "rag-trace-collector-worker.secretEnv" -}}
{{- if .Values.kafka.secretName }}
- name: KAFKA_SASL_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .Values.kafka.secretName | quote }}
      key: username
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.kafka.secretName | quote }}
      key: password
{{- end }}
{{- if .Values.redis.secretName }}
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.redis.secretName | quote }}
      key: password
{{- end }}
{{- if .Values.clickhouse.secretName }}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.clickhouse.secretName | quote }}
      key: password
{{- end }}
{{- end }}

{{- define "rag-trace-collector-worker.env" -}}
{{- $env := .Values.env | default dict -}}
- name: WORKER_PROCESS_COUNT
  value: {{ default .Values.worker.processCount (get $env "WORKER_PROCESS_COUNT") | quote }}
- name: GRACEFUL_DRAIN_TIMEOUT_SECONDS
  value: {{ default .Values.worker.gracefulDrainTimeoutSeconds (get $env "GRACEFUL_DRAIN_TIMEOUT_SECONDS") | quote }}
- name: LOG_LEVEL
  value: {{ default .Values.worker.logLevel (get $env "LOG_LEVEL") | quote }}
- name: TELEMETRY_HOST
  value: {{ default .Values.worker.telemetryHost (get $env "TELEMETRY_HOST") | quote }}
- name: TELEMETRY_PORT
  value: {{ include "rag-trace-collector-worker.telemetryPort" . | quote }}
- name: PROMETHEUS_MULTIPROC_DIR
  value: {{ include "rag-trace-collector-worker.prometheusMultiprocDir" . | quote }}
- name: TIMEWHEEL_EXPIRY_SECONDS
  value: {{ default .Values.worker.timewheelExpirySeconds (get $env "TIMEWHEEL_EXPIRY_SECONDS") | quote }}
- name: TIMEWHEEL_TICK_MS
  value: {{ default .Values.worker.timewheelTickMs (get $env "TIMEWHEEL_TICK_MS") | quote }}
- name: TIMEWHEEL_MAX_OPEN_TRACES
  value: {{ default .Values.worker.timewheelMaxOpenTraces (get $env "TIMEWHEEL_MAX_OPEN_TRACES") | quote }}
- name: BATCH_MAX_ROWS
  value: {{ default .Values.worker.batchMaxRows (get $env "BATCH_MAX_ROWS") | quote }}
- name: BATCH_FLUSH_INTERVAL_SECONDS
  value: {{ default .Values.worker.batchFlushIntervalSeconds (get $env "BATCH_FLUSH_INTERVAL_SECONDS") | quote }}
- name: BATCH_MAX_PENDING_BATCHES
  value: {{ default .Values.worker.batchMaxPendingBatches (get $env "BATCH_MAX_PENDING_BATCHES") | quote }}
- name: DISK_WAL_DIR
  value: {{ include "rag-trace-collector-worker.diskWalDir" . | quote }}
- name: DISK_WAL_MAX_BYTES
  value: {{ default .Values.worker.diskWalMaxBytes (get $env "DISK_WAL_MAX_BYTES") | quote }}
- name: DISK_WAL_DRAIN_INTERVAL_SECONDS
  value: {{ default .Values.worker.diskWalDrainIntervalSeconds (get $env "DISK_WAL_DRAIN_INTERVAL_SECONDS") | quote }}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ include "rag-trace-collector-worker.kafkaBootstrapServers" . | quote }}
- name: KAFKA_TOPIC
  value: {{ include "rag-trace-collector-worker.kafkaTopic" . | quote }}
- name: KAFKA_TOPIC_PARTITIONS
  value: {{ default .Values.kafka.topicPartitions (get $env "KAFKA_TOPIC_PARTITIONS") | quote }}
- name: KAFKA_GROUP_ID
  value: {{ include "rag-trace-collector-worker.kafkaGroupId" . | quote }}
- name: KAFKA_AUTO_OFFSET_RESET
  value: {{ include "rag-trace-collector-worker.kafkaAutoOffsetReset" . | quote }}
- name: KAFKA_SECURITY_PROTOCOL
  value: {{ include "rag-trace-collector-worker.kafkaSecurityProtocol" . | quote }}
{{- $saslMech := include "rag-trace-collector-worker.kafkaSaslMechanism" . }}
{{- if $saslMech }}
- name: KAFKA_SASL_MECHANISM
  value: {{ $saslMech | quote }}
{{- end }}
- name: KAFKA_MAX_POLL_INTERVAL_MS
  value: {{ default .Values.kafka.maxPollIntervalMs (get $env "KAFKA_MAX_POLL_INTERVAL_MS") | quote }}
- name: KAFKA_SESSION_TIMEOUT_MS
  value: {{ default .Values.kafka.sessionTimeoutMs (get $env "KAFKA_SESSION_TIMEOUT_MS") | quote }}
- name: KAFKA_HEARTBEAT_INTERVAL_MS
  value: {{ default .Values.kafka.heartbeatIntervalMs (get $env "KAFKA_HEARTBEAT_INTERVAL_MS") | quote }}
- name: KAFKA_MAX_POLL_RECORDS
  value: {{ default .Values.kafka.maxPollRecords (get $env "KAFKA_MAX_POLL_RECORDS") | quote }}
{{- if .Values.kafka.secretName }}
- name: KAFKA_SASL_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .Values.kafka.secretName | quote }}
      key: username
- name: KAFKA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.kafka.secretName | quote }}
      key: password
{{- end }}
- name: REDIS_CLUSTER_NODES
  value: {{ default .Values.redis.clusterNodes (get $env "REDIS_CLUSTER_NODES") | quote }}
- name: REDIS_TLS_ENABLED
  value: {{ default .Values.redis.tlsEnabled (get $env "REDIS_TLS_ENABLED") | quote }}
- name: REDIS_LRU_CAPACITY
  value: {{ default .Values.redis.lruCapacity (get $env "REDIS_LRU_CAPACITY") | quote }}
- name: REDIS_SOCKET_TIMEOUT_SECONDS
  value: {{ default .Values.redis.socketTimeoutSeconds (get $env "REDIS_SOCKET_TIMEOUT_SECONDS") | quote }}
- name: REDIS_PIPELINE_BATCH_SIZE
  value: {{ default .Values.redis.pipelineBatchSize (get $env "REDIS_PIPELINE_BATCH_SIZE") | quote }}
{{- if .Values.redis.secretName }}
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.redis.secretName | quote }}
      key: password
{{- end }}
- name: CLICKHOUSE_HOST
  value: {{ default .Values.clickhouse.host (get $env "CLICKHOUSE_HOST") | quote }}
- name: CLICKHOUSE_PORT
  value: {{ default .Values.clickhouse.port (get $env "CLICKHOUSE_PORT") | quote }}
- name: CLICKHOUSE_DATABASE
  value: {{ default .Values.clickhouse.database (get $env "CLICKHOUSE_DATABASE") | quote }}
- name: CLICKHOUSE_USER
  value: {{ default .Values.clickhouse.user (get $env "CLICKHOUSE_USER") | quote }}
- name: CLICKHOUSE_TLS_ENABLED
  value: {{ default .Values.clickhouse.tlsEnabled (get $env "CLICKHOUSE_TLS_ENABLED") | quote }}
- name: CLICKHOUSE_INSERT_DEDUPLICATE
  value: {{ default .Values.clickhouse.insertDeduplicate (get $env "CLICKHOUSE_INSERT_DEDUPLICATE") | quote }}
- name: CLICKHOUSE_ASYNC_INSERT
  value: {{ default .Values.clickhouse.asyncInsert (get $env "CLICKHOUSE_ASYNC_INSERT") | quote }}
- name: CLICKHOUSE_REPLICATED
  value: {{ default .Values.clickhouse.replicated (get $env "CLICKHOUSE_REPLICATED") | quote }}
- name: CLICKHOUSE_CLUSTER_NAME
  value: {{ default .Values.clickhouse.clusterName (get $env "CLICKHOUSE_CLUSTER_NAME") | quote }}
{{- if .Values.clickhouse.secretName }}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.clickhouse.secretName | quote }}
      key: password
{{- end }}
{{- end }}
