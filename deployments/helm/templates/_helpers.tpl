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

{{- define "rag-trace-collector-worker.env" -}}
- name: WORKER_PROCESS_COUNT
  value: {{ .Values.worker.processCount | quote }}
- name: GRACEFUL_DRAIN_TIMEOUT_SECONDS
  value: {{ .Values.worker.gracefulDrainTimeoutSeconds | quote }}
- name: LOG_LEVEL
  value: {{ .Values.worker.logLevel | quote }}
- name: TELEMETRY_HOST
  value: {{ .Values.worker.telemetryHost | quote }}
- name: TELEMETRY_PORT
  value: {{ .Values.worker.telemetryPort | quote }}
- name: PROMETHEUS_MULTIPROC_DIR
  value: {{ .Values.worker.prometheusMultiprocDir | quote }}
- name: TIMEWHEEL_EXPIRY_SECONDS
  value: {{ .Values.worker.timewheelExpirySeconds | quote }}
- name: TIMEWHEEL_TICK_MS
  value: {{ .Values.worker.timewheelTickMs | quote }}
- name: TIMEWHEEL_MAX_OPEN_TRACES
  value: {{ .Values.worker.timewheelMaxOpenTraces | quote }}
- name: BATCH_MAX_ROWS
  value: {{ .Values.worker.batchMaxRows | quote }}
- name: BATCH_FLUSH_INTERVAL_SECONDS
  value: {{ .Values.worker.batchFlushIntervalSeconds | quote }}
- name: BATCH_MAX_PENDING_BATCHES
  value: {{ .Values.worker.batchMaxPendingBatches | quote }}
- name: DISK_WAL_DIR
  value: {{ .Values.worker.diskWalDir | quote }}
- name: DISK_WAL_MAX_BYTES
  value: {{ .Values.worker.diskWalMaxBytes | quote }}
- name: DISK_WAL_DRAIN_INTERVAL_SECONDS
  value: {{ .Values.worker.diskWalDrainIntervalSeconds | quote }}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ .Values.kafka.bootstrapServers | quote }}
- name: KAFKA_TOPIC
  value: {{ .Values.kafka.topic | quote }}
- name: KAFKA_TOPIC_PARTITIONS
  value: {{ .Values.kafka.topicPartitions | quote }}
- name: KAFKA_GROUP_ID
  value: {{ .Values.kafka.groupId | quote }}
- name: KAFKA_AUTO_OFFSET_RESET
  value: {{ .Values.kafka.autoOffsetReset | quote }}
- name: KAFKA_SECURITY_PROTOCOL
  value: {{ .Values.kafka.securityProtocol | quote }}
{{- if .Values.kafka.saslMechanism }}
- name: KAFKA_SASL_MECHANISM
  value: {{ .Values.kafka.saslMechanism | quote }}
{{- end }}
- name: KAFKA_MAX_POLL_INTERVAL_MS
  value: {{ .Values.kafka.maxPollIntervalMs | quote }}
- name: KAFKA_SESSION_TIMEOUT_MS
  value: {{ .Values.kafka.sessionTimeoutMs | quote }}
- name: KAFKA_HEARTBEAT_INTERVAL_MS
  value: {{ .Values.kafka.heartbeatIntervalMs | quote }}
- name: KAFKA_MAX_POLL_RECORDS
  value: {{ .Values.kafka.maxPollRecords | quote }}
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
  value: {{ .Values.redis.clusterNodes | quote }}
- name: REDIS_TLS_ENABLED
  value: {{ .Values.redis.tlsEnabled | quote }}
- name: REDIS_LRU_CAPACITY
  value: {{ .Values.redis.lruCapacity | quote }}
- name: REDIS_SOCKET_TIMEOUT_SECONDS
  value: {{ .Values.redis.socketTimeoutSeconds | quote }}
- name: REDIS_PIPELINE_BATCH_SIZE
  value: {{ .Values.redis.pipelineBatchSize | quote }}
{{- if .Values.redis.secretName }}
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.redis.secretName | quote }}
      key: password
{{- end }}
- name: CLICKHOUSE_HOST
  value: {{ .Values.clickhouse.host | quote }}
- name: CLICKHOUSE_PORT
  value: {{ .Values.clickhouse.port | quote }}
- name: CLICKHOUSE_DATABASE
  value: {{ .Values.clickhouse.database | quote }}
- name: CLICKHOUSE_USER
  value: {{ .Values.clickhouse.user | quote }}
- name: CLICKHOUSE_TLS_ENABLED
  value: {{ .Values.clickhouse.tlsEnabled | quote }}
- name: CLICKHOUSE_INSERT_DEDUPLICATE
  value: {{ .Values.clickhouse.insertDeduplicate | quote }}
- name: CLICKHOUSE_ASYNC_INSERT
  value: {{ .Values.clickhouse.asyncInsert | quote }}
- name: CLICKHOUSE_REPLICATED
  value: {{ .Values.clickhouse.replicated | quote }}
- name: CLICKHOUSE_CLUSTER_NAME
  value: {{ .Values.clickhouse.clusterName | quote }}
{{- if .Values.clickhouse.secretName }}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.clickhouse.secretName | quote }}
      key: password
{{- end }}
{{- end }}
