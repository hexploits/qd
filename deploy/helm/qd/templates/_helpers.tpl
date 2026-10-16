{{/* vim: set filetype=mustache: */}}

{{- define "qd.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "qd.fullname" -}}
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

{{- define "qd.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "qd.selectorLabels" -}}
app.kubernetes.io/name: {{ include "qd.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "qd.labels" -}}
helm.sh/chart: {{ include "qd.chart" . }}
{{ include "qd.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: qd
{{- end }}

{{- define "qd.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "qd.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "qd.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}
{{- end }}

{{/* CloudNativePG cluster name */}}
{{- define "qd.cnpgName" -}}
{{- default (printf "%s-pg" (include "qd.fullname" .)) .Values.postgresql.cnpg.name | trunc 50 | trimSuffix "-" }}
{{- end }}

{{/* Secret name + key holding QD_DATABASE_URL, following the documented precedence. */}}
{{- define "qd.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else if .Values.postgresql.cnpg.enabled }}
{{- printf "%s-app" (include "qd.cnpgName" .) }}
{{- else }}
{{- /* database.url -> chart-created Secret; nothing set -> user must create this Secret (see NOTES) */}}
{{- printf "%s-db" (include "qd.fullname" .) }}
{{- end }}
{{- end }}

{{- define "qd.databaseSecretKey" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecretKey }}
{{- else if .Values.postgresql.cnpg.enabled }}
{{- "uri" }}
{{- else }}
{{- "database-url" }}
{{- end }}
{{- end }}

{{/* Whether the chart itself must create the DB Secret. */}}
{{- define "qd.createDatabaseSecret" -}}
{{- if and (not .Values.database.existingSecret) (not .Values.postgresql.cnpg.enabled) .Values.database.url }}true{{- end }}
{{- end }}

{{/* Credentials file Secret (empty string when no credentials are configured). */}}
{{- define "qd.credentialsSecretName" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecret }}
{{- else if .Values.auth.credentials }}
{{- printf "%s-credentials" (include "qd.fullname" .) }}
{{- end }}
{{- end }}

{{- define "qd.credentialsSecretKey" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecretKey }}
{{- else }}
{{- "credentials" }}
{{- end }}
{{- end }}

{{/*
Per-role names and labels.

The server pods keep the original `app.kubernetes.io/name: <name>` + `instance` selector (a
Deployment selector is immutable, and the Service/PDB/HPA/NetworkPolicy of existing installations
select on it). The scheduler and migration pods therefore use a DIFFERENT
`app.kubernetes.io/name` (`<name>-scheduler`, `<name>-migrate`) so that they never match the
server's selector: otherwise the SQS Service would route to schedulers / migration pods and the
server PDB/HPA would count them. Every pod also carries `app.kubernetes.io/component`
(server | scheduler | migrate).
Usage: include "qd.roleSelectorLabels" (dict "root" $ "role" "scheduler")
*/}}
{{- define "qd.roleName" -}}
{{- printf "%s-%s" (include "qd.name" .root) .role | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "qd.roleFullname" -}}
{{- printf "%s-%s" (include "qd.fullname" .root) .role | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "qd.roleSelectorLabels" -}}
app.kubernetes.io/name: {{ include "qd.roleName" . }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .role }}
{{- end }}

{{- define "qd.roleLabels" -}}
helm.sh/chart: {{ include "qd.chart" .root }}
{{ include "qd.roleSelectorLabels" . }}
app.kubernetes.io/version: {{ .root.Values.image.tag | default .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/part-of: qd
{{- end }}

{{/* Pod annotations shared by the long-running roles (server, scheduler). */}}
{{- define "qd.podAnnotations" -}}
checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
checksum/secret: {{ include (print $.Template.BasePath "/secret.yaml") . | sha256sum }}
{{- if .Values.metrics.podAnnotations }}
prometheus.io/scrape: "true"
prometheus.io/port: {{ .Values.containerPorts.admin | quote }}
prometheus.io/path: /metrics
{{- end }}
{{- with .Values.metrics.datadogAutodiscovery }}
{{- if .enabled }}
{{- $instance := dict "openmetrics_endpoint" (printf "http://%%%%host%%%%:%v/metrics" $.Values.containerPorts.admin) "namespace" .namespace "metrics" .metrics }}
{{- with .extraInstanceConfig }}{{- $instance = merge $instance . }}{{- end }}
ad.datadoghq.com/qd.checks: {{ dict "openmetrics" (dict "instances" (list $instance)) | toJson | quote }}
{{- end }}
{{- end }}
{{- with .Values.podAnnotations }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/* envFrom shared by every container (ConfigMap + extraEnvFrom). */}}
{{- define "qd.envFrom" -}}
- configMapRef:
    name: {{ include "qd.fullname" . }}
{{- with .Values.extraEnvFrom }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/* QD_DATABASE_URL from the Secret selected by the documented precedence. */}}
{{- define "qd.databaseEnv" -}}
- name: QD_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "qd.databaseSecretName" . }}
      key: {{ include "qd.databaseSecretKey" . }}
{{- end }}

{{/* DogStatsD exporter env (long-running roles only). */}}
{{- define "qd.dogstatsdEnv" -}}
{{- if .Values.metrics.dogstatsd.enabled }}
{{- if .Values.metrics.dogstatsd.host }}
- name: QD_METRICS_DOGSTATSD_ADDR
  value: {{ printf "%s:%v" .Values.metrics.dogstatsd.host .Values.metrics.dogstatsd.port | quote }}
{{- else }}
# The Datadog Agent DaemonSet listens for DogStatsD on each node (hostPort).
- name: DD_AGENT_HOST
  valueFrom:
    fieldRef:
      fieldPath: status.hostIP
- name: QD_METRICS_DOGSTATSD_ADDR
  value: {{ printf "$(DD_AGENT_HOST):%v" .Values.metrics.dogstatsd.port | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/* Volume mounts every container gets (/tmp + extraVolumeMounts, e.g. a DB CA bundle). */}}
{{- define "qd.commonVolumeMounts" -}}
- name: tmp
  mountPath: /tmp
{{- with .Values.extraVolumeMounts }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
A short-lived helper container running one of the binary's wait/migrate subcommands with the
same image, DB connection and hardening as the main container. The roles that don't serve the
SQS API run with QD_AUTH_MODE=none so they never need the SigV4 credentials Secret.
Usage: include "qd.utilityContainer" (dict "root" $ "name" "wait-for-schema" "args" (list "wait-for-schema") "resources" $res ["env" (list EnvVar...)])
*/}}
{{- define "qd.utilityContainer" -}}
{{- $root := .root -}}
- name: {{ .name }}
  image: {{ include "qd.image" $root | quote }}
  imagePullPolicy: {{ $root.Values.image.pullPolicy }}
  args:
    {{- toYaml .args | nindent 4 }}
  securityContext:
    {{- toYaml $root.Values.securityContext | nindent 4 }}
  envFrom:
    {{- include "qd.envFrom" $root | nindent 4 }}
  env:
    {{- include "qd.databaseEnv" $root | nindent 4 }}
    - name: QD_AUTH_MODE
      value: "none"
    {{- with .env }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
    {{- with $root.Values.extraEnv }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  resources:
    {{- toYaml .resources | nindent 4 }}
  volumeMounts:
    {{- include "qd.commonVolumeMounts" $root | nindent 4 }}
{{- end }}
