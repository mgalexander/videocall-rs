{{/*
Expand the name of the chart.
*/}}
{{- define "rustlemania-postgres.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name. Truncated at 63 chars per
Kubernetes DNS naming spec.
*/}}
{{- define "rustlemania-postgres.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Load-bearing: the StatefulSet/pod base name. With the local release name
`postgres` this evaluates to `postgres-postgresql` — which is what
helm/local/up.sh's `kubectl rollout status statefulset/postgres-postgresql`
expects AND what the Service `postgres-postgresql` (see service.yaml)
selects on. Referenced from BOTH statefulset.yaml and service.yaml so the
selector/labels stay in lockstep.
*/}}
{{- define "rustlemania-postgres.statefulsetName" -}}
{{- printf "%s-postgresql" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Chart name + version label value.
*/}}
{{- define "rustlemania-postgres.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels — applied to every resource's metadata.labels.
*/}}
{{- define "rustlemania-postgres.labels" -}}
helm.sh/chart: {{ include "rustlemania-postgres.chart" . }}
{{ include "rustlemania-postgres.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels — MUST be identical between the StatefulSet's pod template
and the Service selector or the Service has no endpoints.
*/}}
{{- define "rustlemania-postgres.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rustlemania-postgres.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
