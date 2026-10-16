# qd observability

Everything here works off the Prometheus text endpoint every long-running process serves on
the **admin port** (`9325` by default): `GET /metrics` (next to `GET /healthz`, liveness, and
`GET /readyz`, readiness). The optional push exporters (Pushgateway, DogStatsD) send the same
metrics.

| File | What |
|---|---|
| `prometheus-rules.yaml` | Recording rules + alerts (plain Prometheus rule file). **Single source of truth.** |
| `tests/*.test.yaml` | `promtool test rules` unit tests: each alert has a firing and a not-firing case. |
| `grafana-dashboard.json` | Grafana dashboard (Overview / Queues / Runtime rows); datasource, namespace, job, account and queue are template variables. |
| `sync-chart-files.sh` | Copies the rules and dashboard into `deploy/helm/qd/files/` (the chart reads them with `.Files.Get`). |
| `check.sh` | `promtool check rules`, `promtool test rules`, checks that every alert has tests, checks that the chart copies are byte-identical, runs the same unit tests against the Helm-rendered `PrometheusRule`, checks that threshold overrides are applied, and checks the dashboard JSON. Needs `promtool`, `helm` and `python3`. CI job: `observability`. |
| `../../e2e/observability/check-metrics.sh` | Starts a real server, sends traffic with the AWS CLI and checks that every metric used by the rules and the dashboard is really exported. Also runs `promtool check metrics`. CI job: `e2e-observability`. |

**Editing rules or the dashboard:** edit the files in this directory, run `./sync-chart-files.sh`,
then run `./check.sh`. Never edit the copies under the chart's `files/`.

## Metric conventions you need to know

- **Roles.** In production the binary runs as `qd server` (SQS API) and
  `qd scheduler` (background work); `qd_build_info{version, role}` says which. Each role
  exports only its own metrics:

  | Role | Metrics |
  |---|---|
  | `server` | `qd_requests_total`, `qd_request_duration_seconds`, message counters (`qd_messages_sent/received/deleted/dead_lettered/deduplicated_total`, ...), `qd_notifier_connected`, `qd_long_polls_in_progress`, `qd_scheduler_heartbeat_age_seconds` |
  | `scheduler` | queue gauges (`qd_queue_messages`, `qd_queue_oldest_message_age_seconds`, `qd_queue_info`, `qd_queues`, `qd_move_tasks_running`), `qd_queue_metrics_collection_seconds`, `qd_janitor_runs_total`, `qd_janitor_duration_seconds`, `qd_messages_expired_total`, `qd_messages_redriven_total`, `qd_scheduler_heartbeats_total` |
  | both | `qd_build_info`, `qd_ready`, `qd_db_pool_*`, `qd_db_errors_total` |

  The all-in-one mode (`qd` / `qd all`, used by docker-compose and the
  tests) exports everything with `role="all"`. With the Helm chart's ServiceMonitor the roles are
  two scrape jobs: `<fullname>` (server Service) and `<fullname>-scheduler` (scheduler Service).
- **Queue gauges** (`qd_queue_messages`, `qd_queue_oldest_message_age_seconds`, `qd_queue_info`,
  `qd_queues`, `qd_move_tasks_running`) are computed from the database by the scheduler pods.
  **Every scheduler replica reports the same values**, so aggregate them with
  `max by (account, queue, ...)` and never with `sum`. They refresh every
  `QD_QUEUE_METRICS_INTERVAL_SECS` (30s by default) and stop updating when no scheduler runs.
- **Message counters** (`qd_messages_*_total`) and request metrics are per replica, so `sum` them.
  `qd_messages_dead_lettered_total{queue}` is labelled with the **source** queue.
- `qd_request_duration_seconds` for `ReceiveMessage` includes long polls (up to 20s). Leave it out
  of latency SLOs and alerts.
- A counter only appears after its first increment. For example, `qd_db_errors_total` stays
  absent until the first database error.
- The recording rules (`qd:*`) keep `namespace` and `job`, so several installations scraped by one
  Prometheus stay separate.

## Prometheus Operator (kube-prometheus-stack)

The chart renders one `ServiceMonitor` that scrapes the admin port (`/metrics`, every 30s) of both
the server Service (`<fullname>`) and the scheduler Service (`<fullname>-scheduler`), and a
`PrometheusRule` built from the same rule file:

```yaml
# values.yaml
metrics:
  serviceMonitor:
    enabled: true
    labels: {release: kube-prometheus-stack}   # must match Prometheus' serviceMonitorSelector
prometheusRule:
  enabled: true
  labels: {release: kube-prometheus-stack}     # must match Prometheus' ruleSelector
  # jobMatcher: ""            # default: "<fullname>(-scheduler)?" (both ServiceMonitor jobs)
  # runbookBaseUrl: https://wiki.example.com/qd
  thresholds:
    oldestMessageAgeSeconds: 3600
    backlogMessages: 1000
    # ... see values.yaml for all of them
  disabledAlerts: []          # e.g. [QdVersionSkew]
  severityOverrides: {}       # e.g. {QdDeadLetterQueueNotEmpty: info}
  additionalAlertLabels: {}   # e.g. {team: platform}
grafanaDashboard:
  enabled: true               # ConfigMap labelled grafana_dashboard: "1" for the Grafana sidecar
```

The operator adds `namespace`, `pod`, `service` and `job` (the Service name) to every series.

## Plain Prometheus

Scrape the admin port, and load the rule file as it is:

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/rules/qd.yaml      # = deploy/observability/prometheus-rules.yaml

scrape_configs:
  # Kubernetes, with pod annotations (Helm: metrics.podAnnotations=true)
  - job_name: qd
    scrape_interval: 30s
    kubernetes_sd_configs: [{role: pod}]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: "true"
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
        action: keep
        regex: qd(-scheduler)?   # server and scheduler pods
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
        regex: '([^:]+)(?::\d+)?;(\d+)'
        replacement: '$1:$2'
        target_label: __address__
      - source_labels: [__meta_kubernetes_namespace]
        target_label: namespace
      - source_labels: [__meta_kubernetes_pod_name]
        target_label: pod

  # VMs / docker-compose (scrape the server AND the scheduler admin ports)
  # - job_name: qd
  #   static_configs: [{targets: ["qd-1:9325", "qd-2:9325", "qd-scheduler-1:9325"]}]
```

Here server and scheduler share one job; the rules and the dashboard still work because they
tell the roles apart by metric (and `qd_build_info{role}`), not by job.

The `up`-based alerts match `job=~".*qd.*"`. If your job name does not contain
`qd`, edit the two `up{...}` selectors in your copy of the file.

## Grafana

- **Sidecar** (kube-prometheus-stack, or the grafana chart with `sidecar.dashboards.enabled=true`):
  set `grafanaDashboard.enabled=true`. This creates a ConfigMap labelled `grafana_dashboard: "1"`
  (`grafanaDashboard.sidecarLabel` and `sidecarLabelValue` change the label). Put the ConfigMap
  in a namespace the sidecar watches (`grafanaDashboard.namespace`). To choose a folder, set
  `grafanaDashboard.folder` (this needs `sidecar.dashboards.folderAnnotation: grafana_folder`).
- **Manual**: go to Dashboards → New → Import and upload `grafana-dashboard.json`, then pick a
  Prometheus data source. You can also use file provisioning and point a dashboard provider at
  the file.

All queries use the `${datasource}` variable. The namespace and job variables use `.*` for "All",
so the dashboard also works when the `namespace` label doesn't exist (plain Prometheus). The job
variable lists installations: a `<job>-scheduler` job is folded into `<job>` and every query
matches `job=~"$job(-scheduler)?"`, so picking one installation shows its server AND scheduler
metrics. The **Scheduler** row shows the heartbeat age seen by the servers, ready scheduler
replicas, heartbeats written, janitor runs and duration, queue-gauge collection time, running
message move tasks and expiry/redrive throughput.

## OpenTelemetry Collector

Scrape the admin port with the `prometheus` receiver and export over OTLP:

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: qd
          scrape_interval: 30s
          kubernetes_sd_configs: [{role: pod}]
          relabel_configs:
            - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
              action: keep
              regex: qd(-scheduler)?
            - source_labels: [__meta_kubernetes_pod_container_port_name]
              action: keep
              regex: admin
            - source_labels: [__meta_kubernetes_namespace]
              target_label: namespace
processors:
  batch: {}
exporters:
  otlp:
    endpoint: otel-backend.example.com:4317
service:
  pipelines:
    metrics:
      receivers: [prometheus]
      processors: [batch]
      exporters: [otlp]
```

The collector needs RBAC to list pods. The Prometheus histograms become OTLP explicit-bucket
histograms. If the backend is Prometheus-compatible (e.g. Mimir with OTLP ingestion), the rules
and dashboard keep working, as long as the backend keeps the metric names unchanged and does not
add unit suffixes.

## Datadog

Use only one of these two options, otherwise you collect the metrics twice:

1. **OpenMetrics check (Autodiscovery)**: set `metrics.datadogAutodiscovery.enabled=true`. This
   adds the following annotation to the pods:
   `ad.datadoghq.com/qd.checks: {"openmetrics":{"instances":[{"openmetrics_endpoint":"http://%%host%%:9325/metrics","namespace":"qd","metrics":["qd_.*"]}]}}`.
   Datadog removes the `_total` suffix from counters and reports them as `.count` monotonic
   counts, e.g. `qd.qd_requests.count`.
2. **DogStatsD push**: set `metrics.dogstatsd.enabled=true`. The chart sets
   `QD_METRICS_DOGSTATSD_ADDR=$(DD_AGENT_HOST):8125`, where `DD_AGENT_HOST` is the node IP
   (`status.hostIP`), i.e. the Agent DaemonSet's DogStatsD hostPort. To send to an agent Service
   instead, set `metrics.dogstatsd.host`. `metrics.dogstatsd.prefix` sets
   `QD_METRICS_DOGSTATSD_PREFIX`. Histograms are sent as distributions.

The Prometheus rules and the Grafana dashboard don't apply to Datadog. Rebuild the important
alerts from the table below as Datadog monitors.

## Pushgateway

Use this only where Prometheus cannot reach the pods. Set
`metrics.pushgateway.url=http://pushgateway.monitoring:9091`, which sets
`QD_METRICS_PUSHGATEWAY_URL`. `QD_METRICS_PUSH_INTERVAL_SECS` defaults to 15,
`QD_METRICS_PUSH_JOB` to `qd`, and `QD_METRICS_INSTANCE` to the pod name. Scrape the
gateway with `honor_labels: true`. Pushed series are not removed when a pod goes away, so:

- the `up`-based alerts (`QdReplicaDown`, and the second half of
  `QdAllReplicasNotReady`) do not apply. `QdSchedulerDown` still works (the
  servers report the heartbeat age), but only while server pods keep pushing. Alert on staleness instead:
  `time() - push_time_seconds{job="qd"} > 120`;
- delete groups for replicas that no longer exist (`DELETE /metrics/job/qd/instance/<pod>`),
  or `qd_build_info` will report version skew forever.

## Alerts and runbook

`runbook_url` points to `docs/OPERATIONS.md#<alertname-lowercased>`. Use
`prometheusRule.runbookBaseUrl` to change the base URL. The thresholds below are the defaults
and you can change them in Helm.

| Alert | Severity | Fires when | What to do |
|---|---|---|---|
| `QdReplicaDown` | warning | `up == 0` for 5m (server or scheduler pod) | `kubectl get pods`, `describe` and `logs --previous`. Look for crash loops (bad config, DB URL) or an unreachable admin port (NetworkPolicy `adminFrom`). |
| `QdNotReady` | warning | `qd_ready == 0` for 2m | The replica can't ping Postgres or is stuck draining. Check its logs for DB errors, check the DB's reachability from that node, and check whether the pool is exhausted (`QdDBPoolSaturated`). |
| `QdAllReplicasNotReady` | **critical** | no replica of a job (server or scheduler) ready or scrapeable for 2m | Server job: full outage. Scheduler job: see `QdSchedulerDown`. Check Postgres/CNPG first (`kubectl get cluster`, primary pod, failover in progress, disk full, `max_connections`). Then check recent deploys and roll back if needed. |
| `QdServerErrorsHigh` | warning | 5xx ratio > 1% for 5m | Check `qd_db_errors_total` by kind. `unavailable` usually means Postgres failover, restarts or pool timeouts. `other` means a bug or schema problem, so check the logs for `database error`. |
| `QdServerErrorsCritical` | **critical** | 5xx ratio > 5% for 5m | Same as above, but urgent. If it started with a rollout, roll back. |
| `QdHighLatency` | warning | p99 of any non-ReceiveMessage action > 500ms for 10m | Postgres is slow: check CPU, IO, locks, autovacuum, bloat on `qd_messages`, and long transactions (`pg_stat_activity`). Also check pool saturation and the synchronous standby's health, since commits wait for it. |
| `QdVersionSkew` | info | >1 `qd_build_info` version per role for 1h | A rollout is stuck. Run `kubectl rollout status deploy/<name>` (server) or `deploy/<name>-scheduler` and look for new pods failing readiness or stuck in the `wait-for-schema` init container (did the migrate Job finish?), an unschedulable surge pod, or a PDB blocking the rollout. |
| `QdSchedulerDown` | **critical** | freshest scheduler heartbeat (seen by the servers) > 120s old, or none at all, for 5m | The API keeps serving, but retention cleanup, deleted-queue reclaim, DLQ redrive / message-move tasks and the per-queue gauges have stopped (queue alerts go silent, tables bloat). `kubectl get deploy,pods -l app.kubernetes.io/name=qd-scheduler`: replicas 0, crash loops (`logs --previous`), pods stuck in `wait-for-schema` (migrate Job failed: `kubectl get jobs`, `logs job/<name>-migrate-<rev>`), unschedulable or not ready (DB). Any number of schedulers may run, so scaling up or `rollout restart deploy/<name>-scheduler` is always safe. If the schedulers look healthy, check their `qd_db_errors_total` and logs for failed heartbeat writes. |
| `QdDatabaseErrors` | warning | `qd_db_errors_total` increasing for 5m | Errors keep happening (not one blip). Read the logs. `unavailable` points at connectivity, failover or `acquireTimeoutSecs`. `other` points at statement timeouts or a bug. |
| `QdNotifierDisconnected` | warning | `qd_notifier_connected == 0` for 5m | The LISTEN connection is down, so long polls only see messages sent through other replicas when they re-check, and receive latency goes up. Check whether a pooler (PgBouncer in transaction mode can't LISTEN) or `idle_session_timeout` is killing the connection. Restarting the pod reconnects it. |
| `QdDBPoolSaturated` | warning | pool in use > 90% for 10m | Raise `config.db.maxConnections`, keeping replicas × pool below Postgres `max_connections`, or add replicas. Also look for slow queries that hold connections. |
| `QdJanitorFailing` | warning | > 2 failed janitor passes in 15m on a scheduler replica | Look for `janitor pass failed` in the logs. Until it's fixed, expired messages and dedup and receipt rows pile up, and table bloat grows. |
| `QdQueueMetricsCollectionSlow` | warning | p99 of the queue-gauge collection > 20s for 15m | The collection query scans `qd_messages`. Check bloat and vacuum, raise `config.queueMetricsIntervalSecs`, or set it to 0 to disable per-queue gauges on very large backlogs. |
| `QdDeadLetterQueueNotEmpty` | warning | a DLQ has visible messages for 5m | Consumers of the source queue are failing. Inspect a few DLQ messages, fix the consumer, then redrive with `aws sqs start-message-move-task --source-arn <dlq-arn>`. |
| `QdOldestMessageTooOld` | warning | oldest message (non-DLQ) > 1h for 15m | Consumers are stalled or too slow. Check consumer health, scale them up, and check visibility timeouts. The messages are close to their retention limit. |
| `QdBacklogGrowing` | warning | depth > 1000 and rising over 30m, for 15m | Producers are outpacing consumers. Scale the consumers up and check the receive error rate and empty receives. |
