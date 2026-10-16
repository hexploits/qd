# Operations

## Topology

Each release deploys in order: **(1) the migration Job, then (2) the scheduler and the server
Deployments in parallel.** Scheduler and server pods have a `wait-for-schema` init container and
also wait internally, so nothing touches the schema before the migration finishes, and pods that
start early simply stay not-ready until it does.

- **scheduler**: 2 replicas (no leader election needed), PDB. If all schedulers are down the API
  is unaffected; retention cleanup, deleted-queue reclaim, redrive tasks and per-queue gauges
  pause until one returns (alert `QdSchedulerDown`).
- **server**: ≥ 3 replicas (Helm default) spread across nodes/zones, PodDisruptionBudget,
  rolling updates with `maxUnavailable: 0`. Replicas are stateless and leaderless; scale on CPU
  (HPA template included) or request rate.
- **Postgres**: CloudNativePG with 3 instances and **synchronous replication** (the chart default
  when `postgresql.cnpg.enabled=true`: `synchronous: {method: any, number: 1, dataDurability:
  required}`), or a managed Postgres (RDS/Aurora/Cloud SQL) with Multi-AZ / synchronous standby.
  Postgres 14+ is required (16 recommended).

> With asynchronous replication, a primary failure can lose messages that were already
> acknowledged to producers. Keep synchronous replication unless you explicitly accept that.
> `dataDurability: required` means writes block if no standby is available; `preferred`
> favours availability over durability during standby outages.

## Sizing

- Connections: total = replicas × `QD_DB_MAX_CONNECTIONS` (+1 LISTEN connection per replica).
  Keep below Postgres `max_connections` minus reserves. PgBouncer is only supported in *session*
  mode (LISTEN/NOTIFY and advisory locks need session semantics).
- Long polls do not hold database connections while waiting.
- Storage ≈ messages × (body + attributes + ~150 bytes). Retention is enforced by the janitor;
  the messages table ships with aggressive per-table autovacuum settings.
- Postgres settings that matter: `synchronous_commit = on`, `shared_buffers` ~25% RAM,
  `wal_compression = on`, `max_wal_size` ≥ 4GB, `autovacuum_max_workers` ≥ 4,
  `checkpoint_timeout` 10–15min.

## Health, shutdown and rollouts

- `/healthz` — process alive (liveness). `/readyz` — schema current, database reachable and not
  draining (readiness). Both on the admin port (9325) for the server and scheduler roles.
- On SIGTERM: readiness flips to 503, long polls are released (returning empty), in-flight
  requests drain (bounded by `QD_SHUTDOWN_GRACE_SECS`), then the process exits 0. The chart adds
  a `preStop` sleep so the endpoint is removed before draining; keep
  `terminationGracePeriodSeconds` > preStop + grace.
- Rolling restarts and upgrades are invisible to clients: this is asserted by the resilience
  suite (zero client-visible errors, zero lost messages, FIFO order preserved) — see
  [RESILIENCE.md](RESILIENCE.md).

## Upgrades, rollbacks and schema migrations

- Migrations are applied by the per-release migration Job (`qd migrate`) under a
  Postgres advisory lock; concurrent or retried Jobs are safe (tested). The Job waits for the
  database (up to `QD_MIGRATE_TIMEOUT_SECS`), so it tolerates a Postgres cluster that is still
  bootstrapping or failing over.
- **Compatibility contract:** release N+1's schema must work with release N's binary, and
  release N must start against release N+1's schema (it tolerates migrations it doesn't know).
  In practice: additive (expand) migrations only in a release; remove old columns/tables
  (contract) one release later. This is what makes `helm rollback` / `kubectl rollout undo`
  safe, and it is exercised by the upgrade → rollback → upgrade chaos scenario, which builds the
  previous release from the latest git tag.
- With `migrations.enabled=false` in the chart, run `qd migrate` yourself before
  upgrading; the scheduler and server pods wait for it.

## Backups and disaster recovery

All state is in Postgres: use CNPG's barman/object-store backups (or your provider's PITR).
Restoring restores queues, messages (in-flight ones become visible after their visibility
timeout) and redrive tasks.

## Security

- Run with `QD_AUTH_MODE=sigv4` in shared environments and distribute per-team credentials via
  a Secret mounted as `QD_CREDENTIALS_FILE` (one account id per team = isolated namespaces).
- Terminate TLS at the Ingress/Gateway or service mesh; the server speaks plain HTTP.
- The container runs as non-root on a read-only root filesystem with all capabilities dropped.

## Metrics

Exposed in Prometheus/OpenMetrics format at `/metrics` on the admin port (9325), scraped by the
chart's ServiceMonitor (or `prometheus.io/*` annotations). The same endpoint feeds the
OpenTelemetry Collector (`prometheus` receiver) and the Datadog Agent (OpenMetrics check).
Optional push exporters:

| Variable | Effect |
|---|---|
| `QD_METRICS_PUSHGATEWAY_URL` | Push to a Prometheus Pushgateway every `QD_METRICS_PUSH_INTERVAL_SECS` (15) under `job=QD_METRICS_PUSH_JOB`, `instance=QD_METRICS_INSTANCE` (default `$HOSTNAME`); the group is deleted on shutdown |
| `QD_METRICS_DOGSTATSD_ADDR` | Also stream to a DogStatsD agent (histograms as distributions), prefix `QD_METRICS_DOGSTATSD_PREFIX` |

Wiring guides for each backend: [deploy/observability/README.md](../deploy/observability/README.md).

| Metric | Type | Labels | AWS equivalent |
|---|---|---|---|
| `qd_requests_total` | counter | `action`, `protocol`, `code` | — |
| `qd_request_duration_seconds` | histogram | `action`, `protocol` | — |
| `qd_messages_sent_total` | counter | `queue` | NumberOfMessagesSent |
| `qd_messages_received_total` | counter | `queue` | NumberOfMessagesReceived |
| `qd_messages_deleted_total` | counter | `queue` | NumberOfMessagesDeleted |
| `qd_empty_receives_total` | counter | `queue` | NumberOfEmptyReceives |
| `qd_message_size_bytes` | histogram | `queue` | SentMessageSize |
| `qd_queue_messages` | gauge | `account`, `queue`, `state` (visible/in_flight/delayed) | ApproximateNumberOfMessages{Visible,NotVisible,Delayed} |
| `qd_queue_oldest_message_age_seconds` | gauge | `account`, `queue` | ApproximateAgeOfOldestMessage |
| `qd_queue_info` | gauge (=1) | `account`, `queue`, `type`, `is_dlq` | — |
| `qd_queues`, `qd_move_tasks_running` | gauge | | — |
| `qd_messages_dead_lettered_total` | counter | `queue` (source) | — |
| `qd_messages_deduplicated_total` | counter | `queue` | — |
| `qd_messages_expired_total`, `qd_messages_redriven_total` | counter | | — |
| `qd_queues_created_total`, `qd_queues_deleted_total`, `qd_queues_purged_total` | counter | | — |
| `qd_build_info` | gauge (=1) | `version`, `role` | — |
| `qd_scheduler_heartbeat_age_seconds` | gauge (server) | | — |
| `qd_scheduler_heartbeats_total` | counter (scheduler) | | — |
| `qd_ready` | gauge | | — |
| `qd_long_polls_in_progress` | gauge | | — |
| `qd_db_pool_connections`, `qd_db_pool_max_connections` | gauge | `state` | — |
| `qd_db_errors_total` | counter | `kind` (unavailable/other) | — |
| `qd_notifier_connected` | gauge | | — |
| `qd_notifications_published_total` | counter | | — |
| `qd_janitor_runs_total`, `qd_janitor_duration_seconds` | counter, histogram | `result` | — |
| `qd_queue_metrics_collection_seconds` | histogram | | — |

Per-queue gauges, janitor and redrive metrics come from scheduler pods; request, message and
long-poll metrics from server pods. Every scheduler replica reports the per-queue gauges with
identical values: aggregate with `max by (account, queue, ...)` (the shipped recording rules do).

## Alerts and runbooks

Alert rules (`deploy/observability/prometheus-rules.yaml`, also rendered by the chart as a
PrometheusRule with `prometheusRule.enabled=true`) are unit-tested with `promtool test rules`.
Each alert's `runbook_url` points at the matching section below.

### QdSchedulerDown
No scheduler has written a heartbeat for over 2 minutes (`qd_scheduler_heartbeat_age_seconds`,
reported by the servers). The API is unaffected, but retention cleanup, deleted-queue reclaim,
DLQ redrive tasks and per-queue gauges are paused, and storage will grow. Check the scheduler
Deployment (`kubectl get pods -l app.kubernetes.io/component=scheduler`), its logs and database
connectivity. Work resumes automatically once a scheduler is running.

### QdReplicaDown
Prometheus cannot scrape a replica for 5m. Check `kubectl get pods`, events and logs. Other
replicas keep serving; investigate crash loops (bad config, database credentials).

### QdNotReady
A replica reports not-ready for 2m — usually it cannot reach Postgres. Check
`qd_db_errors_total`, the pod's logs and Postgres connectivity from that node.

### QdAllReplicasNotReady
**Critical: the service is down.** Almost always Postgres is unavailable (failover in progress,
storage full, credentials rotated). Check the CNPG cluster (`kubectl cnpg status`), Postgres
logs and the database Secret.

### QdServerErrorsHigh
More than 1% of requests return `InternalFailure`/`ServiceUnavailable` for 5m. Look at
`qd_requests_total` by `code` and `action`, the logs (errors are logged with action and code),
and database health. SDKs retry these, so clients may only see latency.

### QdServerErrorsCritical
More than 5% server errors: treat like a partial outage. Same investigation as above; check for
a Postgres failover or connection exhaustion (`qd_db_pool_connections`).

### QdHighLatency
p99 of non-ReceiveMessage operations above 500ms for 10m. Usually Postgres is slow (CPU, IO,
locks, autovacuum falling behind on `qd_messages`) or the pool is saturated. Check Postgres
`pg_stat_activity` and the DB pool panels.

### QdVersionSkew
Replicas run different versions for over an hour: a rollout is stuck. Check
`kubectl rollout status` and the new pods' readiness/logs.

### QdDatabaseErrors
Database errors for 5m. `kind="unavailable"` means connection/failover/timeouts;
`kind="other"` points at query failures — check the logs for the SQL error.

### QdNotifierDisconnected
A replica's LISTEN connection to Postgres has been down for 5m. Delivery still works (pollers
re-check every 2s) but long-poll latency across replicas degrades. Usually recovers on its own;
restart the pod if it persists.

### QdDBPoolSaturated
More than 90% of the pool in use for 10m. Raise `QD_DB_MAX_CONNECTIONS` (mind Postgres
`max_connections`) or add replicas, and check for slow queries.

### QdJanitorFailing
Background maintenance (retention, deleted-queue cleanup) keeps failing. Expired messages are
still hidden from consumers, but storage will grow. Check the logs for the failing statement.

### QdQueueMetricsCollectionSlow
Collecting per-queue gauges takes too long — typically very large queues. Increase
`QD_QUEUE_METRICS_INTERVAL_SECS` or set it to 0 to disable per-queue gauges.

### QdDeadLetterQueueNotEmpty
Messages are sitting in a dead-letter queue: consumers failed to process them
`maxReceiveCount` times. Inspect them (receive from the DLQ), fix the consumer, then redrive
with `StartMessageMoveTask` (`aws sqs start-message-move-task --source-arn <dlq-arn>`).

### QdOldestMessageTooOld
The oldest message in a queue is older than the threshold (default 1h): consumers are down,
too slow or stuck. Check consumer deployments and `qd_messages_received_total` for the queue.

### QdBacklogGrowing
A queue's backlog has grown steadily for 30m. Producers outpace consumers; scale consumers or
investigate slow processing.
