# qd

*qd* (pronounced "queued") is a highly-available, **AWS SQS-compatible** message queue written
in Rust, with messages durably stored in **PostgreSQL**. Point any AWS SDK, the AWS CLI, or an
SQS library at it by changing the endpoint URL - nothing else.

- **Drop-in SQS API** - both wire protocols (AWS JSON 1.0 used by current SDKs, and the legacy
  Query/XML protocol used by older SDKs), the full set of 23 SQS actions, AWS error codes, MD5
  checksums, receipt handles, and SigV4 authentication.
- **Durable** - every message is committed to Postgres before it is acknowledged; nothing lives
  only in memory. Restarts, crashes and rolling deploys lose nothing.
- **Highly available** - server replicas are stateless and coordinate only through Postgres, so
  you run as many as you want behind a Kubernetes Service. Pair with an HA Postgres
  (CloudNativePG recommended).
- **Full feature set** - standard and FIFO queues, visibility timeouts, delays, long polling,
  dead-letter queues with redrive policies and redrive-allow policies, DLQ redrive (message move
  tasks), deduplication, message groups, retention, tags, purge, batch APIs.
- **Kubernetes-native** - distroless image, Helm chart (PDB, HPA, topology spread, probes,
  ServiceMonitor, CloudNativePG integration), JSON logs, Prometheus metrics, graceful drain.

> Why not ElasticMQ? ElasticMQ keeps queues in memory on a single node - fine for tests, not for
> production. qd keeps the same "just change the endpoint" ergonomics, adds durable
> storage and horizontal HA, and is validated against the official AWS SDKs.

## Quick start

```bash
docker compose up -d          # postgres + qd on :9324 (API) and :9325 (admin)

export AWS_ACCESS_KEY_ID=x AWS_SECRET_ACCESS_KEY=y AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:9324 sqs create-queue --queue-name orders
aws --endpoint-url http://localhost:9324 sqs send-message \
    --queue-url http://localhost:9324/000000000000/orders --message-body hello
aws --endpoint-url http://localhost:9324 sqs receive-message \
    --queue-url http://localhost:9324/000000000000/orders
```

Or run the binary directly against any Postgres 14+:

```bash
cargo build --release
QD_DATABASE_URL=postgres://user:pass@localhost/qd ./target/release/qd
```

Migrations run automatically at startup in this all-in-one mode.

### Process roles

One binary, selected by subcommand. Production runs them as separate workloads:

| Command | What it does |
|---|---|
| `qd migrate` | Waits for the database (up to `QD_MIGRATE_TIMEOUT_SECS`), applies migrations, exits. Run once per release, before anything else. |
| `qd scheduler` | Background maintenance: retention cleanup, deleted-queue reclaim, DLQ redrive tasks, per-queue metrics, heartbeat. Safe to run with several replicas. |
| `qd server` | The SQS API. |
| `qd wait-for-schema` | Exits once every migration of this build is applied (init containers). |
| `qd` (or `all`) | Everything in one process, migrating at startup (development, docker-compose). |

Startup order: **migrations first, then the scheduler and the server in parallel.** `server` and
`scheduler` never migrate: they expose `/healthz` immediately, report not-ready on `/readyz`, and
wait until the schema is current before doing any work, so nothing touches the database schema
until the migration has finished.

## Pointing clients at qd

Only the endpoint changes. Queue URLs returned by the server look like
`http://<host>:9324/<account-id>/<queue-name>`.

| Client | Configuration |
|---|---|
| AWS CLI | `aws --endpoint-url http://qd:9324 sqs ...` or `AWS_ENDPOINT_URL_SQS=http://qd:9324` |
| Any SDK honoring env config | `AWS_ENDPOINT_URL_SQS=http://qd:9324` (SDKs released since 2024 support service-specific endpoint env vars) |
| Python (boto3) | `boto3.client("sqs", endpoint_url="http://qd:9324")` |
| JavaScript (v3) | `new SQSClient({ endpoint: "http://qd:9324" })` |
| Java (v2) | `SqsClient.builder().endpointOverride(URI.create("http://qd:9324"))` |
| Go (v2) | `sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://qd:9324") })` |
| Rust | `aws_sdk_sqs::config::Builder::from(&cfg).endpoint_url("http://qd:9324")` |
| .NET | `new AmazonSQSClient(new AmazonSQSConfig { ServiceURL = "http://qd:9324" })` |

Credentials: with the default `QD_AUTH_MODE=none`, any credentials (even dummy ones) are accepted.
With `QD_AUTH_MODE=sigv4`, issue clients an access key/secret from `QD_CREDENTIALS` and requests
are verified exactly like AWS does (see [Authentication](#authentication)).

## Configuration

Every option is a flag or an `QD_*` environment variable (`qd --help`).

| Variable | Default | Description |
|---|---|---|
| `QD_DATABASE_URL` | - (required) | Postgres connection string |
| `QD_BIND` | `0.0.0.0:9324` | SQS API listener |
| `QD_ADMIN_BIND` | `0.0.0.0:9325` | `/healthz` (liveness), `/readyz`, `/metrics` |
| `QD_REGION` | `us-east-1` | Region used in queue ARNs |
| `QD_DEFAULT_ACCOUNT_ID` | `000000000000` | Account for unauthenticated callers |
| `QD_PUBLIC_URL` | derived from `Host` | Base URL advertised in queue URLs |
| `QD_AUTH_MODE` | `none` | `none` or `sigv4` |
| `QD_CREDENTIALS` | - | `AKID:SECRET[:ACCOUNT],...` |
| `QD_CREDENTIALS_FILE` | - | One `AKID:SECRET[:ACCOUNT]` per line (mount a Secret) |
| `QD_ALLOW_CROSS_ACCOUNT` | `false` | Allow callers to address other accounts' queues |
| `QD_DB_MAX_CONNECTIONS` | `32` | Pool size per replica |
| `QD_DB_MIN_CONNECTIONS` | `2` | Idle connections kept per replica |
| `QD_DB_ACQUIRE_TIMEOUT_SECS` | `5` | Wait for a pooled connection |
| `QD_DB_STATEMENT_TIMEOUT_MS` | `30000` | Server-side statement timeout |
| `QD_RUN_MIGRATIONS` | `true` | All-in-one mode only: apply migrations at startup |
| `QD_MIGRATE_TIMEOUT_SECS` | `600` | How long `migrate` waits for the database |
| `QD_QUEUE_CACHE_TTL_MS` | `1000` | Max staleness of queue metadata across replicas |
| `QD_PURGE_COOLDOWN_SECS` | `60` | Min interval between `PurgeQueue` calls (AWS: 60) |
| `QD_JANITOR_INTERVAL_SECS` | `10` | Retention / cleanup interval |
| `QD_QUEUE_METRICS_INTERVAL_SECS` | `30` | Per-queue depth gauges (0 disables) |
| `QD_SHUTDOWN_GRACE_SECS` | `25` | Drain budget after SIGTERM |
| `QD_MAX_BODY_BYTES` | `8388608` | Max HTTP request size |
| `QD_LOG_FORMAT` | `text` (`json` in the image) | Log format |
| `QD_LOG` / `RUST_LOG` | `info,sqlx=warn` | Log filter |
| `QD_METRICS_PUSHGATEWAY_URL` | - | Also push metrics to a Prometheus Pushgateway |
| `QD_METRICS_PUSH_INTERVAL_SECS` / `QD_METRICS_PUSH_JOB` / `QD_METRICS_INSTANCE` | `15` / `qd` / `$HOSTNAME` | Pushgateway settings |
| `QD_METRICS_DOGSTATSD_ADDR` / `QD_METRICS_DOGSTATSD_PREFIX` | - | Also stream metrics to a DogStatsD agent |

## Authentication

- `none` (default): signatures are not checked. If the request's access key id appears in
  `QD_CREDENTIALS`, its account is used; otherwise `QD_DEFAULT_ACCOUNT_ID`.
- `sigv4`: every request must carry a valid AWS Signature V4 (header or presigned query string)
  made with a configured credential. The credential's account id scopes which queues the caller
  sees; each account gets its own queue namespace. Errors match AWS (`SignatureDoesNotMatch`,
  `InvalidClientTokenId`, `MissingAuthenticationToken`, `RequestExpired`).

IAM policies are not evaluated; `AddPermission`/`RemovePermission`/`Policy` are stored and
returned verbatim. See [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md).

## Deploying on Kubernetes

The chart deploys the three roles: a migration Job per release, a scheduler Deployment (2
replicas) and a server Deployment (3 replicas), both gated on the schema by an init container.

```bash
helm install qd deploy/helm/qd \
  --set postgresql.cnpg.enabled=true          # provision a 3-node CloudNativePG cluster
# or point at an existing database:
helm install qd deploy/helm/qd --set database.existingSecret=qd-db
```

Released artifacts are published by CI to GitLab (tag `vX.Y.Z` to release):

| Artifact | Location |
|---|---|
| Container image | `registry.hexploits.cloud/hexploits/services/simply-queued:<version>` (`:latest` and `:<sha>` from `main`) |
| Helm chart | `helm repo add qd https://gitlab.hexploits.cloud/api/v4/projects/116/packages/helm/stable` |
| Binary (linux/amd64) | Generic package `qd/<tag>/qd-linux-amd64` |

Clients in the cluster then use `http://qd.<namespace>.svc:9324`.
See [docs/OPERATIONS.md](docs/OPERATIONS.md) for sizing, monitoring and upgrades, and
[deploy/examples](deploy/examples) for a kind quickstart and a production CNPG cluster.

## High availability

Replicas are stateless and leaderless - there is no Raft or leader election in qd
itself. All coordination happens in Postgres, and Postgres HA (CloudNativePG with synchronous
replication, the chart default) provides consensus, failover and durability. Any replica can be
killed at any time; an acknowledged message survives the loss of the Postgres primary.
This is proven by the resilience suite: see [docs/RESILIENCE.md](docs/RESILIENCE.md).

## Monitoring

Prometheus metrics at `:9325/metrics` (CloudWatch-equivalent queue metrics such as depth and
age of oldest message, plus request, database and runtime metrics), optional Pushgateway and
DogStatsD push, unit-tested alert rules with runbooks, and a Grafana dashboard - all deployable
from the Helm chart (`serviceMonitor.enabled`, `prometheusRule.enabled`,
`grafanaDashboard.enabled`). See [docs/OPERATIONS.md](docs/OPERATIONS.md#metrics) and
[deploy/observability](deploy/observability/README.md).

## Testing

The test strategy is "use the real AWS clients, real processes and real failures":

| Suite | What it proves | Command |
|---|---|---|
| Unit tests | protocol translation, SigV4, validation, MD5, FIFO checker | `cargo test --workspace --lib` |
| Rust E2E (46 tests) | official **AWS SDK for Rust** against real servers on throwaway databases, incl. multi-replica, restarts, metrics publishing (scrape, Pushgateway, DogStatsD) | `cargo test -p qd-e2e` |
| Python E2E (187 tests × 2) | official **boto3**, run twice: latest botocore (JSON protocol) and botocore 1.31.80 (Query/XML protocol) | `bash e2e/python/run.sh` |
| CLI smoke | **AWS CLI v2** | `bash e2e/cli/smoke.sh` (run by `run.sh`) |
| Resilience / chaos | rolling restarts, upgrade + rollback, SIGKILLed replicas, Postgres outage, primary failover to a synchronous standby, migration races - under load, zero lost messages | `e2e/chaos/run.sh` |
| Soak | hours of load with a disruption every minute | `QD_SOAK_SECONDS=7200 e2e/chaos/run.sh soak` |
| Kubernetes (manual) | kind + CloudNativePG + Helm chart: restart, upgrade, pod crash, Postgres primary kill, rollback with an in-cluster verifier; needs Docker | `e2e/k8s/run.sh` |
| Observability | alert rule unit tests, chart drift, every metric used by alerts/dashboards exists | `deploy/observability/check.sh`, `e2e/observability/check-metrics.sh` |
| Load | AWS SDK load generator | `cargo run --release -p qd-e2e --example loadgen -- http://host:9324 30 16 16` |

All suites that start servers need a Postgres they can create databases on: `DATABASE_URL`
(default `postgres://qd:qd@localhost/postgres`).

The Python suite is target-agnostic: `QD_E2E_TARGET=aws bash e2e/python/run.sh` runs the same
tests against real AWS SQS (using your normal AWS credentials) to check behavioural parity; tests
relying on qd-only knobs are skipped there. See [e2e/README.md](e2e/README.md).

## Layout

```
crates/qd-engine       protocol-agnostic queue engine on Postgres (+ migrations)
crates/qd-sqs          SQS wire protocols (JSON 1.0, Query/XML), SigV4, action handlers
crates/qd   server binary: config, listeners, health, metrics, shutdown
crates/qd-e2e          AWS SDK test harness, E2E tests, load generator
e2e/                   boto3 suite, AWS CLI smoke, chaos runner, Kubernetes resilience test
deploy/                Helm chart, example manifests, alert rules and Grafana dashboard
docs/                  architecture, compatibility, operations (runbooks), resilience
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for how it works and how additional protocols
(e.g. Kafka) fit in.
