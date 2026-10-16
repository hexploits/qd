# Architecture

```
                 ┌──────────────── Kubernetes Service :9324 ────────────────┐
  AWS SDKs ───▶  │  replica 1        replica 2        replica N             │
  AWS CLI        │  ┌─────────┐      ┌─────────┐      ┌─────────┐           │
  libraries      │  │ qd-sqs  │      │ qd-sqs  │      │ qd-sqs  │  protocol │
                 │  │ engine  │      │ engine  │      │ engine  │  engine   │
                 │  └────┬────┘      └────┬────┘      └────┬────┘           │
                 └───────┼────────────────┼────────────────┼────────────────┘
                         │   SQL + LISTEN/NOTIFY (wakeups) │
                         ▼                ▼                ▼
                 ┌─────────────────────────────────────────────────────────┐
                 │ PostgreSQL (CloudNativePG: primary + sync replicas)       │
                 └─────────────────────────────────────────────────────────┘
```

## Process roles

```
 helm upgrade ──▶ 1. migrate Job ──(schema current)──▶ 2a. scheduler Deployment (2 replicas)
                                                      2b. server Deployment (N replicas)
```

- **migrate** (Job, one per release) is the only thing that changes the schema. It waits for
  Postgres, applies migrations under an advisory lock and exits.
- **scheduler** runs background maintenance: the janitor (retention, deleted-queue reclaim,
  dedup/attempt expiry, idle FIFO groups), message move tasks (DLQ redrive), per-queue metric
  gauges, and a heartbeat in `qd_scheduler_heartbeats`. Work is idempotent and uses
  `SKIP LOCKED` batches, so replicas run concurrently without leader election.
- **server** serves the SQS API. Everything on the request path — including dead-lettering,
  which happens inside ReceiveMessage — lives here, so the API keeps working if every scheduler
  is down; only deferred maintenance pauses (and resumes when a scheduler returns).
- Both long-running roles bind their admin port first (`/healthz` live, `/readyz` not ready)
  and wait for the schema before starting work, so pods scheduled before the migration has
  finished simply wait instead of crash-looping.

## Crates

| Crate | Responsibility |
|---|---|
| `qd-engine` | Queue semantics on Postgres: queues, attributes, send/receive/delete, visibility, FIFO, dedup, DLQ, redrive, retention, background janitor, cross-replica wakeups. No knowledge of HTTP or wire formats. |
| `qd-sqs` | The SQS API: protocol detection, AWS JSON 1.0 and Query/XML codecs, SigV4 verification, request validation and mapping to engine calls, AWS-shaped errors. |
| `qd` | Process wiring and roles (`migrate`, `scheduler`, `server`, all-in-one): configuration, connection pool, migrations, listeners, `/healthz` `/readyz` `/metrics`, signal handling and graceful drain. |
| `qd-e2e` | Test harness that boots real servers on throwaway databases and drives them with the official AWS SDK. |

### Adding protocols (e.g. Kafka)

Frontends are independent of the engine, so a new protocol is a new crate that owns its listener
and translates requests into engine calls, wired up in `qd` next to `qd-sqs`.

- **Queue-style protocols** (AMQP-ish work queues, STOMP, an HTTP/gRPC native API) map directly
  onto the existing engine operations (`send_message`, `receive_messages`, `delete_message`, ...).
- **Kafka** is a log, not a queue: consumers track offsets per partition, messages are retained
  regardless of consumption, and many consumer groups read the same data. That needs a sibling
  engine module rather than a reuse of SQS visibility semantics — e.g. `qd_log_partitions`,
  `qd_log_records(partition_id, offset BIGSERIAL, ...)` and `qd_consumer_offsets` tables — plus a
  Kafka wire-protocol crate (Produce/Fetch/Metadata/OffsetCommit/group coordination). The
  shared pieces are the Postgres pool, migrations framework, auth/accounts, notification
  plumbing (`Notifier`), metrics and the server process. Bridges such as "a topic that fans out
  into SQS queues" can then be implemented inside the engine where both models are visible.

## Data model

All state lives in Postgres (`crates/qd-engine/migrations`):

| Table | Purpose |
|---|---|
| `qd_queues` | One row per queue: account, name, FIFO flag, attributes (JSONB), tags, DLQ target (denormalised for `ListDeadLetterSourceQueues`). |
| `qd_messages` | Every message. `visible_at` drives visibility/delays, `expires_at` retention, `receipt_token` identifies the current receive, `receive_count` drives DLQ moves. The `id` (BIGSERIAL) is also the FIFO sequence number. |
| `qd_fifo_groups` | One row per active FIFO message group, used to find deliverable group heads efficiently. |
| `qd_fifo_dedup` | FIFO deduplication window (5 minutes), keyed by queue + scope + dedup id. |
| `qd_receive_attempts` | FIFO `ReceiveRequestAttemptId` replay records (5 minutes). |
| `qd_move_tasks` | DLQ redrive tasks (`StartMessageMoveTask`). |
| `qd_deleted_queues` | Tombstones so the janitor reclaims messages of deleted queues in batches. |

Time is always taken from the database (`now()`), so replicas with skewed clocks agree on
visibility, delays and retention.

## Core algorithms

**Send.** A message is inserted with `visible_at = now() + delay` and `expires_at = now() +
retention`. Standard-queue sends (including whole batches) are a single `INSERT ... SELECT FROM
unnest(...)` statement. The request is acknowledged only after commit.

**Receive (standard).** One statement claims messages:
`SELECT ... WHERE queue_id = $1 AND visible_at <= now() ORDER BY visible_at, id LIMIT n FOR UPDATE
SKIP LOCKED` feeding an `UPDATE` that sets `visible_at = now() + visibility_timeout`, increments
`receive_count` and rotates `receipt_token`. `SKIP LOCKED` lets any number of consumers on any
replica claim disjoint messages without blocking each other.

**Receive (FIFO).** Ordering needs stronger guarantees: receives on one FIFO queue are serialised
with a transaction-scoped advisory lock. A group is deliverable when its oldest live message is
visible (nothing earlier is in flight or delayed); from each deliverable group the visible
prefix is taken, oldest group head first. This gives SQS's guarantees: strict order within a
group, and a group with in-flight messages delivers nothing else until they are deleted or their
visibility expires.

**Receipt handles** encode `(queue id, message row id, receipt token)`. The token is a random
UUID rotated on every receive, so handles are unguessable and stale handles are detectable.
Standard queues delete a message even via a stale handle (as SQS does); FIFO queues and
`ChangeMessageVisibility` require the current handle.

**Dead-letter queues.** When a receive selects a message whose `receive_count` has reached
`maxReceiveCount`, it is moved (same message id, original send time,
`DeadLetterQueueSourceArn` set) to the DLQ in the same transaction instead of being delivered,
and the receive keeps filling from the remaining messages.

**Redrive.** `StartMessageMoveTask` records a task with a cut-off (messages that arrived before
the task). A worker on every replica claims due tasks with `FOR UPDATE SKIP LOCKED` and moves
rate-limited batches back to each message's source queue (or `DestinationArn`). The task row's
`updated_at` doubles as the rate-limit clock, so N replicas don't multiply the rate.

**Long polling.** A poller arms an in-process wakeup, checks the database, and if empty sleeps
until (a) a wakeup, (b) the next message in the queue becomes visible (queried from the
visibility index), (c) a safety recheck interval, or (d) the wait time ends. Writers call
`publish(queue)` after commit: local pollers wake immediately and a background flusher sends a
coalesced `pg_notify` every few milliseconds; every replica `LISTEN`s and wakes its pollers.
NOTIFY is deliberately kept out of data transactions because Postgres serialises the commits of
notifying transactions.

**Janitor.** Every replica periodically deletes expired messages, reclaims deleted queues,
expires dedup/attempt records and removes idle empty FIFO groups. All steps are idempotent
`SKIP LOCKED` batches with time budgets, so concurrent janitors cooperate.

## High availability and consistency

There is deliberately no Raft (or any consensus protocol) inside qd. Replicas hold no
state that needs agreeing on; the only state is in Postgres, whose HA layer (CloudNativePG,
Patroni or a managed service) already solves leader election, fencing and replication. Building
a second consensus layer on top would add failure modes without adding guarantees.

- Replicas are stateless; any replica can serve any request, including deleting a message
  received through another replica. Kill any replica at any time: in-flight messages simply
  become visible again after their visibility timeout (at-least-once delivery, as in SQS).
- Durability and availability of data are those of Postgres. Use synchronous replication
  (`deploy/examples/cnpg-cluster.yaml`) to make acknowledged sends survive a primary failure.
  During a Postgres failover, requests fail with `ServiceUnavailable` (HTTP 503, which SDKs
  retry) until the new primary is reachable; readiness probes report not-ready meanwhile.
- Database migrations are additive per release (expand/contract) and the migrator tolerates
  migrations from newer releases, so mixed-version rollouts and rollbacks are safe.
- Queue metadata is cached per replica for `QD_QUEUE_CACHE_TTL_MS` (default 1s). Attribute
  changes made via one replica are visible on others within that time (AWS documents up to 60s).
  `GetQueueAttributes` always reads fresh.
- Deleting a queue is immediate; its messages are reclaimed in the background.

## Performance notes

Indicative numbers from a 2-vCPU development pod running Postgres, the server and the load
generator together (so treat them as a floor, not a ceiling): ~21k msg/s sends with
`SendMessageBatch` at 7ms p50 batch latency; ~8k msg/s sent with ~3.7k msg/s concurrently
received+deleted on one standard queue; ~2k msg/s through a FIFO queue. Throughput scales with
Postgres CPU/IO; the server itself is light. Per FIFO queue, receive throughput is bounded by
the per-queue lock (similar in spirit to SQS FIFO limits).
