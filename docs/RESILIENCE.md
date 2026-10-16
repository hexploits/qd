# Resilience: what is proven, and how

qd's high availability comes from two layers:

- **Server replicas are stateless and leaderless.** No Raft, no leader election, no in-memory
  state: every replica can serve every request, and coordination happens inside Postgres (row
  locks with `SKIP LOCKED`, a per-queue advisory lock for FIFO ordering, LISTEN/NOTIFY for
  wakeups). Losing a replica loses nothing; its in-flight messages reappear after their
  visibility timeout.
- **Consensus and durability are delegated to Postgres.** CloudNativePG (or a managed Postgres)
  performs primary election, fencing and failover. With synchronous replication (the chart
  default) a message is acknowledged to the producer only after a standby has it, so a primary
  failure loses nothing that was acknowledged.

## Invariants checked by every scenario

The chaos harness (`crates/qd-e2e/src/chaos`) drives a standard and a FIFO queue through the
**official AWS SDK** (standard retry mode, like production clients) while failures are injected,
then drains and checks:

1. **No acknowledged message is lost** — every `SendMessage` that returned success is received.
2. **FIFO order holds** — within a group, no message is delivered for the first time after a
   newer one (redeliveries of the same batch after failed deletes are allowed: at-least-once).
3. **Client-visible errors** (after SDK retries) are zero where the operation should be
   invisible (rollouts, replica crashes), or confined to the outage window (database outage).
4. **The system recovers by itself** — readiness returns, throughput resumes, and cross-replica
   long-poll wakeups work again (asserted < 1.5s, i.e. LISTEN reconnected, not just polling).

## Scenarios

Every scenario runs the production topology: the `migrate` command first, then 2 scheduler
replicas and N server replicas. Rollouts run the new release's migration first, then roll the
schedulers, then the servers. Real server processes (so crashes are real `SIGKILL`s and upgrades
mix binaries) and real PostgreSQL instances; a TCP proxy plays the Kubernetes Service in front of replicas, another
plays CloudNativePG's `-rw` Service in front of the current primary.

| Scenario | What happens | Asserted |
|---|---|---|
| `rolling_restart_is_invisible_to_clients` | 3 replicas replaced one by one, twice (surge → ready → add endpoint → remove old endpoint → preStop → SIGTERM) | invariants 1–2, **zero client errors**, every old replica exits 0 within the grace period |
| `rolling_upgrade_and_rollback` | previous release → this build → previous → this build, under load (old and new replicas serve side by side) | invariants 1–2, zero client errors |
| `replica_crashes_lose_nothing` | 4 × SIGKILL a server replica (and 2 × a scheduler); endpoint removed only after 2s (readiness lag); replacements started | invariants 1–2, zero client errors |
| `database_outage_and_recovery` | Postgres stopped for 8s, restarted | all replicas report not-ready during the outage; recover; errors only in the outage window; invariants 1–2; LISTEN reconnected |
| `primary_failover_with_synchronous_standby_loses_nothing` | primary crashes (`pg_ctl -m immediate`), synchronous standby promoted, `-rw` endpoint switched | **zero acknowledged messages lost**, recovery without restarting replicas, invariants 1–2 |
| `migrations_gate_startup` | scheduler, server and `wait-for-schema` start before the migration Job | alive but not ready, SQS port closed, schema untouched; all start by themselves once `migrate` completes |
| `migrate_waits_for_the_database` | the migration Job starts while Postgres is down | keeps waiting, completes once Postgres is up |
| `concurrent_migrations_apply_once` | 6 migration Jobs start simultaneously on an empty database | all exit 0, migrations applied exactly once |
| `scheduler_outage_does_not_affect_the_api` | every scheduler SIGKILLed under load; a DLQ redrive started meanwhile | **zero client errors**, invariants 1–2, outage visible in `qd_scheduler_heartbeat_age_seconds`, redrive waits and completes once schedulers return |
| `older_release_starts_on_newer_schema` | database carries a migration from a "future" release | this release's `migrate`, `wait-for-schema`, scheduler and server all still work (rollback safety) |
| `soak` (opt-in, `QD_SOAK_SECONDS`) | hours of load with a rolling restart or crash every minute | invariants 1–2, zero client errors, memory growth reported |

Kubernetes-level (`e2e/k8s/run.sh`, run manually — it needs a Docker daemon for kind): kind + CloudNativePG (3
instances, synchronous replication) + the Helm chart; an in-cluster verifier
(`crates/qd-e2e/examples/verify.rs`) runs during `rollout restart`, `helm upgrade`, a forced pod
deletion, deletion of the Postgres primary pod (CNPG failover) and `helm rollback`, then checks
invariants 1–2. The same verifier can be pointed at any environment:
`cargo run --release -p qd-e2e --example verify -- https://sqs.staging.internal 600`.

## Running

```bash
e2e/chaos/run.sh                                   # all scenarios (~2 min)
e2e/chaos/run.sh primary_failover                  # one scenario
QD_UPGRADE_FROM_REF=v0.1.0 e2e/chaos/run.sh rolling_upgrade   # upgrade from a specific release
QD_SOAK_SECONDS=7200 e2e/chaos/run.sh soak         # 2h soak
e2e/k8s/run.sh                                     # needs privileged Docker, kind, kubectl, helm
```

CI runs the chaos suite on every pipeline (it gates image publishing) and the 2-hour soak on
schedules. The Kubernetes test is run manually on a machine with Docker, e.g. before a release.

## Latest results (2-vCPU development environment, 3-role topology)

| Scenario | Acknowledged (std / FIFO) | Lost | FIFO violations | Client errors | Notes |
|---|---|---|---|---|---|
| Rolling restart ×2 | 761 / 750 | 0 | 0 | 0 | migrate → schedulers → servers each time |
| Upgrade → rollback → upgrade | 942 / 927 | 0 | 0 | 0 | same-build upgrade (no release tag yet) |
| Server and scheduler SIGKILLs | 882 / 857 | 0 | 0 | 0 | |
| All schedulers down | 1,880 / 1,753 | 0 | 0 | 0 | redrive resumed after schedulers returned |
| Postgres down 8s | 297 / 336 | 0 | 0 | 4 / 3 | errors only inside the outage window (8s outage exceeds the test client's retry budget); recovered 19ms after Postgres returned |
| Primary crash → sync standby | 559 / 610 | 0 | 0 | 0 | service recovered 139ms after the crash |
| Soak (150s, 3 disruptions) | 13,081 / 11,801 | 0 | 0 | 0 | RSS flat at ~23MB per replica (pre-split run) |

## Known limits and what is not covered here

- **Write availability during a Postgres failover** equals the failover time of your Postgres
  (CNPG typically 10–30s in real clusters); requests fail with retryable 503s meanwhile.
- **FIFO receive throughput per queue** is bounded by the per-queue lock (by design, for
  ordering), like SQS's own FIFO limits.
- **Network partitions between replicas and Postgres** behave like the database outage scenario
  for the partitioned replicas (not-ready, removed from the Service); split-brain at the
  database layer is CNPG's responsibility (fencing).
- The upgrade scenario becomes a true cross-release test once a release tag exists; until then
  it upgrades the build to itself (the rollback-schema scenario still covers schema tolerance).
