//! Resilience scenarios with real processes: rolling restarts, rolling upgrades and rollbacks,
//! crashes, database outages, and Postgres primary failover — all under continuous load, with
//! every acknowledged message accounted for.
//!
//! These are `#[ignore]`d because they need the server binary and PostgreSQL server binaries and
//! take minutes. Run them with `e2e/chaos/run.sh`, or:
//!
//! ```text
//! cargo build -p qd
//! cargo test -p qd-e2e --test chaos -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! Environment: `QD_BINARY` (binary under test), `QD_UPGRADE_FROM_BINARY` (previous release for
//! upgrade/rollback scenarios), `QD_PG_BIN` (Postgres binaries), `DATABASE_URL` (admin URL of a
//! Postgres used by the scenarios that don't need their own cluster).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use qd_e2e::chaos::pg::PgNode;
use qd_e2e::chaos::proc::ServerProc;
use qd_e2e::chaos::proxy::Proxy;
use qd_e2e::chaos::workload::{Workload, WorkloadConfig, resilient_client};
use qd_e2e::chaos::{log, previous_binary, scratch_dir, server_binary, wait_until};

/// Runs the `migrate` command to completion (what the Helm migration Job does).
async fn run_migrate(binary: &Path, db_url: &str, scratch: &Path, name: &str) {
    let mut job = ServerProc::spawn(name, binary, &["migrate"], db_url, scratch, &[]);
    let ok = job.wait_exit(Duration::from_secs(120)).await;
    assert_eq!(ok, Some(true), "{name} failed; log:\n{}", job.tail_log(30));
}

/// The production topology: a migration Job, then scheduler replicas and API server replicas
/// (started in parallel) with the servers behind a load balancer, all on one database.
struct AppCluster {
    /// API servers (`server` role).
    replicas: Vec<ServerProc>,
    /// Background maintenance (`scheduler` role).
    schedulers: Vec<ServerProc>,
    lb: Proxy,
    db_url: String,
    scratch: PathBuf,
    generation: usize,
    extra_env: Vec<(&'static str, String)>,
}

impl AppCluster {
    async fn start(n: usize, binary: &Path, db_url: &str, scratch: &Path) -> Self {
        Self::start_with(n, 2, binary, db_url, scratch).await
    }

    async fn start_with(n: usize, n_schedulers: usize, binary: &Path, db_url: &str, scratch: &Path) -> Self {
        // 1. Migrations; 2. scheduler and servers in parallel.
        run_migrate(binary, db_url, scratch, "migrate-g0").await;
        let mut schedulers: Vec<ServerProc> = (0..n_schedulers)
            .map(|i| ServerProc::spawn(&format!("sched{i}-g0"), binary, &["scheduler"], db_url, scratch, &[]))
            .collect();
        let mut replicas: Vec<ServerProc> = (0..n)
            .map(|i| ServerProc::spawn(&format!("app{i}-g0"), binary, &["server"], db_url, scratch, &[]))
            .collect();
        for p in schedulers.iter_mut().chain(replicas.iter_mut()) {
            p.wait_ready(Duration::from_secs(60)).await;
        }
        let lb = Proxy::start(replicas.iter().map(|r| r.addr()).collect()).await;
        Self {
            replicas,
            schedulers,
            lb,
            db_url: db_url.into(),
            scratch: scratch.into(),
            generation: 0,
            extra_env: Vec::new(),
        }
    }

    fn endpoint(&self) -> String {
        self.lb.endpoint()
    }

    /// A release rollout exactly as the Helm chart performs it: the migration Job of the new
    /// release runs first (old pods keep serving), then the scheduler and server Deployments
    /// roll. Each pod is replaced the Kubernetes way: surge a new pod, wait for readiness, add
    /// it to the Service, remove the old endpoint, wait the preStop delay, SIGTERM, and require
    /// a clean exit within the grace period.
    async fn rolling_replace(&mut self, binary: &Path) {
        self.generation += 1;
        let gen_ = self.generation;
        log(format!("=== rollout of generation {gen_} ({})", binary.display()));
        run_migrate(binary, &self.db_url, &self.scratch, &format!("migrate-g{gen_}")).await;
        for i in 0..self.schedulers.len() {
            let mut fresh = ServerProc::spawn(
                &format!("sched{i}-g{gen_}"),
                binary,
                &["scheduler"],
                &self.db_url,
                &self.scratch,
                &self.extra_env,
            );
            fresh.wait_ready(Duration::from_secs(60)).await;
            let mut old = std::mem::replace(&mut self.schedulers[i], fresh);
            let (clean, took) = old.terminate(Duration::from_secs(20)).await;
            assert!(clean, "{} did not exit cleanly on SIGTERM; log:\n{}", old.name, old.tail_log(30));
            assert!(took < Duration::from_secs(15), "{} took {took:?} to drain", old.name);
        }
        for i in 0..self.replicas.len() {
            let mut fresh = ServerProc::spawn(
                &format!("app{i}-g{gen_}"),
                binary,
                &["server"],
                &self.db_url,
                &self.scratch,
                &self.extra_env,
            );
            fresh.wait_ready(Duration::from_secs(60)).await;
            self.lb.add_backend(fresh.addr());
            let mut old = std::mem::replace(&mut self.replicas[i], fresh);
            self.lb.remove_backend(old.addr());
            tokio::time::sleep(Duration::from_millis(500)).await; // preStop: endpoint propagation
            let (clean, took) = old.terminate(Duration::from_secs(20)).await;
            assert!(clean, "{} did not exit cleanly on SIGTERM; log:\n{}", old.name, old.tail_log(30));
            assert!(took < Duration::from_secs(15), "{} took {took:?} to drain", old.name);
        }
    }

    async fn all_ready(&self) -> bool {
        for p in self.schedulers.iter().chain(self.replicas.iter()) {
            if !p.is_ready().await {
                return false;
            }
        }
        true
    }

    async fn none_ready(&self) -> bool {
        for p in self.schedulers.iter().chain(self.replicas.iter()) {
            if p.is_ready().await {
                return false;
            }
        }
        true
    }

    async fn stop(mut self) {
        for r in self.replicas.iter_mut().chain(self.schedulers.iter_mut()) {
            let _ = r.terminate(Duration::from_secs(20)).await;
        }
        self.lb.shutdown();
    }
}

/// Reads a gauge value from a Prometheus scrape.
fn gauge(scrape: &str, name: &str) -> Option<f64> {
    scrape.lines().find(|l| l.starts_with(name) && l[name.len()..].starts_with(' ')).and_then(|l| {
        let v = l.rsplit(' ').next()?;
        if v == "+Inf" { Some(f64::INFINITY) } else { v.parse().ok() }
    })
}

async fn local_db() -> qd_e2e::TestDb {
    qd_e2e::TestDb::create().await
}

fn finish_scratch(scratch: &Path) {
    let _ = std::fs::remove_dir_all(scratch);
}

/// Rolling restarts of every replica, twice, under load: clients must not notice at all.
#[tokio::test]
#[ignore]
async fn rolling_restart_is_invisible_to_clients() {
    let scratch = scratch_dir("rolling");
    let db = local_db().await;
    let binary = server_binary();
    let mut cluster = AppCluster::start(3, &binary, &db.url, &scratch).await;
    let started = Instant::now();
    let std_load = Workload::start(&cluster.endpoint(), WorkloadConfig::standard(&qd_e2e::qname("roll"))).await;
    let fifo_load = Workload::start(&cluster.endpoint(), WorkloadConfig::fifo(&qd_e2e::qname("roll"))).await;
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await);

    for _ in 0..2 {
        cluster.rolling_replace(&binary).await;
        assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await, "no progress after rollout");
        assert!(fifo_load.wait_for_progress(50, Duration::from_secs(30)).await, "no FIFO progress after rollout");
    }

    let std_report = std_load.finish(Duration::from_secs(60), started).await;
    let fifo_report = fifo_load.finish(Duration::from_secs(60), started).await;
    std_report.assert_no_loss_and_ordered();
    fifo_report.assert_no_loss_and_ordered();
    std_report.assert_no_client_errors();
    fifo_report.assert_no_client_errors();
    cluster.stop().await;
    db.drop_db().await;
    finish_scratch(&scratch);
}

/// Rolling upgrade from the previous release to this build, then a rollback, under load.
/// Old and new replicas serve side by side mid-rollout, sharing one (migrated) schema.
#[tokio::test]
#[ignore]
async fn rolling_upgrade_and_rollback() {
    let scratch = scratch_dir("upgrade");
    let db = local_db().await;
    let new = server_binary();
    let old = previous_binary().unwrap_or_else(|| {
        log(
            "QD_UPGRADE_FROM_BINARY not set: upgrading from this build to itself (set it to a previous release binary)",
        );
        new.clone()
    });
    let mut cluster = AppCluster::start(3, &old, &db.url, &scratch).await;
    let started = Instant::now();
    let std_load = Workload::start(&cluster.endpoint(), WorkloadConfig::standard(&qd_e2e::qname("upg"))).await;
    let fifo_load = Workload::start(&cluster.endpoint(), WorkloadConfig::fifo(&qd_e2e::qname("upg"))).await;
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await);

    cluster.rolling_replace(&new).await; // upgrade
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await);
    cluster.rolling_replace(&old).await; // `kubectl rollout undo`
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await);
    cluster.rolling_replace(&new).await; // and forward again
    assert!(fifo_load.wait_for_progress(50, Duration::from_secs(30)).await);

    let std_report = std_load.finish(Duration::from_secs(60), started).await;
    let fifo_report = fifo_load.finish(Duration::from_secs(60), started).await;
    std_report.assert_no_loss_and_ordered();
    fifo_report.assert_no_loss_and_ordered();
    std_report.assert_no_client_errors();
    fifo_report.assert_no_client_errors();
    cluster.stop().await;
    db.drop_db().await;
    finish_scratch(&scratch);
}

/// Replicas are SIGKILLed (no drain) while serving. Retries go to surviving replicas; messages
/// held by the dead replica's consumers come back after their visibility timeout.
#[tokio::test]
#[ignore]
async fn replica_crashes_lose_nothing() {
    let scratch = scratch_dir("crash");
    let db = local_db().await;
    let binary = server_binary();
    let mut cluster = AppCluster::start(3, &binary, &db.url, &scratch).await;
    let started = Instant::now();
    let std_load = Workload::start(&cluster.endpoint(), WorkloadConfig::standard(&qd_e2e::qname("crash"))).await;
    let fifo_load = Workload::start(&cluster.endpoint(), WorkloadConfig::fifo(&qd_e2e::qname("crash"))).await;
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await);

    for round in 0..4 {
        let i = round % cluster.replicas.len();
        let victim_addr = cluster.replicas[i].addr();
        cluster.replicas[i].kill().await;
        // The Service keeps routing to the dead pod until its readiness probe fails.
        tokio::time::sleep(Duration::from_secs(2)).await;
        cluster.lb.remove_backend(victim_addr);
        let mut fresh = ServerProc::spawn(
            &format!("app{i}-r{round}"),
            &binary,
            &["server"],
            &cluster.db_url,
            &cluster.scratch,
            &[],
        );
        fresh.wait_ready(Duration::from_secs(60)).await;
        cluster.lb.add_backend(fresh.addr());
        cluster.replicas[i] = fresh;
        if round % 2 == 1 {
            // Schedulers crash too; the other replica keeps maintenance going.
            let j = round % cluster.schedulers.len();
            cluster.schedulers[j].kill().await;
            let mut s = ServerProc::spawn(
                &format!("sched{j}-r{round}"),
                &binary,
                &["scheduler"],
                &cluster.db_url,
                &cluster.scratch,
                &[],
            );
            s.wait_ready(Duration::from_secs(60)).await;
            cluster.schedulers[j] = s;
        }
        assert!(std_load.wait_for_progress(50, Duration::from_secs(30)).await, "no progress after crash {round}");
    }

    let std_report = std_load.finish(Duration::from_secs(90), started).await;
    let fifo_report = fifo_load.finish(Duration::from_secs(90), started).await;
    std_report.assert_no_loss_and_ordered();
    fifo_report.assert_no_loss_and_ordered();
    // SDK retries route around the dead replica; nothing should surface to callers.
    std_report.assert_no_client_errors();
    fifo_report.assert_no_client_errors();
    cluster.stop().await;
    db.drop_db().await;
    finish_scratch(&scratch);
}

/// Postgres goes down for ~8s and comes back. Replicas report not-ready, fail fast with
/// retryable errors, then recover on their own (pool + LISTEN connection) with no lost messages.
#[tokio::test]
#[ignore]
async fn database_outage_and_recovery() {
    let scratch = scratch_dir("outage");
    let mut pg = PgNode::primary(&scratch, "pg");
    pg.execute("postgres", "CREATE DATABASE qd").await;
    let binary = server_binary();
    let cluster = AppCluster::start(2, &binary, &pg.url("qd"), &scratch).await;
    let started = Instant::now();
    let std_load = Workload::start(&cluster.endpoint(), WorkloadConfig::standard(&qd_e2e::qname("outage"))).await;
    let fifo_load = Workload::start(&cluster.endpoint(), WorkloadConfig::fifo(&qd_e2e::qname("outage"))).await;
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await);

    pg.stop();
    let outage_started = started.elapsed();
    let all_unready = wait_until(Duration::from_secs(20), || cluster.none_ready()).await;
    assert!(all_unready, "replicas kept reporting ready while the database was down");
    tokio::time::sleep(Duration::from_secs(8)).await;
    pg.start();
    let outage_ended = started.elapsed();

    let recovered = wait_until(Duration::from_secs(30), || cluster.all_ready()).await;
    assert!(recovered, "servers/schedulers did not become ready again after the database came back");
    log(format!("recovered {:?} after the database restarted", started.elapsed() - outage_ended));
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await, "no progress after recovery");
    assert!(fifo_load.wait_for_progress(50, Duration::from_secs(30)).await, "no FIFO progress after recovery");

    // The cross-replica wakeup channel must have reconnected on every replica.
    let reconnected = wait_until(Duration::from_secs(20), || async {
        for r in &cluster.replicas {
            if !r.metrics().await.lines().any(|l| l.trim() == "qd_notifier_connected 1") {
                return false;
            }
        }
        true
    })
    .await;
    assert!(reconnected, "LISTEN connection did not recover");
    cross_replica_wakeup(&cluster).await;

    let std_report = std_load.finish(Duration::from_secs(90), started).await;
    let fifo_report = fifo_load.finish(Duration::from_secs(90), started).await;
    std_report.assert_no_loss_and_ordered();
    fifo_report.assert_no_loss_and_ordered();
    // Errors are acceptable only while the database was actually unavailable.
    let grace = Duration::from_secs(15);
    for report in [&std_report, &fifo_report] {
        for (at, err) in &report.errors {
            assert!(
                *at + Duration::from_secs(1) >= outage_started && *at <= outage_ended + grace,
                "client error outside the outage window at {at:?}: {err}"
            );
        }
    }
    cluster.stop().await;
    drop(pg);
    finish_scratch(&scratch);
}

/// The Postgres primary crashes and the synchronous standby is promoted (what CloudNativePG does
/// automatically). Because commits are acknowledged only after the standby has them, no
/// acknowledged message may be lost, and service must resume without restarting replicas.
#[tokio::test]
#[ignore]
async fn primary_failover_with_synchronous_standby_loses_nothing() {
    let scratch = scratch_dir("failover");
    let mut primary = PgNode::primary(&scratch, "pg-a");
    primary.execute("postgres", "CREATE DATABASE qd").await;
    let mut standby = PgNode::standby(&scratch, "pg-b", &primary);
    primary.require_synchronous_standby().await;
    // The "-rw" service: replicas only ever know this address.
    let rw = Proxy::start(vec![primary.addr()]).await;
    let db_url = format!("postgres://qd@{}/qd", rw.addr);

    let binary = server_binary();
    let cluster = AppCluster::start(2, &binary, &db_url, &scratch).await;
    let started = Instant::now();
    let std_load = Workload::start(&cluster.endpoint(), WorkloadConfig::standard(&qd_e2e::qname("fo"))).await;
    let fifo_load = Workload::start(&cluster.endpoint(), WorkloadConfig::fifo(&qd_e2e::qname("fo"))).await;
    assert!(std_load.wait_for_progress(200, Duration::from_secs(30)).await);
    assert!(fifo_load.wait_for_progress(50, Duration::from_secs(30)).await);

    let acked_before = std_load.sent();
    primary.crash();
    rw.set_backends(vec![]);
    rw.reset_connections();
    let failover_started = Instant::now();
    standby.promote();
    rw.set_backends(vec![standby.addr()]);
    log(format!("failover: {acked_before} sends acknowledged before the crash; standby now primary"));

    let recovered = wait_until(Duration::from_secs(60), || cluster.all_ready()).await;
    assert!(recovered, "servers/schedulers did not recover after failover");
    log(format!("service recovered {:?} after the primary crashed", failover_started.elapsed()));
    assert!(std_load.wait_for_progress(200, Duration::from_secs(60)).await, "no progress after failover");
    assert!(fifo_load.wait_for_progress(50, Duration::from_secs(60)).await, "no FIFO progress after failover");
    cross_replica_wakeup(&cluster).await;

    let std_report = std_load.finish(Duration::from_secs(120), started).await;
    let fifo_report = fifo_load.finish(Duration::from_secs(120), started).await;
    std_report.assert_no_loss_and_ordered();
    fifo_report.assert_no_loss_and_ordered();
    cluster.stop().await;
    rw.shutdown();
    drop(standby);
    drop(primary);
    finish_scratch(&scratch);
}

/// Several migration Jobs (e.g. an overlapping retry and a new release) start at once on an
/// empty database: all succeed and every migration is applied exactly once.
#[tokio::test]
#[ignore]
async fn concurrent_migrations_apply_once() {
    let scratch = scratch_dir("migrate");
    let db = local_db().await;
    let binary = server_binary();
    let mut jobs: Vec<ServerProc> = (0..6)
        .map(|i| ServerProc::spawn(&format!("migrate{i}"), &binary, &["migrate"], &db.url, &scratch, &[]))
        .collect();
    for j in jobs.iter_mut() {
        assert_eq!(j.wait_exit(Duration::from_secs(120)).await, Some(true), "log:\n{}", j.tail_log(20));
    }
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url).await.unwrap();
    let (applied, failed): (i64, i64) =
        sqlx::query_as("SELECT count(*), count(*) FILTER (WHERE NOT success) FROM _sqlx_migrations")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(applied as usize, qd_engine::MIGRATOR.iter().count());
    assert_eq!(failed, 0);
    db.drop_db().await;
    finish_scratch(&scratch);
}

/// Startup order: scheduler and server pods may be scheduled before the migration Job has
/// run. They must stay alive-but-not-ready without touching the schema (no SQS listener, no
/// background work) and start by themselves once the migration completes.
#[tokio::test]
#[ignore]
async fn migrations_gate_startup() {
    let scratch = scratch_dir("gate");
    let db = local_db().await;
    let binary = server_binary();
    let mut scheduler = ServerProc::spawn("sched-early", &binary, &["scheduler"], &db.url, &scratch, &[]);
    let mut server = ServerProc::spawn("server-early", &binary, &["server"], &db.url, &scratch, &[]);
    let mut init = ServerProc::spawn("init-wait", &binary, &["wait-for-schema"], &db.url, &scratch, &[]);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(scheduler.is_alive().await && server.is_alive().await, "pods must be live (liveness) while waiting");
    assert!(!scheduler.is_ready().await && !server.is_ready().await, "pods must not be ready before migrations");
    assert!(
        std::net::TcpStream::connect(server.addr()).is_err(),
        "the SQS port must not accept traffic before migrations"
    );
    assert!(init.is_running(), "wait-for-schema must keep waiting before migrations");
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url).await.unwrap();
    let touched: bool =
        sqlx::query_scalar("SELECT to_regclass('public.qd_messages') IS NOT NULL").fetch_one(&mut conn).await.unwrap();
    assert!(!touched, "nothing but the migration Job may create the schema");

    run_migrate(&binary, &db.url, &scratch, "migrate").await;
    assert_eq!(init.wait_exit(Duration::from_secs(30)).await, Some(true), "wait-for-schema did not finish");
    scheduler.wait_ready(Duration::from_secs(30)).await;
    server.wait_ready(Duration::from_secs(30)).await;

    let client = resilient_client(&server.endpoint(), 3);
    let url = client.create_queue().queue_name("after-migrations").send().await.unwrap().queue_url.unwrap();
    client.send_message().queue_url(&url).message_body("hello").send().await.unwrap();
    let got = client.receive_message().queue_url(&url).wait_time_seconds(2).send().await.unwrap();
    assert_eq!(got.messages().len(), 1);
    // The server sees the scheduler's heartbeat.
    let ok = wait_until(Duration::from_secs(20), || async {
        gauge(&server.metrics().await, "qd_scheduler_heartbeat_age_seconds").is_some_and(|a| a < 15.0)
    })
    .await;
    assert!(ok, "scheduler heartbeat not visible to the server");
    server.terminate(Duration::from_secs(20)).await;
    scheduler.terminate(Duration::from_secs(20)).await;
    db.drop_db().await;
    finish_scratch(&scratch);
}

/// The migration Job starts while Postgres is down (e.g. CNPG still bootstrapping or failing
/// over): it must wait and complete once the database is reachable, not fail.
#[tokio::test]
#[ignore]
async fn migrate_waits_for_the_database() {
    let scratch = scratch_dir("migrate-wait");
    let mut pg = PgNode::primary(&scratch, "pg");
    pg.execute("postgres", "CREATE DATABASE qd").await;
    pg.stop();
    let binary = server_binary();
    let mut job = ServerProc::spawn("migrate", &binary, &["migrate"], &pg.url("qd"), &scratch, &[]);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(job.is_running(), "migrate gave up while the database was down; log:\n{}", job.tail_log(20));
    pg.start();
    assert_eq!(job.wait_exit(Duration::from_secs(60)).await, Some(true), "log:\n{}", job.tail_log(20));
    drop(pg);
    finish_scratch(&scratch);
}

/// Every scheduler dies (or is mid-rollout) for a while under load. The API must be completely
/// unaffected; the outage must be visible in metrics; and deferred scheduler work (here a DLQ
/// redrive started during the outage) must resume once a scheduler is back.
#[tokio::test]
#[ignore]
async fn scheduler_outage_does_not_affect_the_api() {
    let scratch = scratch_dir("sched-outage");
    let db = local_db().await;
    let binary = server_binary();
    let mut cluster = AppCluster::start_with(2, 2, &binary, &db.url, &scratch).await;
    let started = Instant::now();
    let std_load = Workload::start(&cluster.endpoint(), WorkloadConfig::standard(&qd_e2e::qname("so"))).await;
    let fifo_load = Workload::start(&cluster.endpoint(), WorkloadConfig::fifo(&qd_e2e::qname("so"))).await;
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await);

    for s in cluster.schedulers.iter_mut() {
        s.kill().await;
    }
    // Start a redrive while no scheduler runs: it must wait, not fail.
    let c = resilient_client(&cluster.endpoint(), 3);
    let dlq = c.create_queue().queue_name(qd_e2e::qname("so-dlq")).send().await.unwrap().queue_url.unwrap();
    let dlq_arn = c
        .get_queue_attributes()
        .queue_url(&dlq)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap()[&aws_sdk_sqs::types::QueueAttributeName::QueueArn]
        .clone();
    c.create_queue()
        .queue_name(qd_e2e::qname("so-src"))
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy,
            format!(r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":1}}"#),
        )
        .send()
        .await
        .unwrap();
    let dest = c.create_queue().queue_name(qd_e2e::qname("so-dest")).send().await.unwrap().queue_url.unwrap();
    let dest_arn = c
        .get_queue_attributes()
        .queue_url(&dest)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap()[&aws_sdk_sqs::types::QueueAttributeName::QueueArn]
        .clone();
    for i in 0..10 {
        c.send_message().queue_url(&dlq).message_body(format!("dead-{i}")).send().await.unwrap();
    }
    c.start_message_move_task().source_arn(&dlq_arn).destination_arn(&dest_arn).send().await.unwrap();

    let stale = wait_until(Duration::from_secs(40), || async {
        gauge(&cluster.replicas[0].metrics().await, "qd_scheduler_heartbeat_age_seconds").is_some_and(|a| a > 20.0)
    })
    .await;
    assert!(stale, "scheduler outage not visible in qd_scheduler_heartbeat_age_seconds");
    let tasks = c.list_message_move_tasks().source_arn(&dlq_arn).send().await.unwrap();
    assert_eq!(tasks.results()[0].status(), Some("RUNNING"));
    assert_eq!(tasks.results()[0].approximate_number_of_messages_moved(), 0, "nothing moves without a scheduler");
    assert!(std_load.wait_for_progress(100, Duration::from_secs(30)).await, "API stalled without schedulers");

    for (i, slot) in cluster.schedulers.iter_mut().enumerate() {
        let mut fresh = ServerProc::spawn(&format!("sched{i}-back"), &binary, &["scheduler"], &db.url, &scratch, &[]);
        fresh.wait_ready(Duration::from_secs(60)).await;
        *slot = fresh;
    }
    let resumed = wait_until(Duration::from_secs(30), || async {
        let t = c.list_message_move_tasks().source_arn(&dlq_arn).send().await.unwrap();
        t.results()[0].status() == Some("COMPLETED")
    })
    .await;
    assert!(resumed, "redrive did not resume after the schedulers returned");

    let std_report = std_load.finish(Duration::from_secs(60), started).await;
    let fifo_report = fifo_load.finish(Duration::from_secs(60), started).await;
    std_report.assert_no_loss_and_ordered();
    fifo_report.assert_no_loss_and_ordered();
    std_report.assert_no_client_errors();
    fifo_report.assert_no_client_errors();
    cluster.stop().await;
    db.drop_db().await;
    finish_scratch(&scratch);
}

/// A database already migrated by a *newer* release must not stop this release from starting
/// (otherwise `helm rollback` would hang on wait-for-schema or crash-loop).
#[tokio::test]
#[ignore]
async fn older_release_starts_on_newer_schema() {
    let scratch = scratch_dir("rollback");
    let db = local_db().await;
    let binary = server_binary();
    run_migrate(&binary, &db.url, &scratch, "migrate").await;

    // Simulate a future release having applied an extra migration.
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url).await.unwrap();
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
         VALUES (99990101000000, 'future release', true, '\\x00'::bytea, 0)",
    )
    .execute(&mut conn)
    .await
    .unwrap();

    run_migrate(&binary, &db.url, &scratch, "migrate-again").await;
    let mut init = ServerProc::spawn("init-wait", &binary, &["wait-for-schema"], &db.url, &scratch, &[]);
    assert_eq!(init.wait_exit(Duration::from_secs(20)).await, Some(true));
    let mut scheduler = ServerProc::spawn("sched", &binary, &["scheduler"], &db.url, &scratch, &[]);
    let mut server = ServerProc::spawn("server", &binary, &["server"], &db.url, &scratch, &[]);
    scheduler.wait_ready(Duration::from_secs(60)).await;
    server.wait_ready(Duration::from_secs(60)).await;
    let client = resilient_client(&server.endpoint(), 3);
    client.create_queue().queue_name("still-works").send().await.expect("serves after rollback");
    server.terminate(Duration::from_secs(20)).await;
    scheduler.terminate(Duration::from_secs(20)).await;
    db.drop_db().await;
    finish_scratch(&scratch);
}

/// Long soak: continuous load for `QD_SOAK_SECONDS` (skipped when unset) with a disruption every
/// minute (alternating rolling restart and crash), retention-driven cleanup running throughout.
/// Verifies delivery guarantees at the end and reports memory growth of long-lived replicas.
#[tokio::test]
#[ignore]
async fn soak() {
    let Some(seconds) = std::env::var("QD_SOAK_SECONDS").ok().and_then(|s| s.parse::<u64>().ok()) else {
        log("QD_SOAK_SECONDS not set; skipping soak");
        return;
    };
    let scratch = scratch_dir("soak");
    let db = local_db().await;
    let binary = server_binary();
    let mut cluster = AppCluster::start(3, &binary, &db.url, &scratch).await;
    let started = Instant::now();
    let std_load = Workload::start(&cluster.endpoint(), WorkloadConfig::standard(&qd_e2e::qname("soak"))).await;
    let fifo_load = Workload::start(&cluster.endpoint(), WorkloadConfig::fifo(&qd_e2e::qname("soak"))).await;
    let mut round = 0usize;
    let mut rss_samples: Vec<(Duration, Vec<Option<u64>>)> = Vec::new();
    while started.elapsed() < Duration::from_secs(seconds) {
        tokio::time::sleep(Duration::from_secs(60)).await;
        rss_samples.push((started.elapsed(), cluster.replicas.iter().map(|r| r.rss_kib()).collect()));
        if round.is_multiple_of(2) {
            cluster.rolling_replace(&binary).await;
        } else {
            let i = round % cluster.replicas.len();
            let addr = cluster.replicas[i].addr();
            cluster.replicas[i].kill().await;
            tokio::time::sleep(Duration::from_secs(2)).await;
            cluster.lb.remove_backend(addr);
            let mut fresh = ServerProc::spawn(
                &format!("soak{i}-r{round}"),
                &binary,
                &["server"],
                &cluster.db_url,
                &cluster.scratch,
                &[],
            );
            fresh.wait_ready(Duration::from_secs(60)).await;
            cluster.lb.add_backend(fresh.addr());
            cluster.replicas[i] = fresh;
        }
        round += 1;
        log(format!(
            "soak: {}s elapsed, {} std / {} fifo acknowledged",
            started.elapsed().as_secs(),
            std_load.sent(),
            fifo_load.sent()
        ));
    }
    for (at, rss) in &rss_samples {
        log(format!("rss at {:>5}s: {:?} KiB", at.as_secs(), rss));
    }
    let std_report = std_load.finish(Duration::from_secs(180), started).await;
    let fifo_report = fifo_load.finish(Duration::from_secs(180), started).await;
    std_report.assert_no_loss_and_ordered();
    fifo_report.assert_no_loss_and_ordered();
    std_report.assert_no_client_errors();
    fifo_report.assert_no_client_errors();
    cluster.stop().await;
    db.drop_db().await;
    finish_scratch(&scratch);
}

/// After a disruption, a long poll on one replica must still be woken promptly by a send
/// through another (i.e. LISTEN/NOTIFY recovered, not just the periodic re-check).
async fn cross_replica_wakeup(cluster: &AppCluster) {
    let a = resilient_client(&cluster.replicas[0].endpoint(), 3);
    let b = resilient_client(&cluster.replicas[1].endpoint(), 3);
    let url = a.create_queue().queue_name(qd_e2e::qname("wake")).send().await.unwrap().queue_url.unwrap();
    let poller = {
        let b = b.clone();
        let url = url.clone();
        tokio::spawn(async move {
            let t = Instant::now();
            let out = b.receive_message().queue_url(&url).wait_time_seconds(15).send().await.unwrap();
            (out.messages.unwrap_or_default().len(), t.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    a.send_message().queue_url(&url).message_body("wake").send().await.unwrap();
    let (n, took) = poller.await.unwrap();
    assert_eq!(n, 1);
    // The engine re-checks at least every 2s even without notifications; demand better.
    assert!(took < Duration::from_millis(1500), "cross-replica wakeup took {took:?}");
    log(format!("cross-replica wakeup in {took:?}"));
}
