//! Metrics: recorder installation, exporters and background collectors.
//!
//! Metrics are always available for scraping at `GET /metrics` on the admin port (Prometheus /
//! OpenMetrics text format, also consumed by the OpenTelemetry Collector's `prometheus`
//! receiver and the Datadog Agent's OpenMetrics check). Optionally they are also pushed:
//!
//! * to a Prometheus **Pushgateway** (`QD_METRICS_PUSHGATEWAY_URL`), and/or
//! * to a **DogStatsD** agent (`QD_METRICS_DOGSTATSD_ADDR`).

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use metrics_util::layers::FanoutBuilder;
use qd_engine::Engine;
use tokio_util::sync::CancellationToken;

/// Exporter settings; only the first server started in a process installs the recorder.
#[derive(Debug, Clone, Default)]
pub struct TelemetryConfig {
    pub dogstatsd_addr: Option<String>,
    pub dogstatsd_prefix: Option<String>,
    /// Gauges not refreshed within this window are dropped (e.g. queues that were deleted).
    pub gauge_idle_timeout: Duration,
}

struct Installed {
    handle: PrometheusHandle,
}

static INSTALLED: OnceLock<Installed> = OnceLock::new();

const LATENCY_BUCKETS: &[f64] = &[0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 21.0];
const SIZE_BUCKETS: &[f64] = &[64.0, 256.0, 1024.0, 4096.0, 16_384.0, 65_536.0, 262_144.0, 524_288.0, 1_048_576.0];

/// Installs the global metrics recorder (once per process) and returns the scrape handle.
pub fn install(config: &TelemetryConfig) -> anyhow::Result<PrometheusHandle> {
    if let Some(i) = INSTALLED.get() {
        return Ok(i.handle.clone());
    }
    let idle = if config.gauge_idle_timeout.is_zero() { None } else { Some(config.gauge_idle_timeout) };
    let prometheus = PrometheusBuilder::new()
        .set_buckets(LATENCY_BUCKETS)?
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full("qd_message_size_bytes".into()),
            SIZE_BUCKETS,
        )?
        .idle_timeout(MetricKindMask::GAUGE, idle)
        .build_recorder();
    let handle = prometheus.handle();

    let installed = match &config.dogstatsd_addr {
        Some(addr) => {
            let mut builder = metrics_exporter_dogstatsd::DogStatsDBuilder::default()
                .with_remote_address(addr.as_str())?
                .send_histograms_as_distributions(true)
                // Its self-telemetry would otherwise flow into the Prometheus scrape via fan-out.
                .with_telemetry(false);
            if let Some(prefix) = &config.dogstatsd_prefix {
                builder = builder.set_global_prefix(prefix.clone());
            }
            let dogstatsd = builder.build()?;
            let fanout = FanoutBuilder::default().add_recorder(prometheus).add_recorder(dogstatsd).build();
            metrics::set_global_recorder(fanout).is_ok()
        }
        None => metrics::set_global_recorder(prometheus).is_ok(),
    };
    if !installed {
        // Another recorder won the race (e.g. parallel tests); reuse whatever is installed.
        if let Some(i) = INSTALLED.get() {
            return Ok(i.handle.clone());
        }
    }
    let _ = INSTALLED.set(Installed { handle: handle.clone() });
    // Upkeep drains histogram buffers and expires idle gauges.
    let upkeep = handle.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            upkeep.run_upkeep();
        }
    });
    describe_all();
    // Pre-register error counters at zero so dashboards and `rate()` work before the first error.
    for kind in ["unavailable", "other"] {
        metrics::counter!("qd_db_errors_total", "kind" => kind).increment(0);
    }
    metrics::counter!("qd_scheduler_heartbeats_total").increment(0);
    for result in ["ok", "error"] {
        metrics::counter!("qd_janitor_runs_total", "result" => result).increment(0);
    }
    Ok(handle)
}

/// Metric descriptions (rendered as `# HELP`).
fn describe_all() {
    {
        use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};
        describe_counter!(
            "qd_requests_total",
            "SQS API requests by action, protocol and result code (OK or AWS error code)."
        );
        describe_histogram!(
            "qd_request_duration_seconds",
            Unit::Seconds,
            "SQS API request latency (long polls included)."
        );
        describe_counter!("qd_messages_sent_total", "Messages accepted (NumberOfMessagesSent).");
        describe_counter!("qd_messages_received_total", "Messages delivered to consumers (NumberOfMessagesReceived).");
        describe_counter!("qd_messages_deleted_total", "Messages deleted by consumers (NumberOfMessagesDeleted).");
        describe_counter!(
            "qd_empty_receives_total",
            "ReceiveMessage calls that returned nothing (NumberOfEmptyReceives)."
        );
        describe_counter!("qd_messages_dead_lettered_total", "Messages moved to a dead-letter queue, by source queue.");
        describe_counter!("qd_messages_deduplicated_total", "FIFO sends dropped as duplicates.");
        describe_counter!("qd_messages_expired_total", "Messages removed after their retention period.");
        describe_counter!("qd_messages_redriven_total", "Messages moved by message move tasks (DLQ redrive).");
        describe_histogram!("qd_message_size_bytes", Unit::Bytes, "Size of accepted messages (SentMessageSize).");
        describe_gauge!("qd_queue_messages", "Messages per queue by state: visible, in_flight, delayed.");
        describe_gauge!(
            "qd_queue_oldest_message_age_seconds",
            Unit::Seconds,
            "Age of the oldest message in the queue (ApproximateAgeOfOldestMessage)."
        );
        describe_gauge!("qd_queue_info", "Static queue information (type, is_dlq); value is always 1.");
        describe_gauge!("qd_queues", "Number of queues.");
        describe_gauge!("qd_move_tasks_running", "Running message move (redrive) tasks.");
        describe_counter!("qd_queues_created_total", "Queues created.");
        describe_counter!("qd_queues_deleted_total", "Queues deleted.");
        describe_counter!("qd_queues_purged_total", "PurgeQueue operations.");
        describe_gauge!("qd_build_info", "Build information; value is always 1.");
        describe_gauge!("qd_ready", "1 when this replica is ready to serve (database reachable, not draining).");
        describe_gauge!("qd_long_polls_in_progress", "ReceiveMessage long polls currently waiting.");
        describe_gauge!("qd_db_pool_connections", "Database pool connections by state: idle, in_use.");
        describe_gauge!("qd_db_pool_max_connections", "Configured database pool size.");
        describe_counter!("qd_db_errors_total", "Database errors by kind: unavailable, other.");
        describe_gauge!("qd_notifier_connected", "1 when the cross-replica LISTEN connection is established.");
        describe_counter!("qd_notifications_published_total", "Queue wakeup notifications sent to other replicas.");
        describe_counter!("qd_janitor_runs_total", "Background maintenance passes by result.");
        describe_histogram!("qd_janitor_duration_seconds", Unit::Seconds, "Duration of background maintenance passes.");
        describe_histogram!(
            "qd_queue_metrics_collection_seconds",
            Unit::Seconds,
            "Time spent collecting per-queue gauges."
        );
    }
}

/// Refreshes process-level gauges (readiness, pool usage, build info) every few seconds.
pub async fn runtime_collector(
    engine: Engine,
    role: crate::Role,
    max_connections: u32,
    started: std::sync::Arc<AtomicBool>,
    shutting_down: std::sync::Arc<AtomicBool>,
    shutdown: CancellationToken,
) {
    loop {
        let pool = engine.pool();
        let size = pool.size() as f64;
        let idle = pool.num_idle() as f64;
        metrics::gauge!("qd_db_pool_connections", "state" => "idle").set(idle);
        metrics::gauge!("qd_db_pool_connections", "state" => "in_use").set((size - idle).max(0.0));
        metrics::gauge!("qd_db_pool_max_connections").set(max_connections as f64);
        metrics::gauge!("qd_build_info", "version" => env!("CARGO_PKG_VERSION"), "role" => role.as_str()).set(1.0);
        let db_ok = matches!(tokio::time::timeout(Duration::from_secs(2), engine.ping()).await, Ok(Ok(())));
        let ready = db_ok && started.load(Ordering::Relaxed) && !shutting_down.load(Ordering::Relaxed);
        metrics::gauge!("qd_ready").set(if ready { 1.0 } else { 0.0 });
        if role != crate::Role::Scheduler {
            metrics::gauge!("qd_notifier_connected").set(if engine.notifier_connected() { 1.0 } else { 0.0 });
            // Touch so the series exists (and isn't idled out) even when nobody is polling.
            metrics::gauge!("qd_long_polls_in_progress").increment(0.0);
            if db_ok && started.load(Ordering::Relaxed) {
                // No heartbeat at all reads as "infinitely old", so the alert fires.
                let age = engine.scheduler_heartbeat_age().await.ok().flatten().unwrap_or(f64::INFINITY);
                metrics::gauge!("qd_scheduler_heartbeat_age_seconds").set(age);
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => {
                metrics::gauge!("qd_ready").set(0.0);
                return;
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
    }
}

/// Per-queue gauges (depth by state, oldest message age, queue info) and task counts.
pub async fn queue_collector(engine: Engine, interval: Duration, shutdown: CancellationToken) {
    loop {
        let started = Instant::now();
        if let Err(err) = collect_queues(&engine).await {
            tracing::warn!(error = %err, "collecting queue metrics failed");
        }
        metrics::histogram!("qd_queue_metrics_collection_seconds").record(started.elapsed().as_secs_f64());
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

#[derive(sqlx::FromRow)]
struct QueueStats {
    account_id: String,
    name: String,
    fifo: bool,
    is_dlq: bool,
    visible: i64,
    in_flight: i64,
    delayed: i64,
    oldest_age: Option<f64>,
}

async fn collect_queues(engine: &Engine) -> Result<(), sqlx::Error> {
    let region = engine.region().to_string();
    let rows: Vec<QueueStats> = sqlx::query_as(
        "SELECT q.account_id, q.name, q.fifo,
                EXISTS (SELECT 1 FROM qd_queues s WHERE s.dlq_target_arn =
                        'arn:aws:sqs:' || $1 || ':' || q.account_id || ':' || q.name) AS is_dlq,
                count(m.id) FILTER (WHERE m.visible_at <= now()) AS visible,
                count(m.id) FILTER (WHERE m.visible_at > now() AND m.receive_count > 0) AS in_flight,
                count(m.id) FILTER (WHERE m.visible_at > now() AND m.receive_count = 0) AS delayed,
                EXTRACT(EPOCH FROM now() - min(m.sent_at))::float8 AS oldest_age
         FROM qd_queues q
         LEFT JOIN qd_messages m ON m.queue_id = q.id AND m.expires_at > now()
         GROUP BY q.id, q.account_id, q.name, q.fifo",
    )
    .bind(&region)
    .fetch_all(engine.pool())
    .await?;
    metrics::gauge!("qd_queues").set(rows.len() as f64);
    for QueueStats { account_id: account, name, fifo, is_dlq, visible, in_flight, delayed, oldest_age: oldest } in rows
    {
        for (state, value) in [("visible", visible), ("in_flight", in_flight), ("delayed", delayed)] {
            metrics::gauge!("qd_queue_messages", "account" => account.clone(), "queue" => name.clone(), "state" => state)
                .set(value as f64);
        }
        metrics::gauge!("qd_queue_oldest_message_age_seconds", "account" => account.clone(), "queue" => name.clone())
            .set(oldest.unwrap_or(0.0).max(0.0));
        metrics::gauge!(
            "qd_queue_info",
            "account" => account,
            "queue" => name,
            "type" => if fifo { "fifo" } else { "standard" },
            "is_dlq" => if is_dlq { "true" } else { "false" }
        )
        .set(1.0);
    }
    let (running,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM qd_move_tasks WHERE status = 'RUNNING'").fetch_one(engine.pool()).await?;
    metrics::gauge!("qd_move_tasks_running").set(running as f64);
    Ok(())
}

/// Pushes the scrape output to a Prometheus Pushgateway until shutdown, then deletes the group.
pub async fn pushgateway(
    handle: PrometheusHandle,
    base_url: String,
    job: String,
    instance: String,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let url = format!("{}/metrics/job/{job}/instance/{instance}", base_url.trim_end_matches('/'));
    let client = match reqwest::Client::builder().timeout(Duration::from_secs(10)).build() {
        Ok(c) => c,
        Err(err) => {
            tracing::error!(error = %err, "cannot build pushgateway client");
            return;
        }
    };
    loop {
        let body = handle.render();
        match client.put(&url).header("content-type", "text/plain; version=0.0.4").body(body).send().await {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => tracing::warn!(status = %r.status(), "pushgateway rejected metrics"),
            Err(err) => tracing::warn!(error = %err, "pushing metrics failed"),
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }
    }
    // Remove this instance's group so a terminated pod doesn't linger as stale data.
    let _ = client.delete(&url).send().await;
}
