//! qd server: wires configuration, Postgres, the engine and the HTTP frontends.

pub mod telemetry;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use clap::{Parser, Subcommand};
use metrics_exporter_prometheus::PrometheusHandle;
use qd_engine::{Engine, EngineConfig};
use qd_sqs::{AuthMode, Authenticator, Credential, SqsConfig, SqsState};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// What a process does. Production runs one `migrate` Job, then `scheduler` and `server`
/// deployments in parallel; with no command everything runs in one process (development).
#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// API, scheduler and (if QD_RUN_MIGRATIONS) migrations in one process. The default.
    All,
    /// The SQS API only. Waits for the schema to be current; never migrates.
    Server,
    /// Background maintenance only (retention, cleanup, redrive, queue metrics, heartbeat).
    /// Waits for the schema to be current; never migrates. Safe to run with several replicas.
    Scheduler,
    /// Wait for the database, apply migrations, exit.
    Migrate,
    /// Exit once every migration of this build is applied (for init containers).
    WaitForSchema {
        /// Give up after this many seconds (0 = wait forever).
        #[arg(long, default_value_t = 0)]
        timeout_secs: u64,
    },
}

/// The long-running roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    All,
    Server,
    Scheduler,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::All => "all",
            Role::Server => "server",
            Role::Scheduler => "scheduler",
        }
    }

    fn serves_api(self) -> bool {
        matches!(self, Role::All | Role::Server)
    }

    fn runs_scheduler(self) -> bool {
        matches!(self, Role::All | Role::Scheduler)
    }
}

/// Command-line / environment configuration. Every flag can be set through `QD_*` env vars.
#[derive(Debug, Clone, Parser)]
#[command(name = "qd", version, about = "Highly-available, Postgres-backed, AWS SQS-compatible message queue")]
pub struct Config {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Address the SQS API listens on.
    #[arg(long, env = "QD_BIND", default_value = "0.0.0.0:9324")]
    pub bind: SocketAddr,

    /// Address for health checks and Prometheus metrics.
    #[arg(long, env = "QD_ADMIN_BIND", default_value = "0.0.0.0:9325")]
    pub admin_bind: SocketAddr,

    /// Postgres connection string.
    #[arg(long, env = "QD_DATABASE_URL", hide_env_values = true)]
    pub database_url: String,

    #[arg(long, env = "QD_DB_MAX_CONNECTIONS", default_value_t = 32)]
    pub db_max_connections: u32,

    #[arg(long, env = "QD_DB_MIN_CONNECTIONS", default_value_t = 2)]
    pub db_min_connections: u32,

    #[arg(long, env = "QD_DB_ACQUIRE_TIMEOUT_SECS", default_value_t = 5)]
    pub db_acquire_timeout_secs: u64,

    /// Server-side statement timeout applied to every connection.
    #[arg(long, env = "QD_DB_STATEMENT_TIMEOUT_MS", default_value_t = 30_000)]
    pub db_statement_timeout_ms: u64,

    /// In the all-in-one role, apply migrations at startup (advisory-locked). The `server` and
    /// `scheduler` roles never migrate; run `qd migrate` first.
    #[arg(long, env = "QD_RUN_MIGRATIONS", default_value_t = true, action = clap::ArgAction::Set)]
    pub run_migrations: bool,

    /// How long `migrate` waits for the database to become reachable.
    #[arg(long, env = "QD_MIGRATE_TIMEOUT_SECS", default_value_t = 600)]
    pub migrate_timeout_secs: u64,

    /// Region used in queue ARNs.
    #[arg(long, env = "QD_REGION", default_value = "us-east-1")]
    pub region: String,

    /// Account id for unauthenticated callers / credentials without an explicit account.
    #[arg(long, env = "QD_DEFAULT_ACCOUNT_ID", default_value = "000000000000")]
    pub default_account_id: String,

    /// Base URL advertised in queue URLs (default: derived from the request Host header).
    #[arg(long, env = "QD_PUBLIC_URL")]
    pub public_url: Option<String>,

    /// `none` accepts any (or no) signature; `sigv4` verifies AWS Signature V4.
    #[arg(long, env = "QD_AUTH_MODE", default_value = "none")]
    pub auth_mode: String,

    /// Comma-separated `ACCESS_KEY_ID:SECRET[:ACCOUNT_ID]` credentials.
    #[arg(long, env = "QD_CREDENTIALS", hide_env_values = true)]
    pub credentials: Option<String>,

    /// File with one `ACCESS_KEY_ID:SECRET[:ACCOUNT_ID]` per line (e.g. a mounted Secret).
    #[arg(long, env = "QD_CREDENTIALS_FILE")]
    pub credentials_file: Option<PathBuf>,

    /// Allow callers to address queues owned by other accounts.
    #[arg(long, env = "QD_ALLOW_CROSS_ACCOUNT", default_value_t = false, action = clap::ArgAction::Set)]
    pub allow_cross_account: bool,

    /// How long a replica may cache queue metadata.
    #[arg(long, env = "QD_QUEUE_CACHE_TTL_MS", default_value_t = 1_000)]
    pub queue_cache_ttl_ms: u64,

    /// Minimum seconds between PurgeQueue calls on one queue (AWS uses 60).
    #[arg(long, env = "QD_PURGE_COOLDOWN_SECS", default_value_t = 60)]
    pub purge_cooldown_secs: u64,

    #[arg(long, env = "QD_JANITOR_INTERVAL_SECS", default_value_t = 10)]
    pub janitor_interval_secs: u64,

    /// Interval for per-queue depth gauges (0 disables them).
    #[arg(long, env = "QD_QUEUE_METRICS_INTERVAL_SECS", default_value_t = 30)]
    pub queue_metrics_interval_secs: u64,

    /// Maximum time to wait for in-flight requests on shutdown.
    #[arg(long, env = "QD_SHUTDOWN_GRACE_SECS", default_value_t = 25)]
    pub shutdown_grace_secs: u64,

    #[arg(long, env = "QD_MAX_BODY_BYTES", default_value_t = 8 * 1024 * 1024)]
    pub max_body_bytes: usize,

    /// `text` or `json`.
    #[arg(long, env = "QD_LOG_FORMAT", default_value = "text")]
    pub log_format: String,

    /// Push metrics to this Prometheus Pushgateway (e.g. http://pushgateway:9091).
    #[arg(long, env = "QD_METRICS_PUSHGATEWAY_URL")]
    pub metrics_pushgateway_url: Option<String>,

    /// Pushgateway push interval.
    #[arg(long, env = "QD_METRICS_PUSH_INTERVAL_SECS", default_value_t = 15)]
    pub metrics_push_interval_secs: u64,

    /// Pushgateway job label.
    #[arg(long, env = "QD_METRICS_PUSH_JOB", default_value = "qd")]
    pub metrics_push_job: String,

    /// Pushgateway instance label (default: $HOSTNAME, i.e. the pod name).
    #[arg(long, env = "QD_METRICS_INSTANCE")]
    pub metrics_instance: Option<String>,

    /// Also send metrics to a DogStatsD agent at this address (e.g. 127.0.0.1:8125).
    #[arg(long, env = "QD_METRICS_DOGSTATSD_ADDR")]
    pub metrics_dogstatsd_addr: Option<String>,

    /// Prefix for DogStatsD metric names.
    #[arg(long, env = "QD_METRICS_DOGSTATSD_PREFIX")]
    pub metrics_dogstatsd_prefix: Option<String>,
}

impl Config {
    /// A config suitable for tests: ephemeral ports and fast background loops.
    pub fn for_tests(database_url: impl Into<String>) -> Self {
        let mut c = Config::parse_from(["qd", "--database-url", "postgres://unused"]);
        c.database_url = database_url.into();
        c.bind = "127.0.0.1:0".parse().unwrap();
        c.admin_bind = "127.0.0.1:0".parse().unwrap();
        c.db_max_connections = 16;
        c.db_min_connections = 0;
        c.queue_metrics_interval_secs = 0;
        c.janitor_interval_secs = 1;
        c
    }

    fn load_credentials(&self) -> anyhow::Result<Vec<Credential>> {
        let mut specs: Vec<String> = Vec::new();
        if let Some(list) = &self.credentials {
            specs.extend(list.split(',').map(str::to_string));
        }
        if let Some(path) = &self.credentials_file {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("reading credentials file {}", path.display()))?;
            specs.extend(content.lines().map(str::to_string));
        }
        specs
            .iter()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && !s.starts_with('#'))
            .map(|s| Credential::parse(s, &self.default_account_id).map_err(anyhow::Error::msg))
            .collect()
    }
}

/// A running server instance.
pub struct Server {
    pub role: Role,
    /// SQS listener address (`None` for the scheduler role).
    pub sqs_addr: Option<SocketAddr>,
    pub admin_addr: SocketAddr,
    pub engine: Engine,
    shutdown: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    grace: Duration,
}

impl Server {
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.sqs_addr.expect("this role serves the SQS API"))
    }

    /// Triggers graceful shutdown and waits for it to complete.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.engine.shutdown();
        let wait = futures_join(self.tasks);
        if tokio::time::timeout(self.grace + Duration::from_secs(5), wait).await.is_err() {
            tracing::warn!("shutdown grace period exceeded");
        }
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Waits until all server tasks exit (after a shutdown was requested elsewhere).
    pub async fn wait(self) {
        futures_join(self.tasks).await;
    }
}

async fn futures_join(tasks: Vec<JoinHandle<()>>) {
    for t in tasks {
        let _ = t.await;
    }
}

#[derive(Clone)]
struct AdminState {
    engine: Engine,
    metrics: PrometheusHandle,
    /// Set once the role has finished starting (schema current, work running).
    started: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
}

async fn healthz() -> impl IntoResponse {
    "ok"
}

async fn readyz(State(state): State<AdminState>) -> impl IntoResponse {
    if state.shutting_down.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "shutting down");
    }
    if !state.started.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "starting: waiting for database schema");
    }
    match tokio::time::timeout(Duration::from_secs(2), state.engine.ping()).await {
        Ok(Ok(())) => (StatusCode::OK, "ready"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "database unavailable"),
    }
}

async fn metrics_endpoint(State(state): State<AdminState>) -> impl IntoResponse {
    state.metrics.render()
}

fn connect_options(config: &Config) -> anyhow::Result<PgConnectOptions> {
    Ok(PgConnectOptions::from_str(&config.database_url)
        .context("invalid database URL")?
        .application_name("qd")
        .options([("statement_timeout", format!("{}ms", config.db_statement_timeout_ms))]))
}

fn pool_options(config: &Config) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(config.db_max_connections)
        .min_connections(config.db_min_connections)
        .acquire_timeout(Duration::from_secs(config.db_acquire_timeout_secs))
        .test_before_acquire(true)
}

/// Connects eagerly (fails if the database is unreachable).
pub async fn connect(config: &Config) -> anyhow::Result<sqlx::PgPool> {
    pool_options(config).connect_with(connect_options(config)?).await.context("connecting to Postgres")
}

/// A pool that connects on demand, so a process can start while Postgres is still down.
fn lazy_pool(config: &Config) -> anyhow::Result<sqlx::PgPool> {
    Ok(pool_options(config).connect_lazy_with(connect_options(config)?))
}

/// Retries `op` with backoff until it succeeds, `deadline` passes, or `cancel` fires.
async fn retry<T, F, Fut>(
    what: &str,
    deadline: Option<Instant>,
    cancel: &CancellationToken,
    mut op: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Option<T>>>,
{
    let mut delay = Duration::from_millis(500);
    let mut last_log = Instant::now() - Duration::from_secs(60);
    loop {
        let reason = match op().await {
            Ok(Some(v)) => return Ok(v),
            Ok(None) => "not yet".to_string(),
            Err(e) => format!("{e:#}"),
        };
        if deadline.is_some_and(|d| Instant::now() >= d) {
            bail!("timed out waiting for {what}: {reason}");
        }
        if last_log.elapsed() >= Duration::from_secs(10) {
            tracing::info!(%reason, "waiting for {what}");
            last_log = Instant::now();
        }
        tokio::select! {
            _ = cancel.cancelled() => bail!("cancelled while waiting for {what}"),
            _ = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(Duration::from_secs(5));
    }
}

/// Waits until every migration embedded in this build is applied.
pub async fn wait_for_schema(
    pool: &sqlx::PgPool,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let deadline = timeout.map(|t| Instant::now() + t);
    retry("the database schema (run `qd migrate`)", deadline, cancel, || async {
        Ok(qd_engine::schema_is_current(pool).await?.then_some(()))
    })
    .await?;
    tracing::info!("database schema is current");
    Ok(())
}

/// The `migrate` command: waits for the database, applies migrations, returns.
pub async fn migrate(config: &Config) -> anyhow::Result<()> {
    let pool = lazy_pool(config)?;
    let deadline = Some(Instant::now() + Duration::from_secs(config.migrate_timeout_secs));
    let cancel = CancellationToken::new();
    // Retried as a whole: each migration is transactional, so a failover mid-run is safe.
    retry("the database to apply migrations", deadline, &cancel, || async { run_migrations(&pool).await.map(Some) })
        .await?;
    tracing::info!(migrations = qd_engine::MIGRATOR.iter().count(), "migrations applied");
    pool.close().await;
    Ok(())
}

/// The `wait-for-schema` command.
pub async fn wait_for_schema_command(config: &Config, timeout_secs: u64) -> anyhow::Result<()> {
    let pool = lazy_pool(config)?;
    let timeout = (timeout_secs > 0).then(|| Duration::from_secs(timeout_secs));
    wait_for_schema(&pool, timeout, &CancellationToken::new()).await?;
    pool.close().await;
    Ok(())
}

/// Starts the all-in-one role (used by tests and development).
pub async fn start(config: Config) -> anyhow::Result<Server> {
    start_role(config, Role::All).await
}

/// Starts a long-running role. The admin endpoints come up immediately (`/healthz` live,
/// `/readyz` not ready); the role then waits for the database and schema — riding out
/// outages and an in-progress migration — before starting its work.
pub async fn start_role(config: Config, role: Role) -> anyhow::Result<Server> {
    let auth_mode = AuthMode::from_str(&config.auth_mode).map_err(anyhow::Error::msg)?;
    let credentials = config.load_credentials()?;
    if role.serves_api() && auth_mode == AuthMode::SigV4 && credentials.is_empty() {
        bail!("QD_AUTH_MODE=sigv4 requires at least one credential (QD_CREDENTIALS or QD_CREDENTIALS_FILE)");
    }

    // Install the recorder before anything emits metrics.
    let metrics = telemetry::install(&telemetry::TelemetryConfig {
        dogstatsd_addr: config.metrics_dogstatsd_addr.clone(),
        dogstatsd_prefix: config.metrics_dogstatsd_prefix.clone(),
        gauge_idle_timeout: Duration::from_secs((config.queue_metrics_interval_secs * 4).max(120)),
    })?;
    let pool = lazy_pool(&config)?;
    let engine = Engine::new(
        pool.clone(),
        EngineConfig {
            region: config.region.clone(),
            queue_cache_ttl: Duration::from_millis(config.queue_cache_ttl_ms),
            purge_cooldown: Duration::from_secs(config.purge_cooldown_secs),
            janitor_interval: Duration::from_secs(config.janitor_interval_secs.max(1)),
            ..EngineConfig::default()
        },
    );

    let shutdown = CancellationToken::new();
    let shutting_down = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();

    // Admin endpoints first, so probes see "alive, not ready" while we wait.
    let admin_app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_endpoint))
        .with_state(AdminState {
            engine: engine.clone(),
            metrics: metrics.clone(),
            started: started.clone(),
            shutting_down: shutting_down.clone(),
        });
    let admin_listener =
        TcpListener::bind(config.admin_bind).await.with_context(|| format!("binding {}", config.admin_bind))?;
    let admin_addr = admin_listener.local_addr()?;
    let token = shutdown.clone();
    tasks.push(tokio::spawn(async move {
        if let Err(err) =
            axum::serve(admin_listener, admin_app).with_graceful_shutdown(async move { token.cancelled().await }).await
        {
            tracing::error!(error = %err, "admin listener failed");
        }
    }));
    tasks.push(tokio::spawn(telemetry::runtime_collector(
        engine.clone(),
        role,
        config.db_max_connections,
        started.clone(),
        shutting_down.clone(),
        shutdown.clone(),
    )));
    if let Some(url) = &config.metrics_pushgateway_url {
        let instance = config
            .metrics_instance
            .clone()
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_else(|| admin_addr.to_string().replace(':', "_"));
        tasks.push(tokio::spawn(telemetry::pushgateway(
            metrics.clone(),
            url.clone(),
            config.metrics_push_job.clone(),
            instance,
            Duration::from_secs(config.metrics_push_interval_secs.max(1)),
            shutdown.clone(),
        )));
    }
    tracing::info!(role = role.as_str(), %admin_addr, "qd starting");

    // Schema gate: only the all-in-one role may migrate; the others wait for the migrate Job.
    if role == Role::All && config.run_migrations {
        let deadline = Some(Instant::now() + Duration::from_secs(config.migrate_timeout_secs));
        retry("the database to apply migrations", deadline, &shutdown, || async {
            run_migrations(&pool).await.map(Some)
        })
        .await?;
    } else {
        wait_for_schema(&pool, None, &shutdown).await?;
    }

    let instance = std::env::var("HOSTNAME").unwrap_or_else(|_| format!("{}-{}", role.as_str(), uuid_suffix()));
    match role {
        Role::All => tasks.extend(engine.start_background(&instance)),
        Role::Server => tasks.extend(engine.start_api_background()),
        Role::Scheduler => tasks.extend(engine.start_scheduler_background(&instance)),
    }
    if role.runs_scheduler() && config.queue_metrics_interval_secs > 0 {
        tasks.push(tokio::spawn(telemetry::queue_collector(
            engine.clone(),
            Duration::from_secs(config.queue_metrics_interval_secs),
            shutdown.clone(),
        )));
    }

    let mut sqs_addr = None;
    if role.serves_api() {
        let sqs_state = Arc::new(SqsState {
            engine: engine.clone(),
            auth: Authenticator::new(auth_mode, config.default_account_id.clone(), credentials),
            config: SqsConfig {
                public_url: config.public_url.clone(),
                allow_cross_account: config.allow_cross_account,
                max_body_bytes: config.max_body_bytes,
            },
        });
        let sqs_app = qd_sqs::router(sqs_state).layer(tower_http::catch_panic::CatchPanicLayer::new());
        let sqs_listener = TcpListener::bind(config.bind).await.with_context(|| format!("binding {}", config.bind))?;
        sqs_addr = Some(sqs_listener.local_addr()?);
        let token = shutdown.clone();
        let flag = shutting_down.clone();
        let engine_for_shutdown = engine.clone();
        tasks.push(tokio::spawn(async move {
            let signal = async move {
                token.cancelled().await;
                flag.store(true, Ordering::Relaxed);
                // Wake long pollers so they return promptly and connections drain.
                engine_for_shutdown.shutdown();
            };
            if let Err(err) = axum::serve(sqs_listener, sqs_app).with_graceful_shutdown(signal).await {
                tracing::error!(error = %err, "SQS listener failed");
            }
        }));
    } else {
        // Without an SQS listener, flip readiness and stop background work on shutdown here.
        let token = shutdown.clone();
        let flag = shutting_down.clone();
        let engine_for_shutdown = engine.clone();
        tasks.push(tokio::spawn(async move {
            token.cancelled().await;
            flag.store(true, Ordering::Relaxed);
            engine_for_shutdown.shutdown();
        }));
    }
    started.store(true, Ordering::Relaxed);

    tracing::info!(role = role.as_str(), sqs_addr = ?sqs_addr, %admin_addr, auth = ?auth_mode, region = %config.region, "qd started");
    Ok(Server {
        role,
        sqs_addr,
        admin_addr,
        engine,
        shutdown,
        tasks,
        grace: Duration::from_secs(config.shutdown_grace_secs),
    })
}

fn uuid_suffix() -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("{:x}", nanos ^ (std::process::id() as u128))
}

/// Applies embedded migrations.
///
/// Migrations recorded in the database but unknown to this binary are tolerated: that happens
/// when a newer release already migrated and this (older) release is rolled back to. Releases
/// therefore must keep schema changes backwards compatible (expand/contract).
pub async fn run_migrations(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    qd_engine::rollback_tolerant_migrator().run(pool).await.context("running database migrations")?;
    Ok(())
}
