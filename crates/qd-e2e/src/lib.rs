//! Test harness: boots real qd servers on throwaway databases and hands out official
//! AWS SDK clients pointed at them.
//!
//! `DATABASE_URL` must point at a Postgres the tests may create databases on (default
//! `postgres://qd:qd@localhost/postgres`).

use std::time::Duration;

use aws_credential_types::Credentials;
use aws_sdk_sqs::config::retry::RetryConfig;
use aws_sdk_sqs::config::{BehaviorVersion, Region};
use qd::{Config, Server};
use sqlx::Connection;
use sqlx::postgres::PgConnection;

pub mod chaos;

pub use aws_sdk_sqs;

pub fn admin_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://qd:qd@localhost/postgres".into())
}

fn with_database(url: &str, db: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let base = match base.rfind('/') {
        Some(idx) if base[..idx].contains("://") && idx > base.find("://").unwrap() + 2 => &base[..idx],
        _ => base,
    };
    match query {
        Some(q) => format!("{base}/{db}?{q}"),
        None => format!("{base}/{db}"),
    }
}

/// A throwaway database, dropped by [`TestDb::drop_db`].
pub struct TestDb {
    pub name: String,
    pub url: String,
}

impl TestDb {
    pub async fn create() -> Self {
        let name = format!("qd_e2e_{}", uuid::Uuid::new_v4().simple());
        let mut conn = PgConnection::connect(&admin_url()).await.expect("connect to admin database");
        sqlx::query(&format!("CREATE DATABASE {name}")).execute(&mut conn).await.expect("create test database");
        let url = with_database(&admin_url(), &name);
        Self { name, url }
    }

    pub async fn drop_db(&self) {
        if let Ok(mut conn) = PgConnection::connect(&admin_url()).await {
            let _ =
                sqlx::query(&format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name)).execute(&mut conn).await;
        }
    }
}

/// Options for a test server.
#[derive(Clone)]
pub struct Options {
    pub auth_mode: String,
    pub credentials: Option<String>,
    pub purge_cooldown_secs: u64,
    pub queue_cache_ttl_ms: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self { auth_mode: "none".into(), credentials: None, purge_cooldown_secs: 0, queue_cache_ttl_ms: 1000 }
    }
}

pub async fn start_server(db_url: &str, opts: &Options) -> Server {
    let mut config = Config::for_tests(db_url);
    config.db_max_connections = 6;
    config.auth_mode = opts.auth_mode.clone();
    config.credentials = opts.credentials.clone();
    config.purge_cooldown_secs = opts.purge_cooldown_secs;
    config.queue_cache_ttl_ms = opts.queue_cache_ttl_ms;
    config.shutdown_grace_secs = 2;
    qd::start(config).await.expect("server starts")
}

/// One database with one or more server replicas.
pub struct Harness {
    pub db: TestDb,
    pub servers: Vec<Server>,
}

impl Harness {
    pub async fn start() -> Self {
        Self::start_with(1, Options::default()).await
    }

    pub async fn start_with(replicas: usize, opts: Options) -> Self {
        let db = TestDb::create().await;
        let mut servers = Vec::new();
        for _ in 0..replicas {
            servers.push(start_server(&db.url, &opts).await);
        }
        Self { db, servers }
    }

    pub fn endpoint(&self) -> String {
        self.servers[0].endpoint()
    }

    /// SDK client for replica 0 with throwaway credentials.
    pub async fn client(&self) -> aws_sdk_sqs::Client {
        client_for(&self.servers[0].endpoint(), "AKIDTEST", "secret").await
    }

    pub async fn client_for_replica(&self, i: usize) -> aws_sdk_sqs::Client {
        client_for(&self.servers[i].endpoint(), "AKIDTEST", "secret").await
    }

    pub async fn stop(self) {
        for s in self.servers {
            s.shutdown().await;
        }
        self.db.drop_db().await;
    }
}

pub async fn client_for(endpoint: &str, akid: &str, secret: &str) -> aws_sdk_sqs::Client {
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(akid, secret, None, None, "e2e"))
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            aws_sdk_sqs::config::timeout::TimeoutConfig::builder().operation_timeout(Duration::from_secs(40)).build(),
        )
        .build();
    aws_sdk_sqs::Client::from_conf(config)
}

/// Unique queue name for a test.
pub fn qname(prefix: &str) -> String {
    format!("{prefix}-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
}

#[cfg(test)]
mod tests {
    use super::with_database;

    #[test]
    fn database_url_rewrite() {
        assert_eq!(with_database("postgres://u:p@h/postgres", "x"), "postgres://u:p@h/x");
        assert_eq!(
            with_database("postgres://u:p@h:5432/db?sslmode=disable", "x"),
            "postgres://u:p@h:5432/x?sslmode=disable"
        );
        assert_eq!(with_database("postgres://u:p@h", "x"), "postgres://u:p@h/x");
    }
}
