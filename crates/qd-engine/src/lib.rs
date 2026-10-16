//! qd engine.
//!
//! The engine implements SQS queue semantics (standard and FIFO queues, visibility timeouts,
//! delays, dead-letter queues, redrive, retention) on top of PostgreSQL. It knows nothing about
//! wire protocols: frontends such as the SQS JSON/Query API translate requests into engine calls,
//! which keeps the door open for additional protocols later.
//!
//! Every replica is stateless; all coordination happens in Postgres (row locks with
//! `SKIP LOCKED`, advisory locks for FIFO ordering, and `LISTEN/NOTIFY` for long-poll wakeups).

pub mod attributes;
pub mod engine;
pub mod error;
pub mod message;
pub mod names;
pub mod notify;
pub mod queue;
pub mod receipt;

pub use engine::{Engine, EngineConfig};
pub use error::{ErrorCode, Result, SqsError};

/// Embedded SQL migrations (strict: fails if the database has migrations this build lacks).
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Migrator used by servers: tolerates migrations applied by a *newer* release so that an older
/// release can be rolled back to. Schema changes must therefore be backwards compatible.
/// Whether every migration embedded in this build has been applied successfully. Extra
/// migrations from newer releases are fine (see [`rollback_tolerant_migrator`]).
pub async fn schema_is_current(pool: &sqlx::PgPool) -> Result<bool, sqlx::Error> {
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public._sqlx_migrations') IS NOT NULL").fetch_one(pool).await?;
    if !exists {
        return Ok(false);
    }
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations WHERE success").fetch_all(pool).await?;
    Ok(MIGRATOR.iter().all(|m| applied.contains(&m.version)))
}

pub fn rollback_tolerant_migrator() -> sqlx::migrate::Migrator {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
}

/// Identity of the caller as established by the protocol frontend (authentication).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub account_id: String,
    /// Reported as the `SenderId` system attribute.
    pub sender_id: String,
}

impl Caller {
    pub fn new(account_id: impl Into<String>, sender_id: impl Into<String>) -> Self {
        Self { account_id: account_id.into(), sender_id: sender_id.into() }
    }
}
