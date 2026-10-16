//! Scheduler heartbeats: each scheduler records that it is alive, so API replicas (and alerts)
//! can tell whether background maintenance is running.

use std::time::Duration;

use super::Engine;
use crate::error::Result;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

pub(super) async fn run(engine: Engine, instance: String) {
    let shutdown = engine.shutdown_token();
    loop {
        match engine.beat(&instance).await {
            Ok(()) => metrics::counter!("qd_scheduler_heartbeats_total").increment(1),
            Err(err) => tracing::warn!(error = %err, "scheduler heartbeat failed"),
        }
        tokio::select! {
            _ = shutdown.cancelled() => {
                // Leave promptly so the remaining schedulers' heartbeats are what counts.
                let _ = sqlx::query("DELETE FROM qd_scheduler_heartbeats WHERE instance = $1")
                    .bind(&instance)
                    .execute(engine.pool())
                    .await;
                return;
            }
            _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {}
        }
    }
}

impl Engine {
    async fn beat(&self, instance: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO qd_scheduler_heartbeats (instance, version, last_seen) VALUES ($1, $2, now())
             ON CONFLICT (instance) DO UPDATE SET last_seen = now(), version = EXCLUDED.version",
        )
        .bind(instance)
        .bind(env!("CARGO_PKG_VERSION"))
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// Seconds since the freshest scheduler heartbeat, or `None` if no scheduler ever beat.
    pub async fn scheduler_heartbeat_age(&self) -> Result<Option<f64>> {
        Ok(sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM now() - max(last_seen))::float8 FROM qd_scheduler_heartbeats")
            .fetch_one(self.pool())
            .await?)
    }
}
