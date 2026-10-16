//! Background housekeeping. Runs on every replica; all steps are idempotent and use
//! `SKIP LOCKED` batches, so concurrent janitors cooperate instead of conflicting.

use std::time::{Duration, Instant};

use super::Engine;
use crate::error::Result;

const BATCH: i64 = 5_000;
/// Per-step time budget so one huge backlog doesn't starve the other steps.
const STEP_BUDGET: Duration = Duration::from_secs(5);

pub(super) async fn run(engine: Engine) {
    let shutdown = engine.shutdown_token();
    let interval = engine.config().janitor_interval;
    // Spread replicas out so they don't all fire at the same instant.
    let jitter = Duration::from_millis(rand_jitter_ms(interval));
    tokio::select! {
        _ = shutdown.cancelled() => return,
        _ = tokio::time::sleep(jitter) => {}
    }
    loop {
        let started = std::time::Instant::now();
        let result = engine.janitor_pass().await;
        metrics::histogram!("qd_janitor_duration_seconds").record(started.elapsed().as_secs_f64());
        match result {
            Ok(()) => metrics::counter!("qd_janitor_runs_total", "result" => "ok").increment(1),
            Err(err) => {
                metrics::counter!("qd_janitor_runs_total", "result" => "error").increment(1);
                tracing::warn!(error = %err, "janitor pass failed");
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

fn rand_jitter_ms(interval: Duration) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % (interval.as_millis() as u64).max(1)
}

impl Engine {
    /// One janitor pass. Public so tests and operators can force housekeeping.
    pub async fn janitor_pass(&self) -> Result<()> {
        let expired = self
            .delete_in_batches(
                "DELETE FROM qd_messages WHERE id IN (
                    SELECT id FROM qd_messages WHERE expires_at <= now() LIMIT $1 FOR UPDATE SKIP LOCKED)",
            )
            .await?;
        if expired > 0 {
            metrics::counter!("qd_messages_expired_total").increment(expired);
            tracing::debug!(expired, "removed messages past their retention period");
        }
        self.reclaim_deleted_queues().await?;
        self.delete_in_batches(
            "DELETE FROM qd_fifo_dedup WHERE ctid IN (
                SELECT ctid FROM qd_fifo_dedup WHERE expires_at <= now() LIMIT $1 FOR UPDATE SKIP LOCKED)",
        )
        .await?;
        self.delete_in_batches(
            "DELETE FROM qd_receive_attempts WHERE ctid IN (
                SELECT ctid FROM qd_receive_attempts WHERE created_at <= now() - interval '5 minutes'
                LIMIT $1 FOR UPDATE SKIP LOCKED)",
        )
        .await?;
        // Idle, empty FIFO groups. The last_send_at guard (bumped under row lock by senders)
        // prevents deleting a group a concurrent send is about to populate.
        self.delete_in_batches(
            "DELETE FROM qd_fifo_groups WHERE ctid IN (
                SELECT g.ctid FROM qd_fifo_groups g
                WHERE g.last_send_at < now() - interval '5 minutes'
                  AND NOT EXISTS (SELECT 1 FROM qd_messages m WHERE m.queue_id = g.queue_id AND m.group_id = g.group_id)
                LIMIT $1 FOR UPDATE SKIP LOCKED)",
        )
        .await?;
        sqlx::query("DELETE FROM qd_scheduler_heartbeats WHERE last_seen < now() - interval '1 day'")
            .execute(self.pool())
            .await?;
        sqlx::query("DELETE FROM qd_move_tasks WHERE status NOT IN ('RUNNING', 'CANCELLING') AND updated_at < now() - interval '14 days'")
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn delete_in_batches(&self, sql: &str) -> Result<u64> {
        let started = Instant::now();
        let mut total = 0;
        loop {
            let n = sqlx::query(sql).bind(BATCH).execute(self.pool()).await?.rows_affected();
            total += n;
            if (n as i64) < BATCH || started.elapsed() > STEP_BUDGET {
                return Ok(total);
            }
        }
    }

    async fn reclaim_deleted_queues(&self) -> Result<()> {
        let queues: Vec<(i64,)> =
            sqlx::query_as("SELECT queue_id FROM qd_deleted_queues ORDER BY deleted_at LIMIT 100")
                .fetch_all(self.pool())
                .await?;
        for (queue_id,) in queues {
            let started = Instant::now();
            loop {
                let n = sqlx::query(
                    "DELETE FROM qd_messages WHERE id IN (
                        SELECT id FROM qd_messages WHERE queue_id = $1 LIMIT $2 FOR UPDATE SKIP LOCKED)",
                )
                .bind(queue_id)
                .bind(BATCH)
                .execute(self.pool())
                .await?
                .rows_affected();
                if (n as i64) < BATCH || started.elapsed() > STEP_BUDGET {
                    break;
                }
            }
            for table in ["qd_fifo_groups", "qd_fifo_dedup", "qd_receive_attempts"] {
                sqlx::query(&format!("DELETE FROM {table} WHERE queue_id = $1"))
                    .bind(queue_id)
                    .execute(self.pool())
                    .await?;
            }
            sqlx::query("DELETE FROM qd_move_tasks WHERE source_queue_id = $1")
                .bind(queue_id)
                .execute(self.pool())
                .await?;
            // Keep the tombstone for a while: replicas with a stale queue cache may still insert
            // a few messages for the deleted queue, and those must be reclaimed too.
            sqlx::query(
                "DELETE FROM qd_deleted_queues WHERE queue_id = $1 AND deleted_at < now() - interval '5 minutes'
                 AND NOT EXISTS (SELECT 1 FROM qd_messages WHERE queue_id = $1)",
            )
            .bind(queue_id)
            .execute(self.pool())
            .await?;
        }
        Ok(())
    }
}
