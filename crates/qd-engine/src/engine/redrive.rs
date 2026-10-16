//! Dead-letter queue redrive: StartMessageMoveTask, CancelMessageMoveTask, ListMessageMoveTasks.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Utc};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::receive::touch_groups;
use super::{Engine, millis};
use crate::error::{ErrorCode, Result, SqsError};
use crate::queue::Queue;

const DEFAULT_RATE: i64 = 500;
const MAX_RATE: i64 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveTaskStatus {
    Running,
    Completed,
    Cancelling,
    Cancelled,
    Failed,
}

impl MoveTaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Cancelling => "CANCELLING",
            Self::Cancelled => "CANCELLED",
            Self::Failed => "FAILED",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "RUNNING" => Self::Running,
            "COMPLETED" => Self::Completed,
            "CANCELLING" => Self::Cancelling,
            "CANCELLED" => Self::Cancelled,
            _ => Self::Failed,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MoveTask {
    /// Only present while the task is running.
    pub task_handle: Option<String>,
    pub status: MoveTaskStatus,
    pub source_arn: String,
    pub destination_arn: Option<String>,
    pub max_number_of_messages_per_second: Option<i64>,
    pub approximate_number_of_messages_moved: i64,
    pub approximate_number_of_messages_to_move: Option<i64>,
    pub failure_reason: Option<String>,
    pub started_timestamp: i64,
}

#[derive(sqlx::FromRow)]
struct TaskRow {
    id: Uuid,
    source_queue_id: i64,
    source_arn: String,
    destination_arn: Option<String>,
    max_per_second: Option<i32>,
    status: String,
    moved: i64,
    to_move: i64,
    failure_reason: Option<String>,
    cutoff_id: i64,
    started_at: DateTime<Utc>,
}

const TASK_COLUMNS: &str = "id, source_queue_id, source_arn, destination_arn, max_per_second, status, moved, \
     to_move, failure_reason, cutoff_id, started_at";

fn encode_handle(id: Uuid, source_arn: &str) -> String {
    STANDARD.encode(serde_json::json!({"taskId": id.to_string(), "sourceArn": source_arn}).to_string())
}

fn decode_handle(handle: &str) -> Result<Uuid> {
    let invalid = || SqsError::invalid_value("TaskHandle", handle, "Task handle is invalid.");
    let raw = STANDARD.decode(handle.trim()).map_err(|_| invalid())?;
    let value: serde_json::Value = serde_json::from_slice(&raw).map_err(|_| invalid())?;
    value.get("taskId").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok()).ok_or_else(invalid)
}

impl TaskRow {
    fn into_task(self) -> MoveTask {
        let status = MoveTaskStatus::parse(&self.status);
        MoveTask {
            task_handle: (status == MoveTaskStatus::Running).then(|| encode_handle(self.id, &self.source_arn)),
            status,
            source_arn: self.source_arn,
            destination_arn: self.destination_arn,
            max_number_of_messages_per_second: self.max_per_second.map(i64::from),
            approximate_number_of_messages_moved: self.moved,
            approximate_number_of_messages_to_move: Some(self.to_move),
            failure_reason: self.failure_reason,
            started_timestamp: millis(self.started_at),
        }
    }
}

impl Engine {
    pub async fn start_message_move_task(
        &self,
        source_arn: &str,
        destination_arn: Option<&str>,
        max_per_second: Option<i64>,
    ) -> Result<String> {
        if let Some(rate) = max_per_second
            && !(1..=MAX_RATE).contains(&rate)
        {
            return Err(SqsError::invalid_value(
                "MaxNumberOfMessagesPerSecond",
                rate,
                "MaxNumberOfMessagesPerSecond must be an integer between 1 and 500.",
            ));
        }
        let source = self.queue_by_arn(source_arn).await.map_err(|e| match e.code {
            ErrorCode::QueueDoesNotExist => SqsError::new(
                ErrorCode::ResourceNotFound,
                "The resource that you specified for the SourceArn parameter doesn't exist.",
            ),
            _ => e,
        })?;
        let is_dlq: Option<(i64,)> = sqlx::query_as("SELECT id FROM qd_queues WHERE dlq_target_arn = $1 LIMIT 1")
            .bind(&source.arn)
            .fetch_optional(self.pool())
            .await?;
        if is_dlq.is_none() {
            return Err(SqsError::invalid_parameter("Source queue must be configured as a Dead Letter Queue."));
        }
        if let Some(dest_arn) = destination_arn {
            let dest = self.queue_by_arn(dest_arn).await.map_err(|e| match e.code {
                ErrorCode::QueueDoesNotExist => SqsError::new(
                    ErrorCode::ResourceNotFound,
                    "The resource that you specified for the DestinationArn parameter doesn't exist.",
                ),
                _ => e,
            })?;
            if dest.fifo != source.fifo {
                return Err(SqsError::invalid_parameter(
                    "Source queue and destination queue must be of the same queue type.",
                ));
            }
        }

        let id = Uuid::new_v4();
        let inserted = sqlx::query(
            "INSERT INTO qd_move_tasks (id, source_queue_id, source_arn, destination_arn, max_per_second, status, to_move, cutoff_id, updated_at)
             SELECT $1, $2, $3, $4, $5, 'RUNNING', count(*), COALESCE(max(id), 0), now() - interval '1 hour'
             FROM qd_messages WHERE queue_id = $2 AND expires_at > now()
             ON CONFLICT DO NOTHING",
        )
        .bind(id)
        .bind(source.id)
        .bind(&source.arn)
        .bind(destination_arn)
        .bind(max_per_second.map(|r| r as i32))
        .execute(self.pool())
        .await?;
        if inserted.rows_affected() == 0 {
            return Err(SqsError::new(
                ErrorCode::UnsupportedOperation,
                "There is already a task running. Only one active task is allowed for a source queue arn at a given time.",
            ));
        }
        Ok(encode_handle(id, &source.arn))
    }

    /// Cancels a running task; returns the number of messages moved so far.
    pub async fn cancel_message_move_task(&self, task_handle: &str) -> Result<i64> {
        let id = decode_handle(task_handle)?;
        let moved: Option<(i64,)> = sqlx::query_as(
            "UPDATE qd_move_tasks SET status = 'CANCELLED', updated_at = now()
             WHERE id = $1 AND status IN ('RUNNING', 'CANCELLING') RETURNING moved",
        )
        .bind(id)
        .fetch_optional(self.pool())
        .await?;
        moved
            .map(|(m,)| m)
            .ok_or_else(|| SqsError::new(ErrorCode::ResourceNotFound, "Task does not exist or is not running."))
    }

    pub async fn list_message_move_tasks(&self, source_arn: &str, max_results: Option<i64>) -> Result<Vec<MoveTask>> {
        let limit = max_results.unwrap_or(1);
        if !(1..=10).contains(&limit) {
            return Err(SqsError::invalid_value(
                "MaxResults",
                limit,
                "MaxResults must be an integer between 1 and 10.",
            ));
        }
        let source = self.queue_by_arn(source_arn).await.map_err(|e| match e.code {
            ErrorCode::QueueDoesNotExist => SqsError::new(
                ErrorCode::ResourceNotFound,
                "The resource that you specified for the SourceArn parameter doesn't exist.",
            ),
            _ => e,
        })?;
        let rows: Vec<TaskRow> = sqlx::query_as(&format!(
            "SELECT {TASK_COLUMNS} FROM qd_move_tasks WHERE source_queue_id = $1 ORDER BY started_at DESC LIMIT $2"
        ))
        .bind(source.id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().map(TaskRow::into_task).collect())
    }

    /// Processes at most one batch for each due task. Returns the number of messages moved.
    pub async fn run_move_tasks_once(&self) -> Result<u64> {
        let interval = self.config().move_task_interval;
        let mut total = 0;
        // Bounded so a tick cannot run forever; remaining tasks are picked up next tick.
        for _ in 0..32 {
            let mut tx = self.pool().begin().await?;
            // Claim one due task. `updated_at` doubles as the per-tick rate-limit clock so that
            // multiple replicas don't multiply the configured rate.
            let task: Option<TaskRow> = sqlx::query_as(&format!(
                "SELECT {TASK_COLUMNS} FROM qd_move_tasks
                 WHERE status = 'RUNNING' AND updated_at <= now() - make_interval(secs => $1)
                 ORDER BY updated_at LIMIT 1 FOR UPDATE SKIP LOCKED"
            ))
            .bind(interval.as_secs_f64() * 0.9)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(task) = task else {
                return Ok(total);
            };
            let mut destinations = Vec::new();
            let moved = match self.move_batch(&mut tx, &task, interval, &mut destinations).await {
                Ok(n) => n,
                Err(reason) => {
                    sqlx::query(
                        "UPDATE qd_move_tasks SET status = 'FAILED', failure_reason = $2, updated_at = now() WHERE id = $1",
                    )
                    .bind(task.id)
                    .bind(&reason.message)
                    .execute(&mut *tx)
                    .await?;
                    0
                }
            };
            tx.commit().await?;
            for dest in destinations {
                self.publish(dest);
            }
            total += moved;
        }
        Ok(total)
    }

    /// Moves one rate-limited batch; queue ids that received messages are pushed to `destinations`.
    async fn move_batch(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task: &TaskRow,
        interval: Duration,
        destinations: &mut Vec<i64>,
    ) -> Result<u64> {
        let rate = task.max_per_second.map(i64::from).unwrap_or(DEFAULT_RATE);
        let batch = ((rate as f64) * interval.as_secs_f64()).ceil().max(1.0) as i64;

        let source_exists: Option<(i64,)> = sqlx::query_as("SELECT id FROM qd_queues WHERE id = $1")
            .bind(task.source_queue_id)
            .fetch_optional(&mut **tx)
            .await?;
        if source_exists.is_none() {
            return Err(SqsError::invalid_parameter("Source queue was deleted."));
        }

        let candidates: Vec<(i64, Option<String>)> = sqlx::query_as(
            "SELECT id, dlq_source_arn FROM qd_messages
             WHERE queue_id = $1 AND id <= $2 AND visible_at <= now() AND expires_at > now()
             ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED",
        )
        .bind(task.source_queue_id)
        .bind(task.cutoff_id)
        .bind(batch)
        .fetch_all(&mut **tx)
        .await?;

        if candidates.is_empty() {
            let remaining: (bool,) = sqlx::query_as(
                "SELECT EXISTS (SELECT 1 FROM qd_messages WHERE queue_id = $1 AND id <= $2 AND expires_at > now())",
            )
            .bind(task.source_queue_id)
            .bind(task.cutoff_id)
            .fetch_one(&mut **tx)
            .await?;
            let status = if remaining.0 { "RUNNING" } else { "COMPLETED" };
            sqlx::query("UPDATE qd_move_tasks SET status = $2, updated_at = now() WHERE id = $1")
                .bind(task.id)
                .bind(status)
                .execute(&mut **tx)
                .await?;
            return Ok(0);
        }

        // Group by destination: an explicit DestinationArn, otherwise each message's origin.
        let mut by_dest: BTreeMap<String, Vec<i64>> = BTreeMap::new();
        for (id, origin) in candidates {
            let dest = match (&task.destination_arn, origin) {
                (Some(d), _) => d.clone(),
                (None, Some(o)) => o,
                (None, None) => {
                    return Err(SqsError::invalid_parameter(
                        "A message in the source queue has no original source queue; specify DestinationArn.",
                    ));
                }
            };
            by_dest.entry(dest).or_default().push(id);
        }
        let mut moved = 0u64;
        for (dest_arn, ids) in by_dest {
            let dest: Arc<Queue> = self
                .queue_by_arn(&dest_arn)
                .await
                .map_err(|_| SqsError::invalid_parameter(format!("Destination queue {dest_arn} does not exist.")))?;
            let groups: Vec<Option<String>> = sqlx::query_scalar(
                "WITH moved AS (
                     DELETE FROM qd_messages m WHERE m.id = ANY($1) AND m.queue_id = $2 RETURNING m.*
                 )
                 INSERT INTO qd_messages
                    (queue_id, message_id, body, message_attributes, system_attributes, sender_id, sent_at,
                     visible_at, expires_at, receive_count, group_id, dedup_id)
                 SELECT $3, m.message_id, m.body, m.message_attributes, m.system_attributes, m.sender_id, m.sent_at,
                     now(), now() + make_interval(secs => $4), 0, m.group_id, m.dedup_id
                 FROM moved m ORDER BY m.id
                 RETURNING group_id",
            )
            .bind(&ids)
            .bind(task.source_queue_id)
            .bind(dest.id)
            .bind(dest.attributes.message_retention_period as f64)
            .fetch_all(&mut **tx)
            .await?;
            if dest.fifo {
                let mut groups: Vec<String> = groups.iter().flatten().cloned().collect();
                groups.sort();
                groups.dedup();
                touch_groups(tx, dest.id, &groups).await?;
            }
            moved += groups.len() as u64;
            destinations.push(dest.id);
        }
        sqlx::query("UPDATE qd_move_tasks SET moved = moved + $2, updated_at = now() WHERE id = $1")
            .bind(task.id)
            .bind(moved as i64)
            .execute(&mut **tx)
            .await?;
        metrics::counter!("qd_messages_redriven_total").increment(moved);
        Ok(moved)
    }
}

pub(super) async fn run_worker(engine: Engine) {
    let shutdown = engine.shutdown_token();
    let interval = engine.config().move_task_interval;
    loop {
        if let Err(err) = engine.run_move_tasks_once().await {
            tracing::warn!(error = %err, "message move worker failed");
        }
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
    }
}
