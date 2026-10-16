//! ReceiveMessage: short and long polling, FIFO group ordering and dead-letter moves.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::types::Json;
use sqlx::{Postgres, Transaction};
use tokio::time::Instant;
use uuid::Uuid;

use super::messages::format_sequence;
use super::{Engine, fifo_lock_key, millis};
use crate::attributes::{MAX_VISIBILITY_TIMEOUT, MAX_WAIT_TIME_SECONDS};
use crate::error::{Result, SqsError};
use crate::message::{MessageAttributes, ReceivedMessage, md5_hex, system_names as sys};
use crate::names::{QueueArn, QueueRef};
use crate::queue::Queue;
use crate::receipt::ReceiptHandle;

#[derive(Debug, Clone, Default)]
pub struct ReceiveInput {
    pub max_number_of_messages: Option<i64>,
    pub visibility_timeout: Option<i64>,
    pub wait_time_seconds: Option<i64>,
    pub receive_request_attempt_id: Option<String>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct MessageRow {
    pub id: i64,
    pub message_id: Uuid,
    pub body: String,
    pub message_attributes: Option<Json<MessageAttributes>>,
    pub system_attributes: Option<Json<MessageAttributes>>,
    pub sender_id: String,
    pub sent_at: DateTime<Utc>,
    pub receive_count: i32,
    pub first_received_at: Option<DateTime<Utc>>,
    pub receipt_token: Option<Uuid>,
    pub group_id: Option<String>,
    pub dedup_id: Option<String>,
    pub dlq_source_arn: Option<String>,
}

const MESSAGE_COLUMNS: &str = "id, message_id, body, message_attributes, system_attributes, sender_id, \
     sent_at, receive_count, first_received_at, receipt_token, group_id, dedup_id, dlq_source_arn";

/// Upper bound of fill-up rounds when messages are diverted to a dead-letter queue.
const MAX_ROUNDS: usize = 5;

#[derive(serde::Serialize, serde::Deserialize)]
struct AttemptEntry {
    row_id: i64,
    token: Uuid,
}

impl MessageRow {
    fn into_received(self, q: &Queue) -> ReceivedMessage {
        let mut system = BTreeMap::new();
        system.insert(sys::SENDER_ID.to_string(), self.sender_id);
        system.insert(sys::SENT_TIMESTAMP.to_string(), millis(self.sent_at).to_string());
        system.insert(sys::APPROXIMATE_RECEIVE_COUNT.to_string(), self.receive_count.to_string());
        if let Some(first) = self.first_received_at {
            system.insert(sys::APPROXIMATE_FIRST_RECEIVE_TIMESTAMP.to_string(), millis(first).to_string());
        }
        if let Some(trace) = self
            .system_attributes
            .as_ref()
            .and_then(|a| a.0.get(sys::AWS_TRACE_HEADER))
            .and_then(|v| v.string_value.clone())
        {
            system.insert(sys::AWS_TRACE_HEADER.to_string(), trace);
        }
        if let Some(g) = self.group_id {
            system.insert(sys::MESSAGE_GROUP_ID.to_string(), g);
        }
        if q.fifo {
            system.insert(sys::SEQUENCE_NUMBER.to_string(), format_sequence(self.id));
            if let Some(d) = self.dedup_id {
                system.insert(sys::MESSAGE_DEDUPLICATION_ID.to_string(), d);
            }
        }
        if let Some(src) = self.dlq_source_arn {
            system.insert(sys::DEAD_LETTER_QUEUE_SOURCE_ARN.to_string(), src);
        }
        let handle = ReceiptHandle { queue_id: q.id, row_id: self.id, token: self.receipt_token.unwrap_or_default() };
        ReceivedMessage {
            message_id: self.message_id.to_string(),
            receipt_handle: handle.encode(),
            md5_of_body: md5_hex(self.body.as_bytes()),
            body: self.body,
            message_attributes: self.message_attributes.map(|a| a.0).unwrap_or_default(),
            system_attributes: system,
        }
    }
}

impl Engine {
    /// ReceiveMessage. Long-polls for up to `WaitTimeSeconds` (or the queue default).
    pub async fn receive_messages(&self, queue: &QueueRef, input: ReceiveInput) -> Result<Vec<ReceivedMessage>> {
        let q = self.queue(queue).await?;
        let max = input.max_number_of_messages.unwrap_or(1);
        if !(1..=10).contains(&max) {
            return Err(SqsError::invalid_value("MaxNumberOfMessages", max, "Must be between 1 and 10, if provided."));
        }
        let visibility = input.visibility_timeout.unwrap_or(q.attributes.visibility_timeout as i64);
        if !(0..=MAX_VISIBILITY_TIMEOUT as i64).contains(&visibility) {
            return Err(SqsError::invalid_value(
                "VisibilityTimeout",
                visibility,
                "Must be >= 0 and <= 43200, if provided.",
            ));
        }
        let wait = input.wait_time_seconds.unwrap_or(q.attributes.receive_message_wait_time_seconds as i64);
        if !(0..=MAX_WAIT_TIME_SECONDS as i64).contains(&wait) {
            return Err(SqsError::invalid_value("WaitTimeSeconds", wait, "Must be >= 0 and <= 20, if provided."));
        }
        let attempt_id = input.receive_request_attempt_id.as_deref().filter(|_| q.fifo);
        if let Some(a) = attempt_id {
            crate::message::validate_fifo_id("ReceiveRequestAttemptId", a)?;
        }

        let deadline = Instant::now() + Duration::from_secs(wait as u64);
        let _poll_gauge = (wait > 0).then(LongPollGauge::enter);
        let notify = self.inner.notifier.subscribe(q.id);
        let shutdown = self.shutdown_token();
        loop {
            // Arm the wakeup before looking, so a send landing in between is not missed.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let messages = self.receive_once(&q, max, visibility, attempt_id).await?;
            let now = Instant::now();
            if !messages.is_empty() || now >= deadline || shutdown.is_cancelled() {
                if !messages.is_empty() {
                    metrics::counter!("qd_messages_received_total", "queue" => q.name.clone())
                        .increment(messages.len() as u64);
                } else {
                    metrics::counter!("qd_empty_receives_total", "queue" => q.name.clone()).increment(1);
                }
                return Ok(messages);
            }
            let mut sleep_for = (deadline - now).min(self.config().long_poll_recheck);
            if let Some(next) = self.next_visible_in(q.id).await? {
                sleep_for = sleep_for.min(next);
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep(sleep_for) => {}
                _ = shutdown.cancelled() => {}
            }
        }
    }

    /// Time until the next currently-invisible message becomes visible.
    async fn next_visible_in(&self, queue_id: i64) -> Result<Option<Duration>> {
        let secs: Option<f64> = sqlx::query_scalar(
            "SELECT EXTRACT(EPOCH FROM (min(visible_at) - now()))::float8
             FROM qd_messages WHERE queue_id = $1 AND visible_at > now()",
        )
        .bind(queue_id)
        .fetch_one(self.pool())
        .await?;
        Ok(secs.map(|s| Duration::from_secs_f64(s.max(0.0)) + Duration::from_millis(5)))
    }

    async fn receive_once(
        &self,
        q: &Arc<Queue>,
        max: i64,
        visibility: i64,
        attempt_id: Option<&str>,
    ) -> Result<Vec<ReceivedMessage>> {
        let dlq = self.resolve_dlq(q).await?;
        if !q.fifo && dlq.is_none() {
            return self.receive_standard_fast(q, max, visibility).await;
        }
        let mut tx = self.pool().begin().await?;
        if q.fifo {
            sqlx::query("SELECT pg_advisory_xact_lock($1)").bind(fifo_lock_key(q.id)).execute(&mut *tx).await?;
            if let Some(attempt) = attempt_id
                && let Some(replayed) = replay_attempt(&mut tx, q, attempt, visibility).await?
            {
                tx.commit().await?;
                return Ok(replayed);
            }
        }

        let mut delivered: Vec<MessageRow> = Vec::new();
        let mut moved_to_dlq = 0usize;
        for _ in 0..MAX_ROUNDS {
            let want = max - delivered.len() as i64;
            if want <= 0 {
                break;
            }
            let candidates = if q.fifo {
                fifo_candidates(&mut tx, q.id, want).await?
            } else {
                standard_candidates(&mut tx, q.id, want).await?
            };
            if candidates.is_empty() {
                break;
            }
            let (to_dlq, to_deliver): (Vec<_>, Vec<_>) = candidates
                .into_iter()
                .partition(|(_, count)| dlq.as_ref().is_some_and(|(_, max_count)| *count >= *max_count as i32));
            if let Some((dlq_queue, _)) = &dlq
                && !to_dlq.is_empty()
            {
                let ids: Vec<i64> = to_dlq.iter().map(|(id, _)| *id).collect();
                move_to_dlq(&mut tx, q, dlq_queue, &ids).await?;
                moved_to_dlq += ids.len();
            }
            let ids: Vec<i64> = to_deliver.iter().map(|(id, _)| *id).collect();
            if !ids.is_empty() {
                let mut rows: Vec<MessageRow> = sqlx::query_as(&format!(
                    "UPDATE qd_messages SET
                        visible_at = now() + make_interval(secs => $2),
                        receive_count = receive_count + 1,
                        first_received_at = COALESCE(first_received_at, now()),
                        receipt_token = gen_random_uuid()
                     WHERE id = ANY($1)
                     RETURNING {MESSAGE_COLUMNS}"
                ))
                .bind(&ids)
                .bind(visibility as f64)
                .fetch_all(&mut *tx)
                .await?;
                rows.sort_by_key(|r| ids.iter().position(|id| *id == r.id));
                delivered.extend(rows);
            }
            if to_dlq.is_empty() {
                break;
            }
        }

        if let Some(attempt) = attempt_id {
            let entries: Vec<AttemptEntry> = delivered
                .iter()
                .map(|r| AttemptEntry { row_id: r.id, token: r.receipt_token.unwrap_or_default() })
                .collect();
            sqlx::query(
                "INSERT INTO qd_receive_attempts (queue_id, attempt_id, created_at, entries) VALUES ($1, $2, now(), $3)
                 ON CONFLICT (queue_id, attempt_id) DO UPDATE SET created_at = now(), entries = EXCLUDED.entries",
            )
            .bind(q.id)
            .bind(attempt)
            .bind(Json(&entries))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        if moved_to_dlq > 0 {
            if let Some((dlq_queue, _)) = &dlq {
                self.publish(dlq_queue.id);
            }
            metrics::counter!("qd_messages_dead_lettered_total", "queue" => q.name.clone())
                .increment(moved_to_dlq as u64);
        }
        Ok(delivered.into_iter().map(|r| r.into_received(q)).collect())
    }

    /// Standard queue without a dead-letter queue: pick and claim in one statement.
    async fn receive_standard_fast(&self, q: &Queue, max: i64, visibility: i64) -> Result<Vec<ReceivedMessage>> {
        let mut rows: Vec<MessageRow> = sqlx::query_as(&format!(
            "WITH picked AS (
                 SELECT id FROM qd_messages
                 WHERE queue_id = $1 AND visible_at <= now() AND expires_at > now()
                 ORDER BY visible_at, id
                 LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             UPDATE qd_messages m SET
                visible_at = now() + make_interval(secs => $3),
                receive_count = m.receive_count + 1,
                first_received_at = COALESCE(m.first_received_at, now()),
                receipt_token = gen_random_uuid()
             FROM picked WHERE m.id = picked.id
             RETURNING {}",
            MESSAGE_COLUMNS.split(", ").map(|c| format!("m.{c}")).collect::<Vec<_>>().join(", ")
        ))
        .bind(q.id)
        .bind(max)
        .bind(visibility as f64)
        .fetch_all(self.pool())
        .await?;
        rows.sort_by_key(|r| r.id);
        Ok(rows.into_iter().map(|r| r.into_received(q)).collect())
    }

    /// Returns the DLQ and maxReceiveCount if the queue has a usable redrive policy.
    async fn resolve_dlq(&self, q: &Queue) -> Result<Option<(Arc<Queue>, u32)>> {
        let Some(policy) = &q.attributes.redrive_policy else {
            return Ok(None);
        };
        let Some(arn) = QueueArn::parse(&policy.dead_letter_target_arn) else {
            return Ok(None);
        };
        match self.queue(&QueueRef::new(arn.account_id, arn.name)).await {
            Ok(dlq) => Ok(Some((dlq, policy.max_receive_count))),
            Err(e) if e.code == crate::ErrorCode::QueueDoesNotExist => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Visible messages of a standard queue, oldest-visible first, skipping rows other receivers hold.
async fn standard_candidates(tx: &mut Transaction<'_, Postgres>, queue_id: i64, limit: i64) -> Result<Vec<(i64, i32)>> {
    Ok(sqlx::query_as(
        "SELECT id, receive_count FROM qd_messages
         WHERE queue_id = $1 AND visible_at <= now() AND expires_at > now()
         ORDER BY visible_at, id
         LIMIT $2
         FOR UPDATE SKIP LOCKED",
    )
    .bind(queue_id)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?)
}

/// Deliverable messages of a FIFO queue, in order. Must run under the queue's FIFO lock.
///
/// A group is deliverable when its oldest live message is visible (nothing earlier is in flight
/// or delayed). From each deliverable group we take the visible prefix, filling from the group
/// with the oldest head first, so related messages are returned together as SQS does.
async fn fifo_candidates(tx: &mut Transaction<'_, Postgres>, queue_id: i64, limit: i64) -> Result<Vec<(i64, i32)>> {
    let heads: Vec<(String,)> = sqlx::query_as(
        "SELECT g.group_id FROM qd_fifo_groups g
         CROSS JOIN LATERAL (
             SELECT m.id, m.visible_at FROM qd_messages m
             WHERE m.queue_id = g.queue_id AND m.group_id = g.group_id AND m.expires_at > now()
             ORDER BY m.id LIMIT 1
         ) h
         WHERE g.queue_id = $1 AND h.visible_at <= now()
         ORDER BY h.id
         LIMIT $2",
    )
    .bind(queue_id)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?;

    let mut out = Vec::new();
    for (group,) in heads {
        let remaining = limit - out.len() as i64;
        if remaining <= 0 {
            break;
        }
        let rows: Vec<(i64, i32, bool)> = sqlx::query_as(
            "SELECT id, receive_count, visible_at <= now() FROM qd_messages
             WHERE queue_id = $1 AND group_id = $2 AND expires_at > now()
             ORDER BY id LIMIT $3
             FOR UPDATE",
        )
        .bind(queue_id)
        .bind(&group)
        .bind(remaining)
        .fetch_all(&mut **tx)
        .await?;
        out.extend(rows.into_iter().take_while(|(_, _, visible)| *visible).map(|(id, c, _)| (id, c)));
    }
    Ok(out)
}

/// Re-delivers the messages of an earlier receive with the same ReceiveRequestAttemptId.
async fn replay_attempt(
    tx: &mut Transaction<'_, Postgres>,
    q: &Queue,
    attempt: &str,
    visibility: i64,
) -> Result<Option<Vec<ReceivedMessage>>> {
    let entries: Option<Json<Vec<AttemptEntry>>> = sqlx::query_scalar(
        "SELECT entries FROM qd_receive_attempts
         WHERE queue_id = $1 AND attempt_id = $2 AND created_at > now() - interval '5 minutes'",
    )
    .bind(q.id)
    .bind(attempt)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(Json(entries)) = entries else {
        return Ok(None);
    };
    if entries.is_empty() {
        return Ok(None);
    }
    let ids: Vec<i64> = entries.iter().map(|e| e.row_id).collect();
    let tokens: Vec<Uuid> = entries.iter().map(|e| e.token).collect();
    // Still in flight with the same receipt handles: reset their visibility and return them.
    let mut rows: Vec<MessageRow> = sqlx::query_as(&format!(
        "UPDATE qd_messages m SET visible_at = now() + make_interval(secs => $3)
         FROM unnest($1::bigint[], $2::uuid[]) AS e(id, token)
         WHERE m.id = e.id AND m.receipt_token = e.token AND m.queue_id = $4 AND m.visible_at > now()
         RETURNING {}",
        MESSAGE_COLUMNS.split(", ").map(|c| format!("m.{c}")).collect::<Vec<_>>().join(", ")
    ))
    .bind(&ids)
    .bind(&tokens)
    .bind(visibility as f64)
    .bind(q.id)
    .fetch_all(&mut **tx)
    .await?;
    if rows.len() != entries.len() {
        // Some messages were deleted or expired; fall back to a fresh receive. Rows we touched
        // are rolled back with the rest of the attempt if the caller aborts, otherwise they keep
        // the refreshed visibility, which is harmless.
        return Ok(None);
    }
    rows.sort_by_key(|r| ids.iter().position(|id| *id == r.id));
    Ok(Some(rows.into_iter().map(|r| r.into_received(q)).collect()))
}

/// Moves messages into the dead-letter queue, preserving their identity.
async fn move_to_dlq(tx: &mut Transaction<'_, Postgres>, source: &Queue, dlq: &Queue, ids: &[i64]) -> Result<()> {
    // Standard DLQs keep the original enqueue time for retention; FIFO DLQs reset it.
    let retention_base = if dlq.fifo { "now()" } else { "m.sent_at" };
    let groups: Vec<Option<String>> = sqlx::query_scalar(&format!(
        "WITH moved AS (
             DELETE FROM qd_messages m WHERE m.id = ANY($1) AND m.queue_id = $2
             RETURNING m.*
         )
         INSERT INTO qd_messages
            (queue_id, message_id, body, message_attributes, system_attributes, sender_id, sent_at,
             visible_at, expires_at, receive_count, first_received_at, receipt_token, group_id, dedup_id,
             dlq_source_arn)
         SELECT $3, m.message_id, m.body, m.message_attributes, m.system_attributes, m.sender_id, m.sent_at,
             now(), {retention_base} + make_interval(secs => $4), m.receive_count, m.first_received_at, NULL,
             m.group_id, m.dedup_id, $5
         FROM moved m ORDER BY m.id
         RETURNING group_id"
    ))
    .bind(ids)
    .bind(source.id)
    .bind(dlq.id)
    .bind(dlq.attributes.message_retention_period as f64)
    .bind(&source.arn)
    .fetch_all(&mut **tx)
    .await?;
    if dlq.fifo {
        let mut groups: Vec<String> = groups.into_iter().flatten().collect();
        groups.sort();
        groups.dedup();
        touch_groups(tx, dlq.id, &groups).await?;
    }
    Ok(())
}

/// Ensures FIFO group rows exist for messages inserted outside of SendMessage.
pub(crate) async fn touch_groups(tx: &mut Transaction<'_, Postgres>, queue_id: i64, groups: &[String]) -> Result<()> {
    if groups.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO qd_fifo_groups (queue_id, group_id, last_send_at)
         SELECT $1, g, now() FROM unnest($2::text[]) AS g
         ON CONFLICT (queue_id, group_id) DO UPDATE SET last_send_at = now()",
    )
    .bind(queue_id)
    .bind(groups)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Tracks `qd_long_polls_in_progress` for the lifetime of a long poll.
struct LongPollGauge;

impl LongPollGauge {
    fn enter() -> Self {
        metrics::gauge!("qd_long_polls_in_progress").increment(1.0);
        Self
    }
}

impl Drop for LongPollGauge {
    fn drop(&mut self) {
        metrics::gauge!("qd_long_polls_in_progress").decrement(1.0);
    }
}
