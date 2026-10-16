//! SendMessage, DeleteMessage and ChangeMessageVisibility (single and batch).

use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{Engine, validate_batch_ids};
use crate::Caller;
use crate::attributes::{MAX_DELAY_SECONDS, MAX_MESSAGE_SIZE, MAX_VISIBILITY_TIMEOUT};
use crate::error::{ErrorCode, Result, SqsError};
use crate::message::{
    MessageAttributes, md5_hex, md5_of_attributes, validate_body, validate_fifo_id, validate_message_attributes,
    validate_system_attributes,
};
use crate::names::QueueRef;
use crate::queue::Queue;
use crate::receipt::ReceiptHandle;

/// Per-entry outcome of a batch call: the entry id plus its result.
pub type BatchResult<T> = Vec<(String, Result<T>)>;

#[derive(Debug, Clone, Default)]
pub struct SendMessageInput {
    pub body: String,
    pub delay_seconds: Option<i64>,
    pub message_attributes: MessageAttributes,
    pub system_attributes: MessageAttributes,
    pub group_id: Option<String>,
    pub dedup_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendMessageOutput {
    pub message_id: String,
    pub md5_of_body: String,
    pub md5_of_message_attributes: Option<String>,
    pub md5_of_system_attributes: Option<String>,
    pub sequence_number: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DeleteEntry {
    pub id: String,
    pub receipt_handle: String,
}

#[derive(Debug, Clone)]
pub struct ChangeVisibilityEntry {
    pub id: String,
    pub receipt_handle: String,
    pub visibility_timeout: i64,
}

/// A validated message ready for insertion.
struct Prepared<'a> {
    input: &'a SendMessageInput,
    delay: i64,
    size: usize,
    group_id: Option<&'a str>,
    dedup_id: Option<String>,
}

pub(crate) fn format_sequence(seq: i64) -> String {
    format!("{seq:020}")
}

fn prepare<'a>(q: &Queue, input: &'a SendMessageInput) -> Result<Prepared<'a>> {
    validate_body(&input.body)?;
    let attr_size = validate_message_attributes(&input.message_attributes)?;
    validate_system_attributes(&input.system_attributes)?;
    let size = input.body.len() + attr_size;
    let limit = q.attributes.maximum_message_size as usize;
    if size > limit {
        return Err(SqsError::invalid_parameter(format!(
            "One or more parameters are invalid. Reason: Message must be shorter than {limit} bytes."
        )));
    }
    let delay = match input.delay_seconds {
        Some(d) if !(0..=MAX_DELAY_SECONDS as i64).contains(&d) => {
            return Err(SqsError::invalid_value("DelaySeconds", d, "DelaySeconds must be >= 0 and <= 900."));
        }
        Some(_) if q.fifo => {
            return Err(SqsError::invalid_value(
                "DelaySeconds",
                input.delay_seconds.unwrap_or_default(),
                "The request include parameter that is not valid for this queue type.",
            ));
        }
        Some(d) => d,
        None => q.attributes.delay_seconds as i64,
    };
    if let Some(g) = &input.group_id {
        validate_fifo_id("MessageGroupId", g)?;
    }
    if let Some(d) = &input.dedup_id {
        validate_fifo_id("MessageDeduplicationId", d)?;
    }
    let dedup_id = if q.fifo {
        if input.group_id.is_none() {
            return Err(SqsError::missing("MessageGroupId"));
        }
        match &input.dedup_id {
            Some(d) => Some(d.clone()),
            None if q.attributes.content_based_deduplication => {
                Some(hex::encode(Sha256::digest(input.body.as_bytes())))
            }
            None => {
                return Err(SqsError::invalid_parameter(
                    "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                ));
            }
        }
    } else {
        if let Some(d) = &input.dedup_id {
            return Err(SqsError::invalid_value(
                "MessageDeduplicationId",
                d,
                "The request include parameter that is not valid for this queue type.",
            ));
        }
        None
    };
    Ok(Prepared { input, delay, size, group_id: input.group_id.as_deref(), dedup_id })
}

fn invalid_handle_for_queue(handle: &str) -> SqsError {
    SqsError::new(
        ErrorCode::ReceiptHandleIsInvalid,
        format!("The input receipt handle \"{handle}\" is not a valid receipt handle."),
    )
}

fn expired_handle(handle: &str) -> SqsError {
    SqsError::invalid_value("ReceiptHandle", handle, "The receipt handle has expired.")
}

fn validate_visibility(timeout: i64) -> Result<()> {
    if (0..=MAX_VISIBILITY_TIMEOUT as i64).contains(&timeout) {
        Ok(())
    } else {
        Err(SqsError::invalid_value(
            "VisibilityTimeout",
            timeout,
            "VisibilityTimeout must be an integer between 0 and 43200.",
        ))
    }
}

impl Engine {
    pub async fn send_message(
        &self,
        caller: &Caller,
        queue: &QueueRef,
        input: SendMessageInput,
    ) -> Result<SendMessageOutput> {
        let q = self.queue(queue).await?;
        let prepared = prepare(&q, &input)?;
        let out = if q.fifo {
            let mut tx = self.pool().begin().await?;
            let out = insert_message(&mut tx, &q, caller, &prepared).await?;
            tx.commit().await?;
            out
        } else {
            insert_standard(self.pool(), &q, caller, &[&prepared]).await?.remove(0)
        };
        if prepared.delay == 0 {
            self.publish(q.id);
        }
        metrics::counter!("qd_messages_sent_total", "queue" => q.name.clone()).increment(1);
        metrics::histogram!("qd_message_size_bytes", "queue" => q.name.clone()).record(prepared.size as f64);
        Ok(out)
    }

    pub async fn send_message_batch(
        &self,
        caller: &Caller,
        queue: &QueueRef,
        entries: Vec<(String, SendMessageInput)>,
    ) -> Result<BatchResult<SendMessageOutput>> {
        validate_batch_ids(entries.iter().map(|(id, _)| id.as_str()), "SendMessageBatchRequestEntry")?;
        let q = self.queue(queue).await?;
        let prepared: Vec<(String, Result<Prepared>)> =
            entries.iter().map(|(id, input)| (id.clone(), prepare(&q, input))).collect();
        let total: usize = prepared.iter().filter_map(|(_, p)| p.as_ref().ok()).map(|p| p.size).sum();
        if total > MAX_MESSAGE_SIZE as usize {
            return Err(SqsError::new(
                ErrorCode::BatchRequestTooLong,
                format!("Batch requests cannot be longer than {MAX_MESSAGE_SIZE} bytes. You have sent {total} bytes."),
            ));
        }
        if !q.fifo {
            let valid: Vec<&Prepared> = prepared.iter().filter_map(|(_, p)| p.as_ref().ok()).collect();
            let wake = valid.iter().any(|p| p.delay == 0);
            for p in &valid {
                metrics::histogram!("qd_message_size_bytes", "queue" => q.name.clone()).record(p.size as f64);
            }
            let sent = valid.len() as u64;
            let mut outputs = insert_standard(self.pool(), &q, caller, &valid).await?.into_iter();
            drop(valid);
            let results: BatchResult<SendMessageOutput> = prepared
                .into_iter()
                .map(|(id, p)| match p {
                    Ok(_) => (id, Ok(outputs.next().expect("one output per valid entry"))),
                    Err(e) => (id, Err(e)),
                })
                .collect();
            if wake {
                self.publish(q.id);
            }
            metrics::counter!("qd_messages_sent_total", "queue" => q.name.clone()).increment(sent);
            return Ok(results);
        }
        let mut tx = self.pool().begin().await?;
        let mut results = Vec::with_capacity(prepared.len());
        let mut wake = false;
        let mut sent = 0u64;
        for (id, p) in prepared {
            let result = match p {
                Ok(p) => {
                    wake |= p.delay == 0;
                    metrics::histogram!("qd_message_size_bytes", "queue" => q.name.clone()).record(p.size as f64);
                    let r = insert_message(&mut tx, &q, caller, &p).await;
                    if r.is_ok() {
                        sent += 1;
                    }
                    r
                }
                Err(e) => Err(e),
            };
            // A database failure aborts the transaction; report it for the whole request.
            if let Err(e) = &result
                && matches!(e.code, ErrorCode::InternalFailure | ErrorCode::ServiceUnavailable)
            {
                return Err(e.clone());
            }
            results.push((id, result));
        }
        tx.commit().await?;
        if wake {
            self.publish(q.id);
        }
        metrics::counter!("qd_messages_sent_total", "queue" => q.name.clone()).increment(sent);
        Ok(results)
    }

    pub async fn delete_message(&self, queue: &QueueRef, receipt_handle: &str) -> Result<()> {
        let q = self.queue(queue).await?;
        let handle = decode_for_queue(&q, receipt_handle)?;
        delete_handles(self.pool(), &q, &[handle]).await?;
        // Deleting may unblock a FIFO message group.
        if q.fifo {
            self.publish(q.id);
        }
        metrics::counter!("qd_messages_deleted_total", "queue" => q.name.clone()).increment(1);
        Ok(())
    }

    pub async fn delete_message_batch(&self, queue: &QueueRef, entries: Vec<DeleteEntry>) -> Result<BatchResult<()>> {
        validate_batch_ids(entries.iter().map(|e| e.id.as_str()), "DeleteMessageBatchRequestEntry")?;
        let q = self.queue(queue).await?;
        let decoded: Vec<(String, Result<ReceiptHandle>)> =
            entries.into_iter().map(|e| (e.id, decode_for_queue(&q, &e.receipt_handle))).collect();
        let handles: Vec<ReceiptHandle> = decoded.iter().filter_map(|(_, h)| h.as_ref().ok().copied()).collect();
        delete_handles(self.pool(), &q, &handles).await?;
        if q.fifo {
            self.publish(q.id);
        }
        metrics::counter!("qd_messages_deleted_total", "queue" => q.name.clone()).increment(handles.len() as u64);
        Ok(decoded.into_iter().map(|(id, h)| (id, h.map(|_| ()))).collect())
    }

    pub async fn change_message_visibility(
        &self,
        queue: &QueueRef,
        receipt_handle: &str,
        visibility_timeout: i64,
    ) -> Result<()> {
        let q = self.queue(queue).await?;
        let mut tx = self.pool().begin().await?;
        change_one(&mut tx, &q, receipt_handle, visibility_timeout).await?;
        tx.commit().await?;
        if visibility_timeout == 0 {
            self.publish(q.id);
        }
        Ok(())
    }

    pub async fn change_message_visibility_batch(
        &self,
        queue: &QueueRef,
        entries: Vec<ChangeVisibilityEntry>,
    ) -> Result<BatchResult<()>> {
        validate_batch_ids(entries.iter().map(|e| e.id.as_str()), "ChangeMessageVisibilityBatchRequestEntry")?;
        let q = self.queue(queue).await?;
        let mut tx = self.pool().begin().await?;
        let mut results = Vec::with_capacity(entries.len());
        let mut wake = false;
        for entry in entries {
            let r = change_one(&mut tx, &q, &entry.receipt_handle, entry.visibility_timeout).await;
            if let Err(e) = &r
                && matches!(e.code, ErrorCode::InternalFailure | ErrorCode::ServiceUnavailable)
            {
                return Err(e.clone());
            }
            wake |= r.is_ok() && entry.visibility_timeout == 0;
            results.push((entry.id, r));
        }
        tx.commit().await?;
        if wake {
            self.publish(q.id);
        }
        Ok(results)
    }
}

async fn insert_message(
    tx: &mut Transaction<'_, Postgres>,
    q: &Queue,
    caller: &Caller,
    p: &Prepared<'_>,
) -> Result<SendMessageOutput> {
    let input = p.input;
    let message_id = Uuid::new_v4();
    let mut output = SendMessageOutput {
        message_id: message_id.to_string(),
        md5_of_body: md5_hex(input.body.as_bytes()),
        md5_of_message_attributes: md5_of_attributes(&input.message_attributes),
        md5_of_system_attributes: md5_of_attributes(&input.system_attributes),
        sequence_number: None,
    };

    let mut row_id: Option<i64> = None;
    if q.fifo {
        let group = p.group_id.unwrap_or_default();
        let dedup = p.dedup_id.as_deref().unwrap_or_default();
        let scope = if q.attributes.deduplication_scope == "messageGroup" { group } else { "" };
        let (seq,): (i64,) =
            sqlx::query_as("SELECT nextval(pg_get_serial_sequence('qd_messages', 'id'))").fetch_one(&mut **tx).await?;
        let fresh: Option<(Uuid,)> = sqlx::query_as(
            "INSERT INTO qd_fifo_dedup (queue_id, scope, dedup_id, message_id, sequence_number, expires_at)
             VALUES ($1, $2, $3, $4, $5, now() + interval '5 minutes')
             ON CONFLICT (queue_id, scope, dedup_id) DO UPDATE
                SET message_id = EXCLUDED.message_id,
                    sequence_number = EXCLUDED.sequence_number,
                    expires_at = EXCLUDED.expires_at
                WHERE qd_fifo_dedup.expires_at <= now()
             RETURNING message_id",
        )
        .bind(q.id)
        .bind(scope)
        .bind(dedup)
        .bind(message_id)
        .bind(seq)
        .fetch_optional(&mut **tx)
        .await?;
        if fresh.is_none() {
            // Duplicate within the deduplication window: acknowledge without enqueueing.
            let (orig_id, orig_seq): (Uuid, i64) = sqlx::query_as(
                "SELECT message_id, sequence_number FROM qd_fifo_dedup WHERE queue_id = $1 AND scope = $2 AND dedup_id = $3",
            )
            .bind(q.id)
            .bind(scope)
            .bind(dedup)
            .fetch_one(&mut **tx)
            .await?;
            output.message_id = orig_id.to_string();
            output.sequence_number = Some(format_sequence(orig_seq));
            metrics::counter!("qd_messages_deduplicated_total", "queue" => q.name.clone()).increment(1);
            return Ok(output);
        }
        sqlx::query(
            "INSERT INTO qd_fifo_groups (queue_id, group_id, last_send_at) VALUES ($1, $2, now())
             ON CONFLICT (queue_id, group_id) DO UPDATE SET last_send_at = now()",
        )
        .bind(q.id)
        .bind(group)
        .execute(&mut **tx)
        .await?;
        row_id = Some(seq);
        output.sequence_number = Some(format_sequence(seq));
    }

    let msg_attrs = (!input.message_attributes.is_empty()).then_some(Json(&input.message_attributes));
    let sys_attrs = (!input.system_attributes.is_empty()).then_some(Json(&input.system_attributes));
    sqlx::query(
        "INSERT INTO qd_messages
            (id, queue_id, message_id, body, message_attributes, system_attributes, sender_id,
             sent_at, visible_at, expires_at, group_id, dedup_id)
         VALUES (COALESCE($1, nextval(pg_get_serial_sequence('qd_messages', 'id'))), $2, $3, $4, $5, $6, $7,
             now(), now() + make_interval(secs => $8), now() + make_interval(secs => $9), $10, $11)",
    )
    .bind(row_id)
    .bind(q.id)
    .bind(message_id)
    .bind(&input.body)
    .bind(msg_attrs)
    .bind(sys_attrs)
    .bind(&caller.sender_id)
    .bind(p.delay as f64)
    .bind(q.attributes.message_retention_period as f64)
    .bind(p.group_id)
    .bind(p.dedup_id.as_deref())
    .execute(&mut **tx)
    .await?;
    Ok(output)
}

fn decode_for_queue(q: &Queue, handle: &str) -> Result<ReceiptHandle> {
    let h = ReceiptHandle::decode(handle)?;
    if h.queue_id != q.id {
        return Err(invalid_handle_for_queue(handle));
    }
    Ok(h)
}

/// Deletes messages by receipt handle in one statement.
///
/// Standard queues honour stale handles (the message is still deleted); FIFO queues only honour
/// the handle from the most recent receive, as reordering would otherwise be possible.
async fn delete_handles(pool: &PgPool, q: &Queue, handles: &[ReceiptHandle]) -> Result<()> {
    if handles.is_empty() {
        return Ok(());
    }
    let ids: Vec<i64> = handles.iter().map(|h| h.row_id).collect();
    let tokens: Vec<Uuid> = handles.iter().map(|h| h.token).collect();
    sqlx::query(
        "DELETE FROM qd_messages m USING unnest($2::bigint[], $3::uuid[]) AS e(id, token)
         WHERE m.queue_id = $1 AND m.id = e.id AND (m.receipt_token = e.token OR NOT $4)",
    )
    .bind(q.id)
    .bind(&ids)
    .bind(&tokens)
    .bind(q.fifo)
    .execute(pool)
    .await?;
    Ok(())
}

/// Inserts standard-queue messages with a single statement (no explicit transaction).
async fn insert_standard(
    pool: &PgPool,
    q: &Queue,
    caller: &Caller,
    batch: &[&Prepared<'_>],
) -> Result<Vec<SendMessageOutput>> {
    if batch.is_empty() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::with_capacity(batch.len());
    let mut bodies = Vec::with_capacity(batch.len());
    let mut msg_attrs = Vec::with_capacity(batch.len());
    let mut sys_attrs = Vec::with_capacity(batch.len());
    let mut delays = Vec::with_capacity(batch.len());
    let mut groups = Vec::with_capacity(batch.len());
    let mut outputs = Vec::with_capacity(batch.len());
    for p in batch {
        let input = p.input;
        let message_id = Uuid::new_v4();
        ids.push(message_id);
        bodies.push(input.body.as_str());
        msg_attrs.push(
            (!input.message_attributes.is_empty())
                .then(|| serde_json::to_value(&input.message_attributes).unwrap_or_default()),
        );
        sys_attrs.push(
            (!input.system_attributes.is_empty())
                .then(|| serde_json::to_value(&input.system_attributes).unwrap_or_default()),
        );
        delays.push(p.delay as f64);
        groups.push(p.group_id);
        outputs.push(SendMessageOutput {
            message_id: message_id.to_string(),
            md5_of_body: md5_hex(input.body.as_bytes()),
            md5_of_message_attributes: md5_of_attributes(&input.message_attributes),
            md5_of_system_attributes: md5_of_attributes(&input.system_attributes),
            sequence_number: None,
        });
    }
    sqlx::query(
        "INSERT INTO qd_messages
            (queue_id, message_id, body, message_attributes, system_attributes, sender_id,
             sent_at, visible_at, expires_at, group_id)
         SELECT $1, t.message_id, t.body, t.message_attributes, t.system_attributes, $2,
             now(), now() + make_interval(secs => t.delay), now() + make_interval(secs => $3), t.group_id
         FROM unnest($4::uuid[], $5::text[], $6::jsonb[], $7::jsonb[], $8::float8[], $9::text[])
             AS t(message_id, body, message_attributes, system_attributes, delay, group_id)",
    )
    .bind(q.id)
    .bind(&caller.sender_id)
    .bind(q.attributes.message_retention_period as f64)
    .bind(&ids)
    .bind(&bodies)
    .bind(&msg_attrs)
    .bind(&sys_attrs)
    .bind(&delays)
    .bind(&groups)
    .execute(pool)
    .await?;
    Ok(outputs)
}

async fn change_one(
    tx: &mut Transaction<'_, Postgres>,
    q: &Queue,
    handle: &str,
    visibility_timeout: i64,
) -> Result<()> {
    let h = decode_for_queue(q, handle)?;
    validate_visibility(visibility_timeout)?;
    let updated = sqlx::query(
        "UPDATE qd_messages SET visible_at = now() + make_interval(secs => $4)
         WHERE id = $1 AND queue_id = $2 AND receipt_token = $3",
    )
    .bind(h.row_id)
    .bind(q.id)
    .bind(h.token)
    .bind(visibility_timeout as f64)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() == 0 {
        return Err(expired_handle(handle));
    }
    Ok(())
}
