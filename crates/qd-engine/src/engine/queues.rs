//! Queue lifecycle and metadata operations.

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sqlx::types::Json;

use super::Engine;
use crate::attributes::{QueueAttributes, names as attr};
use crate::error::{ErrorCode, Result, SqsError};
use crate::names::{QueueArn, QueueRef, is_fifo_name, queue_arn, validate_queue_name};
use crate::queue::{QUEUE_COLUMNS, Queue, QueueCounts, QueueRow};

pub const MAX_TAGS: usize = 50;

/// One page of a listing.
#[derive(Debug, Clone, Default)]
pub struct ListPage<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
}

/// Input of AddPermission.
#[derive(Debug, Clone)]
pub struct Permission {
    pub label: String,
    pub account_ids: Vec<String>,
    pub actions: Vec<String>,
}

fn encode_token(name: &str) -> String {
    URL_SAFE_NO_PAD.encode(name)
}

fn decode_token(token: &str) -> Result<String> {
    URL_SAFE_NO_PAD
        .decode(token)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or_else(|| SqsError::invalid_parameter("Invalid NextToken value."))
}

fn validate_max_results(max_results: Option<i64>) -> Result<Option<i64>> {
    match max_results {
        Some(n) if !(1..=1000).contains(&n) => {
            Err(SqsError::invalid_value("MaxResults", n, "MaxResults must be an integer between 1 and 1000."))
        }
        other => Ok(other),
    }
}

fn validate_tags(tags: &BTreeMap<String, String>) -> Result<()> {
    for (k, v) in tags {
        if k.is_empty() || k.chars().count() > 128 {
            return Err(SqsError::invalid_parameter(format!("Tag key {k} must be between 1 and 128 characters.")));
        }
        if v.chars().count() > 256 {
            return Err(SqsError::invalid_parameter(format!("Tag value for key {k} must be at most 256 characters.")));
        }
        if k.to_ascii_lowercase().starts_with("aws:") {
            return Err(SqsError::invalid_parameter("Tag keys must not start with the reserved prefix aws:."));
        }
    }
    Ok(())
}

impl Engine {
    /// CreateQueue. Idempotent when the queue exists with identical requested attributes.
    pub async fn create_queue(
        &self,
        account_id: &str,
        name: &str,
        attributes: &BTreeMap<String, String>,
        tags: &BTreeMap<String, String>,
    ) -> Result<Arc<Queue>> {
        let fifo_requested = match attributes.get(attr::FIFO_QUEUE) {
            Some(v) => match v.trim().to_ascii_lowercase().as_str() {
                "true" => true,
                "false" => false,
                _ => {
                    return Err(SqsError::new(
                        ErrorCode::InvalidAttributeValue,
                        "Invalid value for the parameter FifoQueue.",
                    ));
                }
            },
            None => false,
        };
        if is_fifo_name(name) && !fifo_requested {
            // AWS rejects `.fifo` names unless FifoQueue=true is passed.
            validate_queue_name(name, false)?;
        }
        validate_queue_name(name, fifo_requested)?;
        let requested = QueueAttributes::for_create(fifo_requested, attributes)?;
        validate_tags(tags)?;
        if tags.len() > MAX_TAGS {
            return Err(SqsError::invalid_parameter("Too many tags: a queue can have at most 50 tags."));
        }
        let arn = queue_arn(self.region(), account_id, name);
        self.validate_redrive(&arn, fifo_requested, &requested).await?;

        let dlq_target = requested.redrive_policy.as_ref().map(|p| p.dead_letter_target_arn.clone());
        let inserted: Option<QueueRow> = sqlx::query_as(&format!(
            "INSERT INTO qd_queues (account_id, name, fifo, attributes, tags, dlq_target_arn)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (account_id, name) DO NOTHING
             RETURNING {QUEUE_COLUMNS}"
        ))
        .bind(account_id)
        .bind(name)
        .bind(fifo_requested)
        .bind(Json(&requested))
        .bind(Json(tags))
        .bind(dlq_target)
        .fetch_optional(self.pool())
        .await?;

        let qref = QueueRef::new(account_id, name);
        self.invalidate(&qref);
        if let Some(row) = inserted {
            metrics::counter!("qd_queues_created_total").increment(1);
            return Ok(Arc::new(row.into_queue(self.region())));
        }
        // Already exists: only succeed if every requested attribute matches.
        let existing = self.queue_fresh(&qref).await?;
        if let Some(conflict) = existing.attributes.first_conflict(&requested, attributes) {
            return Err(SqsError::new(
                ErrorCode::QueueNameExists,
                format!("A queue already exists with the same name and a different value for attribute {conflict}"),
            ));
        }
        Ok(existing)
    }

    /// Validates a redrive policy: the DLQ must exist, match the queue type and allow us.
    async fn validate_redrive(&self, source_arn: &str, fifo: bool, attrs: &QueueAttributes) -> Result<()> {
        let Some(policy) = &attrs.redrive_policy else {
            return Ok(());
        };
        let invalid = |reason: &str| {
            SqsError::new(
                ErrorCode::InvalidParameterValue,
                format!("Value {} for parameter RedrivePolicy is invalid. Reason: {reason}", policy.render()),
            )
        };
        let target =
            QueueArn::parse(&policy.dead_letter_target_arn).ok_or_else(|| invalid("Invalid deadLetterTargetArn."))?;
        if policy.dead_letter_target_arn == source_arn {
            return Err(invalid("A queue cannot be its own dead-letter queue."));
        }
        let dlq = match self.queue(&QueueRef::new(&target.account_id, &target.name)).await {
            Ok(q) => q,
            Err(e) if e.code == ErrorCode::QueueDoesNotExist => {
                return Err(invalid("Dead letter target does not exist."));
            }
            Err(e) => return Err(e),
        };
        if dlq.fifo != fifo {
            return Err(invalid(if fifo {
                "Dead-letter queue for a FIFO queue must also be a FIFO queue."
            } else {
                "Dead-letter queue for a standard queue must also be a standard queue."
            }));
        }
        if let Some(allow) = &dlq.attributes.redrive_allow_policy
            && !allow.allows(source_arn)
        {
            return Err(invalid("Dead-letter queue redrive allow policy does not permit this source queue."));
        }
        Ok(())
    }

    /// DeleteQueue. Messages are reclaimed asynchronously by the janitor.
    pub async fn delete_queue(&self, queue: &QueueRef) -> Result<()> {
        let mut tx = self.pool().begin().await?;
        let deleted: Option<(i64,)> =
            sqlx::query_as("DELETE FROM qd_queues WHERE account_id = $1 AND name = $2 RETURNING id")
                .bind(&queue.account_id)
                .bind(&queue.name)
                .fetch_optional(&mut *tx)
                .await?;
        let Some((id,)) = deleted else {
            return Err(SqsError::queue_does_not_exist());
        };
        sqlx::query("INSERT INTO qd_deleted_queues (queue_id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE qd_move_tasks SET status = 'CANCELLED', updated_at = now() WHERE source_queue_id = $1 AND status IN ('RUNNING', 'CANCELLING')")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(crate::notify::CHANNEL)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        self.invalidate(queue);
        self.inner.notifier.forget(id);
        metrics::counter!("qd_queues_deleted_total").increment(1);
        Ok(())
    }

    /// ListQueues for one account.
    pub async fn list_queues(
        &self,
        account_id: &str,
        prefix: Option<&str>,
        max_results: Option<i64>,
        next_token: Option<&str>,
    ) -> Result<ListPage<QueueRef>> {
        let max_results = validate_max_results(max_results)?;
        let after = next_token.filter(|t| !t.is_empty()).map(decode_token).transpose()?;
        let limit = max_results.unwrap_or(1000);
        let pattern =
            format!("{}%", prefix.unwrap_or("").replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"));
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM qd_queues
             WHERE account_id = $1 AND name LIKE $2 AND ($3::text IS NULL OR name > $3)
             ORDER BY name LIMIT $4",
        )
        .bind(account_id)
        .bind(pattern)
        .bind(after)
        .bind(limit + 1)
        .fetch_all(self.pool())
        .await?;
        page(rows.into_iter().map(|(n,)| n).collect(), limit, max_results.is_some(), |name| {
            QueueRef::new(account_id, name)
        })
    }

    /// ListDeadLetterSourceQueues.
    pub async fn list_dead_letter_source_queues(
        &self,
        queue: &QueueRef,
        max_results: Option<i64>,
        next_token: Option<&str>,
    ) -> Result<ListPage<QueueRef>> {
        let q = self.queue_fresh(queue).await?;
        let max_results = validate_max_results(max_results)?;
        let after = next_token.filter(|t| !t.is_empty()).map(decode_token).transpose()?;
        let limit = max_results.unwrap_or(1000);
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT account_id, name FROM qd_queues
             WHERE dlq_target_arn = $1 AND ($2::text IS NULL OR name > $2)
             ORDER BY name LIMIT $3",
        )
        .bind(&q.arn)
        .bind(after)
        .bind(limit + 1)
        .fetch_all(self.pool())
        .await?;
        let mut accounts: BTreeMap<String, String> = BTreeMap::new();
        let names = rows
            .into_iter()
            .map(|(acct, name)| {
                accounts.insert(name.clone(), acct);
                name
            })
            .collect();
        page(names, limit, max_results.is_some(), |name| {
            let acct = accounts.get(&name).cloned().unwrap_or_default();
            QueueRef::new(acct, name)
        })
    }

    /// GetQueueAttributes. Unknown names are rejected; an empty request returns nothing.
    pub async fn get_queue_attributes(
        &self,
        queue: &QueueRef,
        requested: &[String],
    ) -> Result<BTreeMap<String, String>> {
        for name in requested {
            if !attr::GETTABLE.contains(&name.as_str()) {
                return Err(SqsError::new(ErrorCode::InvalidAttributeName, format!("Unknown Attribute {name}.")));
            }
        }
        let q = self.queue_fresh(queue).await?;
        let all = requested.iter().any(|n| n == attr::ALL);
        let wants = |name: &str| all || requested.iter().any(|n| n == name);

        let mut out = BTreeMap::new();
        for (name, value) in q.attributes.render() {
            if wants(name) {
                out.insert(name.to_string(), value);
            }
        }
        if wants(attr::QUEUE_ARN) {
            out.insert(attr::QUEUE_ARN.into(), q.arn.clone());
        }
        if wants(attr::CREATED_TIMESTAMP) {
            out.insert(attr::CREATED_TIMESTAMP.into(), q.created_at.timestamp().to_string());
        }
        if wants(attr::LAST_MODIFIED_TIMESTAMP) {
            out.insert(attr::LAST_MODIFIED_TIMESTAMP.into(), q.updated_at.timestamp().to_string());
        }
        let needs_counts = [
            attr::APPROXIMATE_NUMBER_OF_MESSAGES,
            attr::APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
            attr::APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED,
        ]
        .iter()
        .any(|n| wants(n));
        if needs_counts {
            let counts = self.queue_counts(q.id).await?;
            if wants(attr::APPROXIMATE_NUMBER_OF_MESSAGES) {
                out.insert(attr::APPROXIMATE_NUMBER_OF_MESSAGES.into(), counts.visible.to_string());
            }
            if wants(attr::APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE) {
                out.insert(attr::APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE.into(), counts.in_flight.to_string());
            }
            if wants(attr::APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED) {
                out.insert(attr::APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED.into(), counts.delayed.to_string());
            }
        }
        Ok(out)
    }

    pub async fn queue_counts(&self, queue_id: i64) -> Result<QueueCounts> {
        let (visible, in_flight, delayed): (i64, i64, i64) = sqlx::query_as(
            "SELECT
                count(*) FILTER (WHERE visible_at <= now()),
                count(*) FILTER (WHERE visible_at > now() AND receive_count > 0),
                count(*) FILTER (WHERE visible_at > now() AND receive_count = 0)
             FROM qd_messages WHERE queue_id = $1 AND expires_at > now()",
        )
        .bind(queue_id)
        .fetch_one(self.pool())
        .await?;
        Ok(QueueCounts { visible, in_flight, delayed })
    }

    /// SetQueueAttributes.
    pub async fn set_queue_attributes(&self, queue: &QueueRef, input: &BTreeMap<String, String>) -> Result<()> {
        let q = self.queue_fresh(queue).await?;
        let mut attrs = q.attributes.clone();
        attrs.apply(input, false)?;
        if input.contains_key(attr::REDRIVE_POLICY) {
            self.validate_redrive(&q.arn, q.fifo, &attrs).await?;
        }
        let dlq_target = attrs.redrive_policy.as_ref().map(|p| p.dead_letter_target_arn.clone());
        let updated =
            sqlx::query("UPDATE qd_queues SET attributes = $2, dlq_target_arn = $3, updated_at = now() WHERE id = $1")
                .bind(q.id)
                .bind(Json(&attrs))
                .bind(dlq_target)
                .execute(self.pool())
                .await?;
        self.invalidate(queue);
        if updated.rows_affected() == 0 {
            return Err(SqsError::queue_does_not_exist());
        }
        Ok(())
    }

    /// PurgeQueue: deletes every message currently in the queue.
    pub async fn purge_queue(&self, queue: &QueueRef) -> Result<()> {
        let q = self.queue_fresh(queue).await?;
        let mut tx = self.pool().begin().await?;
        let cooldown = self.config().purge_cooldown.as_secs_f64();
        let claimed: Option<(i64,)> = sqlx::query_as(
            "UPDATE qd_queues SET last_purged_at = now()
             WHERE id = $1 AND (last_purged_at IS NULL OR last_purged_at <= now() - make_interval(secs => $2))
             RETURNING id",
        )
        .bind(q.id)
        .bind(cooldown)
        .fetch_optional(&mut *tx)
        .await?;
        if claimed.is_none() {
            return Err(SqsError::new(
                ErrorCode::PurgeQueueInProgress,
                format!("Only one PurgeQueue operation on {} is allowed every 60 seconds.", q.name),
            ));
        }
        sqlx::query("DELETE FROM qd_messages WHERE queue_id = $1").bind(q.id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM qd_fifo_groups WHERE queue_id = $1").bind(q.id).execute(&mut *tx).await?;
        tx.commit().await?;
        metrics::counter!("qd_queues_purged_total").increment(1);
        Ok(())
    }

    pub async fn tag_queue(&self, queue: &QueueRef, tags: &BTreeMap<String, String>) -> Result<()> {
        validate_tags(tags)?;
        let q = self.queue_fresh(queue).await?;
        let mut merged = q.tags.clone();
        merged.extend(tags.iter().map(|(k, v)| (k.clone(), v.clone())));
        if merged.len() > MAX_TAGS {
            return Err(SqsError::invalid_parameter("Too many tags: a queue can have at most 50 tags."));
        }
        // jsonb `||` merges atomically so concurrent TagQueue calls don't clobber each other.
        sqlx::query("UPDATE qd_queues SET tags = tags || $2 WHERE id = $1")
            .bind(q.id)
            .bind(Json(tags))
            .execute(self.pool())
            .await?;
        self.invalidate(queue);
        Ok(())
    }

    pub async fn untag_queue(&self, queue: &QueueRef, keys: &[String]) -> Result<()> {
        let q = self.queue_fresh(queue).await?;
        sqlx::query("UPDATE qd_queues SET tags = tags - $2::text[] WHERE id = $1")
            .bind(q.id)
            .bind(keys)
            .execute(self.pool())
            .await?;
        self.invalidate(queue);
        Ok(())
    }

    pub async fn list_queue_tags(&self, queue: &QueueRef) -> Result<BTreeMap<String, String>> {
        Ok(self.queue_fresh(queue).await?.tags.clone())
    }

    /// AddPermission: records a statement in the queue policy (not enforced; see docs).
    pub async fn add_permission(&self, queue: &QueueRef, permission: &Permission) -> Result<()> {
        if permission.label.is_empty()
            || permission.label.len() > 80
            || !permission.label.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(SqsError::invalid_value(
                "Label",
                &permission.label,
                "Label can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length.",
            ));
        }
        if permission.account_ids.is_empty() {
            return Err(SqsError::missing("AWSAccountIds"));
        }
        if permission.actions.is_empty() {
            return Err(SqsError::missing("Actions"));
        }
        let q = self.queue_fresh(queue).await?;
        let mut policy: serde_json::Value = match &q.attributes.policy {
            Some(p) => serde_json::from_str(p).unwrap_or_else(|_| serde_json::json!({})),
            None => serde_json::json!({
                "Version": "2012-10-17",
                "Id": format!("{}/SQSDefaultPolicy", q.arn),
                "Statement": [],
            }),
        };
        let statements = policy
            .as_object_mut()
            .map(|o| o.entry("Statement").or_insert_with(|| serde_json::json!([])))
            .and_then(|s| s.as_array_mut())
            .ok_or_else(|| SqsError::internal("Existing queue policy is malformed."))?;
        if statements.iter().any(|s| s.get("Sid").and_then(|v| v.as_str()) == Some(&permission.label)) {
            return Err(SqsError::invalid_value("Label", &permission.label, "Already exists."));
        }
        let principals: Vec<String> = permission.account_ids.iter().map(|a| format!("arn:aws:iam::{a}:root")).collect();
        let actions: Vec<String> = permission
            .actions
            .iter()
            .map(|a| if a == "*" { "SQS:*".to_string() } else { format!("SQS:{a}") })
            .collect();
        statements.push(serde_json::json!({
            "Sid": permission.label,
            "Effect": "Allow",
            "Principal": { "AWS": principals },
            "Action": actions,
            "Resource": q.arn,
        }));
        self.store_policy(&q, Some(policy.to_string())).await
    }

    pub async fn remove_permission(&self, queue: &QueueRef, label: &str) -> Result<()> {
        let q = self.queue_fresh(queue).await?;
        let not_found = || SqsError::invalid_value("Label", label, "can't find label on existing policy.");
        let mut policy: serde_json::Value =
            q.attributes.policy.as_deref().and_then(|p| serde_json::from_str(p).ok()).ok_or_else(not_found)?;
        let statements = policy.get_mut("Statement").and_then(|s| s.as_array_mut()).ok_or_else(not_found)?;
        let before = statements.len();
        statements.retain(|s| s.get("Sid").and_then(|v| v.as_str()) != Some(label));
        if statements.len() == before {
            return Err(not_found());
        }
        let new_policy = if statements.is_empty() { None } else { Some(policy.to_string()) };
        self.store_policy(&q, new_policy).await
    }

    async fn store_policy(&self, q: &Queue, policy: Option<String>) -> Result<()> {
        let mut attrs = q.attributes.clone();
        attrs.policy = policy;
        sqlx::query("UPDATE qd_queues SET attributes = $2, updated_at = now() WHERE id = $1")
            .bind(q.id)
            .bind(Json(&attrs))
            .execute(self.pool())
            .await?;
        self.invalidate(&q.queue_ref());
        Ok(())
    }

    /// Returns every queue (used for metrics). Not exposed through the SQS API.
    pub async fn all_queues(&self) -> Result<Vec<Queue>> {
        let rows: Vec<QueueRow> =
            sqlx::query_as(&format!("SELECT {QUEUE_COLUMNS} FROM qd_queues ORDER BY account_id, name"))
                .fetch_all(self.pool())
                .await?;
        Ok(rows.into_iter().map(|r| r.into_queue(self.region())).collect())
    }
}

fn page<T>(
    mut names: Vec<String>,
    limit: i64,
    paginate: bool,
    to_item: impl FnMut(String) -> T,
) -> Result<ListPage<T>> {
    let mut next_token = None;
    if names.len() as i64 > limit {
        names.truncate(limit as usize);
        if paginate {
            next_token = names.last().map(|n| encode_token(n));
        }
    }
    Ok(ListPage { items: names.into_iter().map(to_item).collect(), next_token })
}
