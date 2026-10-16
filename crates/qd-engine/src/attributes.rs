//! Queue attributes: parsing, validation, defaults and rendering.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{ErrorCode, Result, SqsError};
use crate::names::QueueArn;

pub mod names {
    pub const ALL: &str = "All";
    pub const DELAY_SECONDS: &str = "DelaySeconds";
    pub const MAXIMUM_MESSAGE_SIZE: &str = "MaximumMessageSize";
    pub const MESSAGE_RETENTION_PERIOD: &str = "MessageRetentionPeriod";
    pub const RECEIVE_MESSAGE_WAIT_TIME_SECONDS: &str = "ReceiveMessageWaitTimeSeconds";
    pub const VISIBILITY_TIMEOUT: &str = "VisibilityTimeout";
    pub const REDRIVE_POLICY: &str = "RedrivePolicy";
    pub const REDRIVE_ALLOW_POLICY: &str = "RedriveAllowPolicy";
    pub const POLICY: &str = "Policy";
    pub const KMS_MASTER_KEY_ID: &str = "KmsMasterKeyId";
    pub const KMS_DATA_KEY_REUSE_PERIOD_SECONDS: &str = "KmsDataKeyReusePeriodSeconds";
    pub const SQS_MANAGED_SSE_ENABLED: &str = "SqsManagedSseEnabled";
    pub const FIFO_QUEUE: &str = "FifoQueue";
    pub const CONTENT_BASED_DEDUPLICATION: &str = "ContentBasedDeduplication";
    pub const DEDUPLICATION_SCOPE: &str = "DeduplicationScope";
    pub const FIFO_THROUGHPUT_LIMIT: &str = "FifoThroughputLimit";
    // Read-only.
    pub const APPROXIMATE_NUMBER_OF_MESSAGES: &str = "ApproximateNumberOfMessages";
    pub const APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE: &str = "ApproximateNumberOfMessagesNotVisible";
    pub const APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED: &str = "ApproximateNumberOfMessagesDelayed";
    pub const CREATED_TIMESTAMP: &str = "CreatedTimestamp";
    pub const LAST_MODIFIED_TIMESTAMP: &str = "LastModifiedTimestamp";
    pub const QUEUE_ARN: &str = "QueueArn";

    pub const READ_ONLY: &[&str] = &[
        APPROXIMATE_NUMBER_OF_MESSAGES,
        APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
        APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED,
        CREATED_TIMESTAMP,
        LAST_MODIFIED_TIMESTAMP,
        QUEUE_ARN,
    ];

    pub const FIFO_ONLY: &[&str] =
        &[FIFO_QUEUE, CONTENT_BASED_DEDUPLICATION, DEDUPLICATION_SCOPE, FIFO_THROUGHPUT_LIMIT];

    /// Every attribute name GetQueueAttributes accepts.
    pub const GETTABLE: &[&str] = &[
        ALL,
        DELAY_SECONDS,
        MAXIMUM_MESSAGE_SIZE,
        MESSAGE_RETENTION_PERIOD,
        RECEIVE_MESSAGE_WAIT_TIME_SECONDS,
        VISIBILITY_TIMEOUT,
        REDRIVE_POLICY,
        REDRIVE_ALLOW_POLICY,
        POLICY,
        KMS_MASTER_KEY_ID,
        KMS_DATA_KEY_REUSE_PERIOD_SECONDS,
        SQS_MANAGED_SSE_ENABLED,
        FIFO_QUEUE,
        CONTENT_BASED_DEDUPLICATION,
        DEDUPLICATION_SCOPE,
        FIFO_THROUGHPUT_LIMIT,
        APPROXIMATE_NUMBER_OF_MESSAGES,
        APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
        APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED,
        CREATED_TIMESTAMP,
        LAST_MODIFIED_TIMESTAMP,
        QUEUE_ARN,
    ];
}

pub const MAX_MESSAGE_SIZE: u32 = 1_048_576;
pub const MIN_MESSAGE_SIZE: u32 = 1_024;
pub const MAX_DELAY_SECONDS: u32 = 900;
pub const MAX_VISIBILITY_TIMEOUT: u32 = 43_200;
pub const MAX_WAIT_TIME_SECONDS: u32 = 20;
pub const MIN_RETENTION: u32 = 60;
pub const MAX_RETENTION: u32 = 1_209_600;
pub const MAX_RECEIVE_COUNT_LIMIT: u32 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedrivePolicy {
    pub dead_letter_target_arn: String,
    pub max_receive_count: u32,
}

impl RedrivePolicy {
    pub fn parse(raw: &str) -> Result<Self> {
        let invalid = |reason: &str| {
            SqsError::new(
                ErrorCode::InvalidAttributeValue,
                format!("Value {raw} for parameter RedrivePolicy is invalid. Reason: {reason}"),
            )
        };
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|_| invalid("Redrive policy is not a valid JSON map."))?;
        let obj = value.as_object().ok_or_else(|| invalid("Redrive policy is not a valid JSON map."))?;
        let arn = obj
            .get("deadLetterTargetArn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid("Redrive policy does not contain mandatory attribute: deadLetterTargetArn."))?;
        if QueueArn::parse(arn).is_none() {
            return Err(invalid("Invalid value for deadLetterTargetArn."));
        }
        let count = match obj.get("maxReceiveCount") {
            Some(serde_json::Value::Number(n)) => n.as_u64(),
            Some(serde_json::Value::String(s)) => s.trim().parse::<u64>().ok(),
            None => return Err(invalid("Redrive policy does not contain mandatory attribute: maxReceiveCount.")),
            _ => None,
        }
        .filter(|n| (1..=MAX_RECEIVE_COUNT_LIMIT as u64).contains(n))
        .ok_or_else(|| invalid("Invalid value for maxReceiveCount: must be an integer from 1 to 1000."))?;
        if let Some(extra) = obj.keys().find(|k| *k != "deadLetterTargetArn" && *k != "maxReceiveCount") {
            return Err(invalid(&format!(
                "Only following attributes are supported: [deadLetterTargetArn, maxReceiveCount], got {extra}."
            )));
        }
        Ok(Self { dead_letter_target_arn: arn.to_string(), max_receive_count: count as u32 })
    }

    pub fn render(&self) -> String {
        serde_json::json!({
            "deadLetterTargetArn": self.dead_letter_target_arn,
            "maxReceiveCount": self.max_receive_count,
        })
        .to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RedrivePermission {
    AllowAll,
    DenyAll,
    ByQueue(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedriveAllowPolicy {
    pub permission: RedrivePermission,
}

impl RedriveAllowPolicy {
    pub fn parse(raw: &str) -> Result<Self> {
        let invalid = |reason: &str| {
            SqsError::new(
                ErrorCode::InvalidAttributeValue,
                format!("Value {raw} for parameter RedriveAllowPolicy is invalid. Reason: {reason}"),
            )
        };
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|_| invalid("Redrive allow policy is not a valid JSON map."))?;
        let obj = value.as_object().ok_or_else(|| invalid("Redrive allow policy is not a valid JSON map."))?;
        let permission = obj
            .get("redrivePermission")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid("Missing redrivePermission."))?;
        let arns = obj.get("sourceQueueArns");
        let permission = match permission {
            "allowAll" | "denyAll" if arns.is_some() => {
                return Err(invalid("sourceQueueArns can only be specified when redrivePermission is byQueue."));
            }
            "allowAll" => RedrivePermission::AllowAll,
            "denyAll" => RedrivePermission::DenyAll,
            "byQueue" => {
                let list = arns
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| invalid("sourceQueueArns must be specified when redrivePermission is byQueue."))?;
                let arns: Vec<String> = list
                    .iter()
                    .map(|v| v.as_str().map(str::to_string))
                    .collect::<Option<_>>()
                    .ok_or_else(|| invalid("sourceQueueArns must be a list of strings."))?;
                if arns.is_empty() || arns.len() > 10 {
                    return Err(invalid("sourceQueueArns must contain between 1 and 10 ARNs."));
                }
                RedrivePermission::ByQueue(arns)
            }
            _ => return Err(invalid("redrivePermission must be one of allowAll, denyAll, byQueue.")),
        };
        Ok(Self { permission })
    }

    pub fn render(&self) -> String {
        match &self.permission {
            RedrivePermission::AllowAll => r#"{"redrivePermission":"allowAll"}"#.to_string(),
            RedrivePermission::DenyAll => r#"{"redrivePermission":"denyAll"}"#.to_string(),
            RedrivePermission::ByQueue(arns) => {
                serde_json::json!({"redrivePermission": "byQueue", "sourceQueueArns": arns}).to_string()
            }
        }
    }

    pub fn allows(&self, source_arn: &str) -> bool {
        match &self.permission {
            RedrivePermission::AllowAll => true,
            RedrivePermission::DenyAll => false,
            RedrivePermission::ByQueue(arns) => arns.iter().any(|a| a == source_arn),
        }
    }
}

/// Persisted, user-settable queue configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueAttributes {
    pub delay_seconds: u32,
    pub maximum_message_size: u32,
    pub message_retention_period: u32,
    pub receive_message_wait_time_seconds: u32,
    pub visibility_timeout: u32,
    pub redrive_policy: Option<RedrivePolicy>,
    pub redrive_allow_policy: Option<RedriveAllowPolicy>,
    pub policy: Option<String>,
    pub kms_master_key_id: Option<String>,
    pub kms_data_key_reuse_period_seconds: u32,
    pub sqs_managed_sse_enabled: bool,
    pub fifo_queue: bool,
    pub content_based_deduplication: bool,
    pub deduplication_scope: String,
    pub fifo_throughput_limit: String,
}

impl Default for QueueAttributes {
    fn default() -> Self {
        Self {
            delay_seconds: 0,
            maximum_message_size: MAX_MESSAGE_SIZE,
            message_retention_period: 345_600,
            receive_message_wait_time_seconds: 0,
            visibility_timeout: 30,
            redrive_policy: None,
            redrive_allow_policy: None,
            policy: None,
            kms_master_key_id: None,
            kms_data_key_reuse_period_seconds: 300,
            sqs_managed_sse_enabled: true,
            fifo_queue: false,
            content_based_deduplication: false,
            deduplication_scope: "queue".into(),
            fifo_throughput_limit: "perQueue".into(),
        }
    }
}

fn invalid_attr_value(name: &str) -> SqsError {
    SqsError::new(ErrorCode::InvalidAttributeValue, format!("Invalid value for the parameter {name}."))
}

fn unknown_attr(name: &str) -> SqsError {
    SqsError::new(ErrorCode::InvalidAttributeName, format!("Unknown Attribute {name}."))
}

fn parse_range(name: &str, value: &str, min: u32, max: u32) -> Result<u32> {
    value.trim().parse::<u32>().ok().filter(|v| (min..=max).contains(v)).ok_or_else(|| invalid_attr_value(name))
}

fn parse_bool(name: &str, value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid_attr_value(name)),
    }
}

impl QueueAttributes {
    /// Builds the attributes for a new queue. `fifo` must already reflect the `FifoQueue` input.
    pub fn for_create(fifo: bool, input: &BTreeMap<String, String>) -> Result<Self> {
        let mut attrs = Self { fifo_queue: fifo, ..Self::default() };
        attrs.apply(input, true)?;
        Ok(attrs)
    }

    /// Applies user-supplied attribute changes (CreateQueue or SetQueueAttributes).
    pub fn apply(&mut self, input: &BTreeMap<String, String>, creating: bool) -> Result<()> {
        use names::*;
        for (name, value) in input {
            let name = name.as_str();
            if FIFO_ONLY.contains(&name) && !self.fifo_queue {
                // FifoQueue=false is accepted when creating a standard queue.
                if creating && name == FIFO_QUEUE && !parse_bool(name, value)? {
                    continue;
                }
                return Err(unknown_attr(name));
            }
            match name {
                DELAY_SECONDS => self.delay_seconds = parse_range(name, value, 0, MAX_DELAY_SECONDS)?,
                MAXIMUM_MESSAGE_SIZE => {
                    self.maximum_message_size = parse_range(name, value, MIN_MESSAGE_SIZE, MAX_MESSAGE_SIZE)?
                }
                MESSAGE_RETENTION_PERIOD => {
                    self.message_retention_period = parse_range(name, value, MIN_RETENTION, MAX_RETENTION)?
                }
                RECEIVE_MESSAGE_WAIT_TIME_SECONDS => {
                    self.receive_message_wait_time_seconds = parse_range(name, value, 0, MAX_WAIT_TIME_SECONDS)?
                }
                VISIBILITY_TIMEOUT => self.visibility_timeout = parse_range(name, value, 0, MAX_VISIBILITY_TIMEOUT)?,
                REDRIVE_POLICY => {
                    self.redrive_policy =
                        if value.trim().is_empty() { None } else { Some(RedrivePolicy::parse(value)?) }
                }
                REDRIVE_ALLOW_POLICY => {
                    self.redrive_allow_policy =
                        if value.trim().is_empty() { None } else { Some(RedriveAllowPolicy::parse(value)?) }
                }
                POLICY => {
                    if value.trim().is_empty() {
                        self.policy = None;
                    } else {
                        let parsed: serde_json::Value =
                            serde_json::from_str(value).map_err(|_| invalid_attr_value(name))?;
                        if !parsed.is_object() {
                            return Err(invalid_attr_value(name));
                        }
                        self.policy = Some(value.clone());
                    }
                }
                KMS_MASTER_KEY_ID => {
                    if value.is_empty() {
                        self.kms_master_key_id = None;
                    } else {
                        self.kms_master_key_id = Some(value.clone());
                        self.sqs_managed_sse_enabled = false;
                    }
                }
                KMS_DATA_KEY_REUSE_PERIOD_SECONDS => {
                    self.kms_data_key_reuse_period_seconds = parse_range(name, value, 60, 86_400)?
                }
                SQS_MANAGED_SSE_ENABLED => {
                    let enabled = parse_bool(name, value)?;
                    if enabled && self.kms_master_key_id.is_some() && !input.contains_key(KMS_MASTER_KEY_ID) {
                        self.kms_master_key_id = None;
                    }
                    self.sqs_managed_sse_enabled = enabled;
                }
                FIFO_QUEUE => {
                    if !creating || !parse_bool(name, value)? {
                        // A FIFO queue cannot be converted (in either direction).
                        return Err(invalid_attr_value(name));
                    }
                }
                CONTENT_BASED_DEDUPLICATION => self.content_based_deduplication = parse_bool(name, value)?,
                DEDUPLICATION_SCOPE => match value.as_str() {
                    "queue" | "messageGroup" => self.deduplication_scope = value.clone(),
                    _ => return Err(invalid_attr_value(name)),
                },
                FIFO_THROUGHPUT_LIMIT => match value.as_str() {
                    "perQueue" | "perMessageGroupId" => self.fifo_throughput_limit = value.clone(),
                    _ => return Err(invalid_attr_value(name)),
                },
                _ => return Err(unknown_attr(name)),
            }
        }
        if self.fifo_queue
            && self.deduplication_scope == "messageGroup"
            && self.fifo_throughput_limit == "perQueue"
            && (input.contains_key(DEDUPLICATION_SCOPE) || input.contains_key(FIFO_THROUGHPUT_LIMIT))
        {
            return Err(SqsError::new(
                ErrorCode::InvalidAttributeValue,
                "Invalid value for the parameter FifoThroughputLimit. Reason: perQueue is not supported when DeduplicationScope is messageGroup.",
            ));
        }
        Ok(())
    }

    /// Renders the stored (non-computed) attributes as SQS attribute strings.
    pub fn render(&self) -> BTreeMap<&'static str, String> {
        use names::*;
        let mut out = BTreeMap::new();
        out.insert(DELAY_SECONDS, self.delay_seconds.to_string());
        out.insert(MAXIMUM_MESSAGE_SIZE, self.maximum_message_size.to_string());
        out.insert(MESSAGE_RETENTION_PERIOD, self.message_retention_period.to_string());
        out.insert(RECEIVE_MESSAGE_WAIT_TIME_SECONDS, self.receive_message_wait_time_seconds.to_string());
        out.insert(VISIBILITY_TIMEOUT, self.visibility_timeout.to_string());
        out.insert(SQS_MANAGED_SSE_ENABLED, self.sqs_managed_sse_enabled.to_string());
        if let Some(p) = &self.redrive_policy {
            out.insert(REDRIVE_POLICY, p.render());
        }
        if let Some(p) = &self.redrive_allow_policy {
            out.insert(REDRIVE_ALLOW_POLICY, p.render());
        }
        if let Some(p) = &self.policy {
            out.insert(POLICY, p.clone());
        }
        if let Some(k) = &self.kms_master_key_id {
            out.insert(KMS_MASTER_KEY_ID, k.clone());
            out.insert(KMS_DATA_KEY_REUSE_PERIOD_SECONDS, self.kms_data_key_reuse_period_seconds.to_string());
        }
        if self.fifo_queue {
            out.insert(FIFO_QUEUE, "true".into());
            out.insert(CONTENT_BASED_DEDUPLICATION, self.content_based_deduplication.to_string());
            out.insert(DEDUPLICATION_SCOPE, self.deduplication_scope.clone());
            out.insert(FIFO_THROUGHPUT_LIMIT, self.fifo_throughput_limit.clone());
        }
        out
    }

    /// For idempotent CreateQueue: returns the first requested attribute whose value differs.
    pub fn first_conflict(&self, requested: &Self, input: &BTreeMap<String, String>) -> Option<String> {
        let mine = self.render();
        let theirs = requested.render();
        input.keys().find(|name| mine.get(name.as_str()) != theirs.get(name.as_str())).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn defaults_and_ranges() {
        let a = QueueAttributes::for_create(false, &map(&[])).unwrap();
        assert_eq!(a.visibility_timeout, 30);
        assert_eq!(a.message_retention_period, 345_600);
        let a =
            QueueAttributes::for_create(false, &map(&[("DelaySeconds", "900"), ("VisibilityTimeout", "0")])).unwrap();
        assert_eq!(a.delay_seconds, 900);
        assert_eq!(a.visibility_timeout, 0);
        let e = QueueAttributes::for_create(false, &map(&[("DelaySeconds", "901")])).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidAttributeValue);
        let e = QueueAttributes::for_create(false, &map(&[("MessageRetentionPeriod", "59")])).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidAttributeValue);
        let e = QueueAttributes::for_create(false, &map(&[("Bogus", "1")])).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidAttributeName);
        let e = QueueAttributes::for_create(false, &map(&[("QueueArn", "x")])).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidAttributeName);
    }

    #[test]
    fn fifo_only_attributes() {
        let e = QueueAttributes::for_create(false, &map(&[("ContentBasedDeduplication", "true")])).unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidAttributeName);
        assert!(QueueAttributes::for_create(false, &map(&[("FifoQueue", "false")])).is_ok());
        let a =
            QueueAttributes::for_create(true, &map(&[("FifoQueue", "true"), ("ContentBasedDeduplication", "true")]))
                .unwrap();
        assert!(a.content_based_deduplication);
        let mut a2 = a.clone();
        assert!(a2.apply(&map(&[("FifoQueue", "true")]), false).is_err());
    }

    #[test]
    fn redrive_policy() {
        let p = RedrivePolicy::parse(r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:dlq","maxReceiveCount":"3"}"#)
            .unwrap();
        assert_eq!(p.max_receive_count, 3);
        assert_eq!(p.render(), r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:dlq","maxReceiveCount":3}"#);
        assert!(
            RedrivePolicy::parse(r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:1:dlq","maxReceiveCount":0}"#)
                .is_err()
        );
        assert!(RedrivePolicy::parse(r#"{"maxReceiveCount":2}"#).is_err());
        assert!(RedrivePolicy::parse("nope").is_err());
        let mut a = QueueAttributes::default();
        a.apply(&map(&[("RedrivePolicy", &p.render())]), false).unwrap();
        assert!(a.redrive_policy.is_some());
        a.apply(&map(&[("RedrivePolicy", "")]), false).unwrap();
        assert!(a.redrive_policy.is_none());
    }

    #[test]
    fn redrive_allow_policy() {
        let p = RedriveAllowPolicy::parse(r#"{"redrivePermission":"byQueue","sourceQueueArns":["arn:a"]}"#).unwrap();
        assert!(p.allows("arn:a"));
        assert!(!p.allows("arn:b"));
        assert!(!RedriveAllowPolicy::parse(r#"{"redrivePermission":"denyAll"}"#).unwrap().allows("x"));
        assert!(RedriveAllowPolicy::parse(r#"{"redrivePermission":"byQueue"}"#).is_err());
        assert!(RedriveAllowPolicy::parse(r#"{"redrivePermission":"allowAll","sourceQueueArns":["a"]}"#).is_err());
    }

    #[test]
    fn conflicts() {
        let input = map(&[("VisibilityTimeout", "60")]);
        let existing = QueueAttributes::default();
        let requested = QueueAttributes::for_create(false, &input).unwrap();
        assert_eq!(existing.first_conflict(&requested, &input).as_deref(), Some("VisibilityTimeout"));
        let input = map(&[("VisibilityTimeout", "30")]);
        let requested = QueueAttributes::for_create(false, &input).unwrap();
        assert_eq!(existing.first_conflict(&requested, &input), None);
    }
}
