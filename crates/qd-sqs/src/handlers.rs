//! SQS actions: typed requests (JSON shape) mapped onto engine calls.

use std::collections::BTreeMap;

use qd_engine::engine::{
    BatchResult, ChangeVisibilityEntry, DeleteEntry, MoveTask, Permission, ReceiveInput, SendMessageInput,
    SendMessageOutput,
};
use qd_engine::message::{MessageAttributes, ReceivedMessage, md5_of_attributes, system_names};
use qd_engine::names::QueueRef;
use qd_engine::{Caller, Engine, ErrorCode, SqsError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value, json};

/// Everything an action needs besides its input.
pub struct Ctx<'a> {
    pub engine: &'a Engine,
    pub caller: &'a Caller,
    /// Base URL used to build queue URLs, e.g. `http://sqs.internal:9324`.
    pub base_url: &'a str,
    /// Queue addressed by the request path (Query protocol clients post to the queue URL).
    pub path_queue: Option<QueueRef>,
    /// Whether callers may address queues owned by other accounts.
    pub allow_cross_account: bool,
}

impl Ctx<'_> {
    fn queue_url(&self, q: &QueueRef) -> String {
        format!("{}/{}/{}", self.base_url.trim_end_matches('/'), q.account_id, q.name)
    }

    fn resolve(&self, queue_url: Option<&str>) -> Result<QueueRef, SqsError> {
        let q = match queue_url.filter(|u| !u.is_empty()) {
            Some(url) => QueueRef::from_url(url)?,
            None => self.path_queue.clone().ok_or_else(|| SqsError::missing("QueueUrl"))?,
        };
        self.check_account(&q.account_id)?;
        Ok(q)
    }

    fn check_account(&self, account: &str) -> Result<(), SqsError> {
        if !self.allow_cross_account && account != self.caller.account_id {
            return Err(SqsError::new(ErrorCode::AccessDenied, format!("Access to the resource {account} is denied.")));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Lenient scalar decoding: Query sends everything as strings, JSON sends numbers.

fn de_opt_i64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Num {
        Int(i64),
        Float(f64),
        Str(String),
    }
    match Option::<Num>::deserialize(d)? {
        None => Ok(None),
        Some(Num::Int(i)) => Ok(Some(i)),
        Some(Num::Float(f)) if f.fract() == 0.0 => Ok(Some(f as i64)),
        Some(Num::Float(f)) => Err(serde::de::Error::custom(format!("{f} is not an integer"))),
        Some(Num::Str(s)) => {
            s.trim().parse().map(Some).map_err(|_| serde::de::Error::custom(format!("{s} is not a valid integer")))
        }
    }
}

fn de_string_map<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<String, String>, D::Error> {
    let raw: Option<BTreeMap<String, Value>> = Option::deserialize(d)?;
    Ok(raw
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            let v = match v {
                Value::String(s) => s,
                Value::Null => String::new(),
                other => other.to_string(),
            };
            (k, v)
        })
        .collect())
}

fn de_vec<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Vec<T>, D::Error> {
    Ok(Option::<Vec<T>>::deserialize(d)?.unwrap_or_default())
}

fn de_attrs<'de, D: Deserializer<'de>>(d: D) -> Result<MessageAttributes, D::Error> {
    Ok(Option::<MessageAttributes>::deserialize(d)?.unwrap_or_default())
}

fn parse<T: DeserializeOwned>(input: Value) -> Result<T, SqsError> {
    serde_json::from_value(input).map_err(|e| {
        tracing::debug!(error = %e, "request did not match the operation's input shape");
        SqsError::invalid_parameter("The request has an invalid parameter type or structure.")
    })
}

fn require<'a>(value: &'a Option<String>, name: &str) -> Result<&'a str, SqsError> {
    value.as_deref().ok_or_else(|| SqsError::missing(name))
}

// ---------------------------------------------------------------------------------------------
// Requests.

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateQueueReq {
    queue_name: Option<String>,
    #[serde(default, deserialize_with = "de_string_map")]
    attributes: BTreeMap<String, String>,
    #[serde(default, rename = "tags", deserialize_with = "de_string_map")]
    tags: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct QueueUrlReq {
    queue_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetQueueUrlReq {
    queue_name: Option<String>,
    #[serde(rename = "QueueOwnerAWSAccountId")]
    queue_owner_aws_account_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListQueuesReq {
    queue_name_prefix: Option<String>,
    next_token: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    max_results: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListDlqSourcesReq {
    queue_url: Option<String>,
    next_token: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    max_results: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetQueueAttributesReq {
    queue_url: Option<String>,
    #[serde(default, deserialize_with = "de_vec")]
    attribute_names: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SetQueueAttributesReq {
    queue_url: Option<String>,
    #[serde(default, deserialize_with = "de_string_map")]
    attributes: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TagQueueReq {
    queue_url: Option<String>,
    #[serde(default, deserialize_with = "de_string_map")]
    tags: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct UntagQueueReq {
    queue_url: Option<String>,
    #[serde(default, deserialize_with = "de_vec")]
    tag_keys: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AddPermissionReq {
    queue_url: Option<String>,
    label: Option<String>,
    #[serde(default, rename = "AWSAccountIds", deserialize_with = "de_vec")]
    aws_account_ids: Vec<String>,
    #[serde(default, deserialize_with = "de_vec")]
    actions: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RemovePermissionReq {
    queue_url: Option<String>,
    label: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SendMessageReq {
    queue_url: Option<String>,
    #[serde(flatten)]
    message: SendEntry,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SendEntry {
    id: Option<String>,
    message_body: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    delay_seconds: Option<i64>,
    #[serde(default, deserialize_with = "de_attrs")]
    message_attributes: MessageAttributes,
    #[serde(default, deserialize_with = "de_attrs")]
    message_system_attributes: MessageAttributes,
    message_group_id: Option<String>,
    message_deduplication_id: Option<String>,
}

impl SendEntry {
    /// A missing body becomes empty and is rejected by the engine with MissingParameter.
    fn into_input(self) -> SendMessageInput {
        SendMessageInput {
            body: self.message_body.unwrap_or_default(),
            delay_seconds: self.delay_seconds,
            message_attributes: self.message_attributes,
            system_attributes: self.message_system_attributes,
            group_id: self.message_group_id,
            dedup_id: self.message_deduplication_id,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SendBatchReq {
    queue_url: Option<String>,
    #[serde(default, deserialize_with = "de_vec")]
    entries: Vec<SendEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ReceiveReq {
    queue_url: Option<String>,
    #[serde(default, deserialize_with = "de_vec")]
    attribute_names: Vec<String>,
    #[serde(default, deserialize_with = "de_vec")]
    message_system_attribute_names: Vec<String>,
    #[serde(default, deserialize_with = "de_vec")]
    message_attribute_names: Vec<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    max_number_of_messages: Option<i64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    visibility_timeout: Option<i64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    wait_time_seconds: Option<i64>,
    receive_request_attempt_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct DeleteReq {
    queue_url: Option<String>,
    receipt_handle: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct HandleEntry {
    id: Option<String>,
    receipt_handle: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    visibility_timeout: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct HandleBatchReq {
    queue_url: Option<String>,
    #[serde(default, deserialize_with = "de_vec")]
    entries: Vec<HandleEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ChangeVisibilityReq {
    queue_url: Option<String>,
    receipt_handle: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    visibility_timeout: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StartMoveTaskReq {
    source_arn: Option<String>,
    destination_arn: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    max_number_of_messages_per_second: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CancelMoveTaskReq {
    task_handle: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListMoveTasksReq {
    source_arn: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    max_results: Option<i64>,
}

// ---------------------------------------------------------------------------------------------
// Responses.

fn send_output_json(out: &SendMessageOutput) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("MessageId".into(), json!(out.message_id));
    m.insert("MD5OfMessageBody".into(), json!(out.md5_of_body));
    if let Some(v) = &out.md5_of_message_attributes {
        m.insert("MD5OfMessageAttributes".into(), json!(v));
    }
    if let Some(v) = &out.md5_of_system_attributes {
        m.insert("MD5OfMessageSystemAttributes".into(), json!(v));
    }
    if let Some(v) = &out.sequence_number {
        m.insert("SequenceNumber".into(), json!(v));
    }
    m
}

fn error_entry(id: &str, err: &SqsError) -> Value {
    json!({
        "Id": id,
        "SenderFault": err.code.sender_fault(),
        "Code": err.code.query_code(),
        "Message": err.message,
    })
}

fn batch_json<T>(results: BatchResult<T>, mut ok: impl FnMut(&str, T) -> Value) -> Value {
    let mut successful = Vec::new();
    let mut failed = Vec::new();
    for (id, r) in results {
        match r {
            Ok(v) => successful.push(ok(&id, v)),
            Err(e) => failed.push(error_entry(&id, &e)),
        }
    }
    json!({ "Successful": successful, "Failed": failed })
}

fn message_json(m: &ReceivedMessage, system_names: &[String], attribute_names: &[String]) -> Value {
    let mut obj = Map::new();
    obj.insert("MessageId".into(), json!(m.message_id));
    obj.insert("ReceiptHandle".into(), json!(m.receipt_handle));
    obj.insert("MD5OfBody".into(), json!(m.md5_of_body));
    obj.insert("Body".into(), json!(m.body));
    let system = m.filter_system_attributes(system_names);
    if !system.is_empty() {
        obj.insert("Attributes".into(), json!(system));
    }
    let attrs = m.filter_message_attributes(attribute_names);
    if let Some(md5) = md5_of_attributes(&attrs) {
        obj.insert("MD5OfMessageAttributes".into(), json!(md5));
        obj.insert("MessageAttributes".into(), serde_json::to_value(&attrs).unwrap_or(Value::Null));
    }
    Value::Object(obj)
}

fn move_task_json(t: &MoveTask) -> Value {
    let mut obj = Map::new();
    if let Some(h) = &t.task_handle {
        obj.insert("TaskHandle".into(), json!(h));
    }
    obj.insert("Status".into(), json!(t.status.as_str()));
    obj.insert("SourceArn".into(), json!(t.source_arn));
    if let Some(d) = &t.destination_arn {
        obj.insert("DestinationArn".into(), json!(d));
    }
    if let Some(r) = t.max_number_of_messages_per_second {
        obj.insert("MaxNumberOfMessagesPerSecond".into(), json!(r));
    }
    obj.insert("ApproximateNumberOfMessagesMoved".into(), json!(t.approximate_number_of_messages_moved));
    if let Some(n) = t.approximate_number_of_messages_to_move {
        obj.insert("ApproximateNumberOfMessagesToMove".into(), json!(n));
    }
    if let Some(f) = &t.failure_reason {
        obj.insert("FailureReason".into(), json!(f));
    }
    obj.insert("StartedTimestamp".into(), json!(t.started_timestamp));
    Value::Object(obj)
}

// ---------------------------------------------------------------------------------------------
// Dispatch.

pub const ACTIONS: &[&str] = &[
    "CreateQueue",
    "DeleteQueue",
    "GetQueueUrl",
    "ListQueues",
    "GetQueueAttributes",
    "SetQueueAttributes",
    "PurgeQueue",
    "SendMessage",
    "SendMessageBatch",
    "ReceiveMessage",
    "DeleteMessage",
    "DeleteMessageBatch",
    "ChangeMessageVisibility",
    "ChangeMessageVisibilityBatch",
    "TagQueue",
    "UntagQueue",
    "ListQueueTags",
    "AddPermission",
    "RemovePermission",
    "ListDeadLetterSourceQueues",
    "StartMessageMoveTask",
    "CancelMessageMoveTask",
    "ListMessageMoveTasks",
];

pub async fn dispatch(ctx: &Ctx<'_>, action: &str, input: Value) -> Result<Value, SqsError> {
    let engine = ctx.engine;
    match action {
        "CreateQueue" => {
            let req: CreateQueueReq = parse(input)?;
            let name = require(&req.queue_name, "QueueName")?;
            let q = engine.create_queue(&ctx.caller.account_id, name, &req.attributes, &req.tags).await?;
            Ok(json!({ "QueueUrl": ctx.queue_url(&q.queue_ref()) }))
        }
        "DeleteQueue" => {
            let req: QueueUrlReq = parse(input)?;
            engine.delete_queue(&ctx.resolve(req.queue_url.as_deref())?).await?;
            Ok(json!({}))
        }
        "GetQueueUrl" => {
            let req: GetQueueUrlReq = parse(input)?;
            let name = require(&req.queue_name, "QueueName")?;
            let account = req.queue_owner_aws_account_id.unwrap_or_else(|| ctx.caller.account_id.clone());
            ctx.check_account(&account)?;
            let q = engine.queue_fresh(&QueueRef::new(account, name)).await?;
            Ok(json!({ "QueueUrl": ctx.queue_url(&q.queue_ref()) }))
        }
        "ListQueues" => {
            let req: ListQueuesReq = parse(input)?;
            let page = engine
                .list_queues(
                    &ctx.caller.account_id,
                    req.queue_name_prefix.as_deref(),
                    req.max_results,
                    req.next_token.as_deref(),
                )
                .await?;
            let mut out = Map::new();
            if !page.items.is_empty() {
                let urls: Vec<String> = page.items.iter().map(|q| ctx.queue_url(q)).collect();
                out.insert("QueueUrls".into(), json!(urls));
            }
            if let Some(t) = page.next_token {
                out.insert("NextToken".into(), json!(t));
            }
            Ok(Value::Object(out))
        }
        "ListDeadLetterSourceQueues" => {
            let req: ListDlqSourcesReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let page = engine.list_dead_letter_source_queues(&q, req.max_results, req.next_token.as_deref()).await?;
            let urls: Vec<String> = page.items.iter().map(|q| ctx.queue_url(q)).collect();
            let mut out = Map::new();
            out.insert("queueUrls".into(), json!(urls));
            if let Some(t) = page.next_token {
                out.insert("NextToken".into(), json!(t));
            }
            Ok(Value::Object(out))
        }
        "GetQueueAttributes" => {
            let req: GetQueueAttributesReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let attrs = engine.get_queue_attributes(&q, &req.attribute_names).await?;
            if attrs.is_empty() {
                return Ok(json!({}));
            }
            Ok(json!({ "Attributes": attrs }))
        }
        "SetQueueAttributes" => {
            let req: SetQueueAttributesReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            engine.set_queue_attributes(&q, &req.attributes).await?;
            Ok(json!({}))
        }
        "PurgeQueue" => {
            let req: QueueUrlReq = parse(input)?;
            engine.purge_queue(&ctx.resolve(req.queue_url.as_deref())?).await?;
            Ok(json!({}))
        }
        "TagQueue" => {
            let req: TagQueueReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            engine.tag_queue(&q, &req.tags).await?;
            Ok(json!({}))
        }
        "UntagQueue" => {
            let req: UntagQueueReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            engine.untag_queue(&q, &req.tag_keys).await?;
            Ok(json!({}))
        }
        "ListQueueTags" => {
            let req: QueueUrlReq = parse(input)?;
            let tags = engine.list_queue_tags(&ctx.resolve(req.queue_url.as_deref())?).await?;
            if tags.is_empty() {
                return Ok(json!({}));
            }
            Ok(json!({ "Tags": tags }))
        }
        "AddPermission" => {
            let req: AddPermissionReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let permission = Permission {
                label: require(&req.label, "Label")?.to_string(),
                account_ids: req.aws_account_ids,
                actions: req.actions,
            };
            engine.add_permission(&q, &permission).await?;
            Ok(json!({}))
        }
        "RemovePermission" => {
            let req: RemovePermissionReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            engine.remove_permission(&q, require(&req.label, "Label")?).await?;
            Ok(json!({}))
        }
        "SendMessage" => {
            let req: SendMessageReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let out = engine.send_message(ctx.caller, &q, req.message.into_input()).await?;
            Ok(Value::Object(send_output_json(&out)))
        }
        "SendMessageBatch" => {
            let req: SendBatchReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let entries = req.entries.into_iter().map(|e| (e.id.clone().unwrap_or_default(), e.into_input())).collect();
            let results = engine.send_message_batch(ctx.caller, &q, entries).await?;
            Ok(batch_json(results, |id, out| {
                let mut m = send_output_json(&out);
                m.insert("Id".into(), json!(id));
                Value::Object(m)
            }))
        }
        "ReceiveMessage" => {
            let req: ReceiveReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let mut system_names_requested = req.attribute_names;
            system_names_requested.extend(req.message_system_attribute_names);
            for name in &system_names_requested {
                // Queue attribute names were historically accepted here; ignore those.
                if !system_names::KNOWN.contains(&name.as_str())
                    && !qd_engine::attributes::names::GETTABLE.contains(&name.as_str())
                {
                    return Err(SqsError::new(ErrorCode::InvalidAttributeName, format!("Unknown Attribute {name}.")));
                }
            }
            let messages = engine
                .receive_messages(
                    &q,
                    ReceiveInput {
                        max_number_of_messages: req.max_number_of_messages,
                        visibility_timeout: req.visibility_timeout,
                        wait_time_seconds: req.wait_time_seconds,
                        receive_request_attempt_id: req.receive_request_attempt_id,
                    },
                )
                .await?;
            if messages.is_empty() {
                return Ok(json!({}));
            }
            let list: Vec<Value> = messages
                .iter()
                .map(|m| message_json(m, &system_names_requested, &req.message_attribute_names))
                .collect();
            Ok(json!({ "Messages": list }))
        }
        "DeleteMessage" => {
            let req: DeleteReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            engine.delete_message(&q, require(&req.receipt_handle, "ReceiptHandle")?).await?;
            Ok(json!({}))
        }
        "DeleteMessageBatch" => {
            let req: HandleBatchReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let entries = req
                .entries
                .into_iter()
                .map(|e| DeleteEntry {
                    id: e.id.unwrap_or_default(),
                    receipt_handle: e.receipt_handle.unwrap_or_default(),
                })
                .collect();
            let results = engine.delete_message_batch(&q, entries).await?;
            Ok(batch_json(results, |id, ()| json!({ "Id": id })))
        }
        "ChangeMessageVisibility" => {
            let req: ChangeVisibilityReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let timeout = req.visibility_timeout.ok_or_else(|| SqsError::missing("VisibilityTimeout"))?;
            engine.change_message_visibility(&q, require(&req.receipt_handle, "ReceiptHandle")?, timeout).await?;
            Ok(json!({}))
        }
        "ChangeMessageVisibilityBatch" => {
            let req: HandleBatchReq = parse(input)?;
            let q = ctx.resolve(req.queue_url.as_deref())?;
            let entries = req
                .entries
                .into_iter()
                .map(|e| ChangeVisibilityEntry {
                    id: e.id.unwrap_or_default(),
                    receipt_handle: e.receipt_handle.unwrap_or_default(),
                    visibility_timeout: e.visibility_timeout.unwrap_or(-1),
                })
                .collect();
            let results = engine.change_message_visibility_batch(&q, entries).await?;
            Ok(batch_json(results, |id, ()| json!({ "Id": id })))
        }
        "StartMessageMoveTask" => {
            let req: StartMoveTaskReq = parse(input)?;
            let source = require(&req.source_arn, "SourceArn")?;
            if let Some(arn) = qd_engine::names::QueueArn::parse(source) {
                ctx.check_account(&arn.account_id)?;
            }
            let handle = engine
                .start_message_move_task(source, req.destination_arn.as_deref(), req.max_number_of_messages_per_second)
                .await?;
            Ok(json!({ "TaskHandle": handle }))
        }
        "CancelMessageMoveTask" => {
            let req: CancelMoveTaskReq = parse(input)?;
            let moved = engine.cancel_message_move_task(require(&req.task_handle, "TaskHandle")?).await?;
            Ok(json!({ "ApproximateNumberOfMessagesMoved": moved }))
        }
        "ListMessageMoveTasks" => {
            let req: ListMoveTasksReq = parse(input)?;
            let source = require(&req.source_arn, "SourceArn")?;
            if let Some(arn) = qd_engine::names::QueueArn::parse(source) {
                ctx.check_account(&arn.account_id)?;
            }
            let tasks = engine.list_message_move_tasks(source, req.max_results).await?;
            let results: Vec<Value> = tasks.iter().map(move_task_json).collect();
            Ok(json!({ "Results": results }))
        }
        other => {
            Err(SqsError::new(ErrorCode::InvalidAction, format!("The action {other} is not valid for this endpoint.")))
        }
    }
}
