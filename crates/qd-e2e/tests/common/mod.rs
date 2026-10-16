#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_sqs::Client;
use aws_sdk_sqs::error::ProvideErrorMetadata;
use aws_sdk_sqs::types::{Message, MessageSystemAttributeName, QueueAttributeName};

#[allow(unused_imports)]
pub use qd_e2e::{Harness, Options, qname};

/// Error code as reported by the SDK (works for every operation's error type).
pub fn code<E: ProvideErrorMetadata>(err: &E) -> String {
    err.code().unwrap_or("<none>").to_string()
}

pub async fn create(client: &Client, name: &str, attrs: &[(&str, &str)]) -> String {
    let mut req = client.create_queue().queue_name(name);
    for (k, v) in attrs {
        req = req.attributes(QueueAttributeName::from(*k), *v);
    }
    req.send().await.expect("create queue").queue_url().expect("queue url").to_string()
}

pub async fn send(client: &Client, url: &str, body: &str) -> String {
    client
        .send_message()
        .queue_url(url)
        .message_body(body)
        .send()
        .await
        .expect("send")
        .message_id()
        .unwrap()
        .to_string()
}

pub async fn send_fifo(client: &Client, url: &str, body: &str, group: &str, dedup: &str) -> String {
    client
        .send_message()
        .queue_url(url)
        .message_body(body)
        .message_group_id(group)
        .message_deduplication_id(dedup)
        .send()
        .await
        .expect("send fifo")
        .message_id()
        .unwrap()
        .to_string()
}

pub async fn receive(client: &Client, url: &str, max: i32, visibility: Option<i32>) -> Vec<Message> {
    let mut req = client
        .receive_message()
        .queue_url(url)
        .max_number_of_messages(max)
        .message_system_attribute_names(MessageSystemAttributeName::All)
        .message_attribute_names("All");
    if let Some(v) = visibility {
        req = req.visibility_timeout(v);
    }
    req.send().await.expect("receive").messages.unwrap_or_default()
}

pub async fn receive_wait(client: &Client, url: &str, max: i32, wait: i32, visibility: Option<i32>) -> Vec<Message> {
    let mut req = client
        .receive_message()
        .queue_url(url)
        .max_number_of_messages(max)
        .wait_time_seconds(wait)
        .message_system_attribute_names(MessageSystemAttributeName::All)
        .message_attribute_names("All");
    if let Some(v) = visibility {
        req = req.visibility_timeout(v);
    }
    req.send().await.expect("receive").messages.unwrap_or_default()
}

/// Receives until `n` messages are collected or the deadline passes.
pub async fn receive_n(client: &Client, url: &str, n: usize, visibility: i32, deadline: Duration) -> Vec<Message> {
    let start = std::time::Instant::now();
    let mut out = Vec::new();
    while out.len() < n && start.elapsed() < deadline {
        let batch = receive_wait(client, url, 10.min((n - out.len()) as i32), 1, Some(visibility)).await;
        out.extend(batch);
    }
    out
}

pub async fn delete(client: &Client, url: &str, m: &Message) {
    client.delete_message().queue_url(url).receipt_handle(m.receipt_handle().unwrap()).send().await.expect("delete");
}

pub async fn attrs(client: &Client, url: &str) -> HashMap<String, String> {
    client
        .get_queue_attributes()
        .queue_url(url)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await
        .expect("get attributes")
        .attributes
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k.as_str().to_string(), v))
        .collect()
}

pub async fn arn(client: &Client, url: &str) -> String {
    attrs(client, url).await["QueueArn"].clone()
}

pub fn sys(m: &Message, name: &str) -> Option<String> {
    m.attributes().and_then(|a| a.get(&MessageSystemAttributeName::from(name))).cloned()
}

pub fn bodies(msgs: &[Message]) -> Vec<String> {
    msgs.iter().map(|m| m.body().unwrap_or_default().to_string()).collect()
}
