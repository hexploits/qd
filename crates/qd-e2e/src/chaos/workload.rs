//! Continuous producer/consumer workload driven through the official AWS SDK, with bookkeeping
//! to prove delivery guarantees across failures:
//!
//! * **No acknowledged message is lost**: every message whose `SendMessage` returned success
//!   must eventually be received.
//! * **FIFO order holds**: within a message group, deliveries never go backwards (redeliveries of
//!   the same message are allowed — at-least-once — but never an older message after a newer).
//! * **Client-visible errors** (after the SDK's own retries) are counted with timestamps so a
//!   scenario can assert "zero errors" (rolling restarts) or "errors only during the outage".

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aws_credential_types::Credentials;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::retry::RetryConfig;
use aws_sdk_sqs::config::timeout::TimeoutConfig;
use aws_sdk_sqs::config::{BehaviorVersion, Region};
use aws_sdk_sqs::types::QueueAttributeName;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::log;

/// An SDK client configured like a production client: standard retry mode with backoff.
pub fn resilient_client(endpoint: &str, max_attempts: u32) -> Client {
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("AKIDCHAOS", "secret", None, None, "chaos"))
        .retry_config(
            RetryConfig::standard()
                .with_max_attempts(max_attempts)
                .with_initial_backoff(Duration::from_millis(100))
                .with_max_backoff(Duration::from_secs(2)),
        )
        .timeout_config(
            TimeoutConfig::builder()
                .connect_timeout(Duration::from_secs(2))
                .operation_attempt_timeout(Duration::from_secs(10))
                .operation_timeout(Duration::from_secs(60))
                .build(),
        )
        .build();
    Client::from_conf(config)
}

#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub queue_name: String,
    pub fifo: bool,
    pub producers: usize,
    pub consumers: usize,
    /// Pause between sends per producer.
    pub send_interval: Duration,
    pub visibility_timeout: i32,
    pub max_attempts: u32,
}

impl WorkloadConfig {
    pub fn standard(name: &str) -> Self {
        Self {
            queue_name: name.into(),
            fifo: false,
            producers: 3,
            consumers: 3,
            send_interval: Duration::from_millis(20),
            visibility_timeout: 5,
            max_attempts: 8,
        }
    }

    pub fn fifo(name: &str) -> Self {
        Self { queue_name: format!("{name}.fifo"), fifo: true, ..Self::standard(name) }
    }
}

#[derive(Default)]
struct Books {
    acked: HashSet<String>,
    /// Sends that failed after all retries: may or may not have been stored.
    unknown: HashSet<String>,
    received: HashMap<String, u32>,
    /// FIFO: delivery order of sequence numbers per group.
    group_log: HashMap<String, Vec<u64>>,
    errors: Vec<(Instant, String)>,
}

pub struct Workload {
    pub config: WorkloadConfig,
    pub queue_url: String,
    client: Client,
    books: Arc<Mutex<Books>>,
    sent: Arc<AtomicU64>,
    stop_producers: CancellationToken,
    stop_consumers: CancellationToken,
    producers: Vec<tokio::task::JoinHandle<()>>,
    consumers: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(Debug)]
pub struct Report {
    pub acked: usize,
    pub unknown: usize,
    pub received_unique: usize,
    pub duplicates: u64,
    pub lost: Vec<String>,
    pub fifo_violations: Vec<String>,
    pub errors: Vec<(Duration, String)>,
}

impl Report {
    /// Panics unless no acknowledged message was lost and FIFO order held.
    pub fn assert_no_loss_and_ordered(&self) {
        assert!(self.acked > 0, "workload acknowledged nothing");
        assert!(
            self.lost.is_empty(),
            "{} acknowledged messages were LOST (first: {:?})",
            self.lost.len(),
            &self.lost[..self.lost.len().min(10)]
        );
        assert!(self.fifo_violations.is_empty(), "FIFO order violated: {:?}", self.fifo_violations);
    }

    pub fn assert_no_client_errors(&self) {
        assert!(
            self.errors.is_empty(),
            "clients saw {} errors after retries: {:?}",
            self.errors.len(),
            &self.errors[..self.errors.len().min(10)]
        );
    }
}

/// FIFO guarantee check over one group's delivery log.
///
/// Redeliveries are legitimate (at-least-once: a batch whose deletes failed comes back, in order,
/// after its visibility timeout), so repeated sequence numbers are ignored. What must never happen
/// is a *first* delivery of a message older than one already delivered — that means the group's
/// order was broken.
pub fn fifo_violation(seqs: &[u64]) -> Option<String> {
    let mut seen = HashSet::new();
    let mut newest: Option<u64> = None;
    for &s in seqs {
        if seen.insert(s) {
            if let Some(n) = newest
                && s < n
            {
                return Some(format!("first delivery of {s} after {n} had already been delivered"));
            }
            newest = Some(newest.map_or(s, |n| n.max(s)));
        }
    }
    None
}

impl Workload {
    /// Creates the queue (via `endpoint`) and starts producers and consumers.
    pub async fn start(endpoint: &str, config: WorkloadConfig) -> Self {
        let client = resilient_client(endpoint, config.max_attempts);
        let mut create = client.create_queue().queue_name(&config.queue_name);
        create = create.attributes(QueueAttributeName::VisibilityTimeout, config.visibility_timeout.to_string());
        if config.fifo {
            create = create.attributes(QueueAttributeName::FifoQueue, "true");
        }
        let queue_url = create.send().await.expect("create workload queue").queue_url.unwrap();
        let mut w = Self {
            config,
            queue_url,
            client,
            books: Arc::default(),
            sent: Arc::default(),
            stop_producers: CancellationToken::new(),
            stop_consumers: CancellationToken::new(),
            producers: Vec::new(),
            consumers: Vec::new(),
        };
        for p in 0..w.config.producers {
            w.producers.push(tokio::spawn(produce(
                w.client.clone(),
                w.queue_url.clone(),
                w.config.clone(),
                p,
                w.books.clone(),
                w.sent.clone(),
                w.stop_producers.clone(),
            )));
        }
        for _ in 0..w.config.consumers {
            w.consumers.push(tokio::spawn(consume(
                w.client.clone(),
                w.queue_url.clone(),
                w.config.clone(),
                w.books.clone(),
                w.stop_consumers.clone(),
            )));
        }
        log(format!(
            "workload started on {} ({} producers, {} consumers)",
            w.config.queue_name, w.config.producers, w.config.consumers
        ));
        w
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub async fn received_count(&self) -> usize {
        self.books.lock().await.received.len()
    }

    pub async fn error_count(&self) -> usize {
        self.books.lock().await.errors.len()
    }

    /// Waits until at least `n` more messages have been acknowledged.
    pub async fn wait_for_progress(&self, n: u64, timeout: Duration) -> bool {
        let target = self.sent() + n;
        super::wait_until(timeout, || async { self.sent() >= target }).await
    }

    /// Stops producing, consumes until the queue is drained, and verifies the books.
    pub async fn finish(mut self, drain_timeout: Duration, started: Instant) -> Report {
        self.stop_producers.cancel();
        for p in self.producers.drain(..) {
            let _ = p.await;
        }
        log(format!(
            "{}: producers stopped after {} acknowledged sends; draining",
            self.config.queue_name,
            self.sent()
        ));
        // Drain: done once every acknowledged message was received and the queue is empty.
        let deadline = Instant::now() + drain_timeout;
        loop {
            let (missing, _) = {
                let b = self.books.lock().await;
                (b.acked.iter().filter(|m| !b.received.contains_key(*m)).count(), ())
            };
            let empty = self.queue_empty().await;
            if (missing == 0 && empty) || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        self.stop_consumers.cancel();
        for c in self.consumers.drain(..) {
            let _ = c.await;
        }
        let b = self.books.lock().await;
        let lost: Vec<String> = b.acked.iter().filter(|m| !b.received.contains_key(*m)).cloned().collect();
        let duplicates = b.received.values().map(|n| (*n as u64).saturating_sub(1)).sum();
        let fifo_violations: Vec<String> = b
            .group_log
            .iter()
            .filter_map(|(group, seqs)| fifo_violation(seqs).map(|v| format!("group {group}: {v}")))
            .collect();
        let report = Report {
            acked: b.acked.len(),
            unknown: b.unknown.len(),
            received_unique: b.received.len(),
            duplicates,
            lost,
            fifo_violations,
            errors: b.errors.iter().map(|(t, e)| (t.duration_since(started), e.clone())).collect(),
        };
        log(format!(
            "{}: acked={} unknown={} received={} duplicates={} lost={} fifo_violations={} client_errors={}",
            self.config.queue_name,
            report.acked,
            report.unknown,
            report.received_unique,
            report.duplicates,
            report.lost.len(),
            report.fifo_violations.len(),
            report.errors.len()
        ));
        report
    }

    async fn queue_empty(&self) -> bool {
        let Ok(out) = self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesDelayed)
            .send()
            .await
        else {
            return false;
        };
        out.attributes().is_some_and(|a| a.values().all(|v| v == "0"))
    }
}

async fn produce(
    client: Client,
    url: String,
    config: WorkloadConfig,
    producer: usize,
    books: Arc<Mutex<Books>>,
    sent: Arc<AtomicU64>,
    stop: CancellationToken,
) {
    let mut seq: u64 = 0;
    while !stop.is_cancelled() {
        let body = format!("p{producer}:{seq}");
        let mut req = client.send_message().queue_url(&url).message_body(&body);
        if config.fifo {
            // One group per producer, so the producer's own order is the group order.
            req = req.message_group_id(format!("g{producer}")).message_deduplication_id(&body);
        }
        match req.send().await {
            Ok(_) => {
                books.lock().await.acked.insert(body);
                sent.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                let mut b = books.lock().await;
                b.unknown.insert(body);
                b.errors.push((Instant::now(), format!("send: {}", aws_sdk_sqs::error::DisplayErrorContext(&e))));
            }
        }
        seq += 1;
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tokio::time::sleep(config.send_interval) => {}
        }
    }
}

async fn consume(
    client: Client,
    url: String,
    config: WorkloadConfig,
    books: Arc<Mutex<Books>>,
    stop: CancellationToken,
) {
    while !stop.is_cancelled() {
        let result = client
            .receive_message()
            .queue_url(&url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .visibility_timeout(config.visibility_timeout)
            .send()
            .await;
        let messages = match result {
            Ok(out) => out.messages.unwrap_or_default(),
            Err(e) => {
                books
                    .lock()
                    .await
                    .errors
                    .push((Instant::now(), format!("receive: {}", aws_sdk_sqs::error::DisplayErrorContext(&e))));
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        for m in messages {
            let body = m.body().unwrap_or_default().to_string();
            {
                let mut b = books.lock().await;
                *b.received.entry(body.clone()).or_insert(0) += 1;
                if config.fifo
                    && let Some((group, seq)) = body.split_once(':')
                {
                    b.group_log.entry(group.to_string()).or_default().push(seq.parse().unwrap_or(0));
                }
            }
            if let Err(e) = client
                .delete_message()
                .queue_url(&url)
                .receipt_handle(m.receipt_handle().unwrap_or_default())
                .send()
                .await
            {
                // The message will be redelivered after its visibility timeout (a duplicate).
                books
                    .lock()
                    .await
                    .errors
                    .push((Instant::now(), format!("delete: {}", aws_sdk_sqs::error::DisplayErrorContext(&e))));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fifo_violation;

    #[test]
    fn fifo_checker() {
        assert!(fifo_violation(&[0, 1, 2, 3]).is_none());
        // A batch redelivered after failed deletes is fine.
        assert!(fifo_violation(&[0, 1, 2, 1, 2, 3]).is_none());
        // Skipping a message and delivering it later is not.
        assert!(fifo_violation(&[0, 2, 1]).is_some());
        assert!(fifo_violation(&[0, 1, 3, 1, 2]).is_some());
    }
}
