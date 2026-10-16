//! The queue engine: all queue and message operations.

mod heartbeat;
mod janitor;
mod messages;
mod queues;
mod receive;
mod redrive;

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::error::{Result, SqsError};
use crate::names::{QueueArn, QueueRef};
use crate::notify::Notifier;
use crate::queue::{QUEUE_COLUMNS, Queue, QueueRow};

pub use messages::{BatchResult, ChangeVisibilityEntry, DeleteEntry, SendMessageInput, SendMessageOutput};
pub use queues::{ListPage, Permission};
pub use receive::ReceiveInput;
pub use redrive::{MoveTask, MoveTaskStatus};

/// Tunables for the engine.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Region reported in queue ARNs.
    pub region: String,
    /// How long a replica may serve queue metadata from its local cache. Attribute changes made
    /// through other replicas become visible after at most this long.
    pub queue_cache_ttl: Duration,
    /// Upper bound on how long a long-poller sleeps before re-checking the database, in case a
    /// notification was missed.
    pub long_poll_recheck: Duration,
    /// Minimum interval between two PurgeQueue calls on the same queue (AWS: 60s).
    pub purge_cooldown: Duration,
    /// Interval of the background janitor (retention, deleted queues, dedup windows).
    pub janitor_interval: Duration,
    /// Interval of the redrive (message move task) worker.
    pub move_task_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".into(),
            queue_cache_ttl: Duration::from_secs(1),
            long_poll_recheck: Duration::from_secs(2),
            purge_cooldown: Duration::from_secs(60),
            janitor_interval: Duration::from_secs(10),
            move_task_interval: Duration::from_millis(1000),
        }
    }
}

struct CachedQueue {
    fetched: Instant,
    queue: Arc<Queue>,
}

/// Cheaply cloneable handle to the engine.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

struct Inner {
    pool: PgPool,
    config: EngineConfig,
    cache: DashMap<QueueRef, CachedQueue>,
    notifier: Arc<Notifier>,
    shutdown: CancellationToken,
}

impl Engine {
    pub fn new(pool: PgPool, config: EngineConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                config,
                cache: DashMap::new(),
                notifier: Notifier::new(),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }

    pub fn region(&self) -> &str {
        &self.inner.config.region
    }

    /// Starts everything a single all-in-one process needs: API wakeups plus all scheduler work.
    pub fn start_background(&self, instance: &str) -> Vec<tokio::task::JoinHandle<()>> {
        let mut tasks = self.start_api_background();
        tasks.extend(self.start_scheduler_work(instance));
        tasks
    }

    /// Background tasks an API server needs: the cross-replica LISTEN connection (long-poll
    /// wakeups) and the notification flusher.
    pub fn start_api_background(&self) -> Vec<tokio::task::JoinHandle<()>> {
        let token = self.inner.shutdown.clone();
        vec![
            tokio::spawn(self.inner.notifier.clone().run(self.inner.pool.clone(), token.clone())),
            tokio::spawn(self.inner.notifier.clone().run_flusher(self.inner.pool.clone(), token)),
        ]
    }

    /// Background tasks of the scheduler role: retention/cleanup janitor, message move tasks,
    /// the heartbeat, and the flusher (redrive wakes long-pollers on the API replicas).
    pub fn start_scheduler_background(&self, instance: &str) -> Vec<tokio::task::JoinHandle<()>> {
        let mut tasks = vec![tokio::spawn(
            self.inner.notifier.clone().run_flusher(self.inner.pool.clone(), self.inner.shutdown.clone()),
        )];
        tasks.extend(self.start_scheduler_work(instance));
        tasks
    }

    fn start_scheduler_work(&self, instance: &str) -> Vec<tokio::task::JoinHandle<()>> {
        vec![
            tokio::spawn(janitor::run(self.clone())),
            tokio::spawn(redrive::run_worker(self.clone())),
            tokio::spawn(heartbeat::run(self.clone(), instance.to_string())),
        ]
    }

    /// Signals background tasks and in-progress long polls to finish.
    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown.clone()
    }

    /// Whether this replica currently receives cross-replica wakeups.
    pub fn notifier_connected(&self) -> bool {
        self.inner.notifier.is_connected()
    }

    /// Checks database connectivity (used by readiness probes).
    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.inner.pool).await?;
        Ok(())
    }

    /// Resolves a queue, serving from the local cache when fresh.
    pub async fn queue(&self, queue: &QueueRef) -> Result<Arc<Queue>> {
        if let Some(entry) = self.inner.cache.get(queue)
            && entry.fetched.elapsed() < self.inner.config.queue_cache_ttl
        {
            return Ok(entry.queue.clone());
        }
        self.queue_fresh(queue).await
    }

    /// Resolves a queue straight from the database and refreshes the cache.
    pub async fn queue_fresh(&self, queue: &QueueRef) -> Result<Arc<Queue>> {
        let row: Option<QueueRow> =
            sqlx::query_as(&format!("SELECT {QUEUE_COLUMNS} FROM qd_queues WHERE account_id = $1 AND name = $2"))
                .bind(&queue.account_id)
                .bind(&queue.name)
                .fetch_optional(&self.inner.pool)
                .await?;
        match row {
            Some(row) => {
                let q = Arc::new(row.into_queue(self.region()));
                self.inner.cache.insert(queue.clone(), CachedQueue { fetched: Instant::now(), queue: q.clone() });
                Ok(q)
            }
            None => {
                self.inner.cache.remove(queue);
                Err(SqsError::queue_does_not_exist())
            }
        }
    }

    pub async fn queue_by_arn(&self, arn: &str) -> Result<Arc<Queue>> {
        let parsed = QueueArn::parse(arn).ok_or_else(SqsError::queue_does_not_exist)?;
        self.queue(&QueueRef::new(parsed.account_id, parsed.name)).await
    }

    /// Wakes long-pollers of a queue on every replica. Call after the write has committed.
    pub(crate) fn publish(&self, queue_id: i64) {
        self.inner.notifier.publish(queue_id);
    }

    fn invalidate(&self, queue: &QueueRef) {
        self.inner.cache.remove(queue);
    }
}

/// Converts a timestamp to epoch milliseconds.
pub(crate) fn millis(ts: chrono::DateTime<chrono::Utc>) -> i64 {
    ts.timestamp_millis()
}

/// Advisory lock key that serialises FIFO receives on one queue.
pub(crate) fn fifo_lock_key(queue_id: i64) -> i64 {
    0x5144_0000_0000_0000_i64 ^ queue_id
}

/// Validates the `Id` of a batch entry and that ids are unique.
pub(crate) fn validate_batch_ids<'a>(ids: impl Iterator<Item = &'a str>, entry_kind: &str) -> Result<()> {
    use crate::error::ErrorCode;
    let mut seen = std::collections::HashSet::new();
    let mut count = 0;
    for id in ids {
        count += 1;
        if id.is_empty() || id.len() > 80 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
            return Err(SqsError::new(
                ErrorCode::InvalidBatchEntryId,
                "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.",
            ));
        }
        if !seen.insert(id) {
            return Err(SqsError::new(ErrorCode::BatchEntryIdsNotDistinct, format!("Id {id} repeated.")));
        }
    }
    if count == 0 {
        return Err(SqsError::new(
            ErrorCode::EmptyBatchRequest,
            format!("There should be at least one {entry_kind} in the request."),
        ));
    }
    if count > 10 {
        return Err(SqsError::new(
            ErrorCode::TooManyEntriesInBatchRequest,
            format!("Maximum number of entries per request are 10. You have sent {count}."),
        ));
    }
    Ok(())
}
