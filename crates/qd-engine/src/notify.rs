//! Cross-replica wakeups for long polling, built on Postgres `LISTEN/NOTIFY`.
//!
//! After a write commits, the engine calls [`Notifier::publish`]: local long-pollers are woken
//! immediately, and the queue id is queued for a background flusher that sends one coalesced
//! `pg_notify` batch every few milliseconds. Every replica's listener receives those and wakes
//! its own pollers.
//!
//! NOTIFY is deliberately kept out of data transactions: Postgres serialises the commits of all
//! notifying transactions behind a database-wide lock, which would cap write throughput.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub const CHANNEL: &str = "qd_queue_events";

/// How long the flusher waits to coalesce notifications before sending them.
const COALESCE_WINDOW: Duration = Duration::from_millis(5);

#[derive(Default)]
pub struct Notifier {
    queues: DashMap<i64, Arc<Notify>>,
    pending: Mutex<HashSet<i64>>,
    flush: Notify,
    connected: AtomicBool,
}

impl Notifier {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Returns the wakeup handle for a queue. Callers must create and `enable()` a `notified()`
    /// future *before* checking for messages so that no notification is lost.
    pub fn subscribe(&self, queue_id: i64) -> Arc<Notify> {
        self.queues.entry(queue_id).or_default().clone()
    }

    /// Whether the cross-replica LISTEN connection is currently established.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    /// Announces that a queue has newly deliverable messages (call after commit).
    pub fn publish(&self, queue_id: i64) {
        self.wake(queue_id);
        self.pending.lock().expect("pending lock").insert(queue_id);
        self.flush.notify_one();
    }

    /// Sends pending notifications to the other replicas until shutdown.
    pub async fn run_flusher(self: Arc<Self>, pool: PgPool, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = self.flush.notified() => {}
            }
            tokio::time::sleep(COALESCE_WINDOW).await;
            let ids: Vec<String> = std::mem::take(&mut *self.pending.lock().expect("pending lock"))
                .into_iter()
                .map(|id| id.to_string())
                .collect();
            if ids.is_empty() {
                continue;
            }
            let sent = sqlx::query("SELECT pg_notify($1, id) FROM unnest($2::text[]) AS id")
                .bind(CHANNEL)
                .bind(&ids)
                .execute(&pool)
                .await;
            if sent.is_ok() {
                metrics::counter!("qd_notifications_published_total").increment(ids.len() as u64);
            }
            if let Err(err) = sent {
                // Remote pollers still re-check periodically, so this only adds latency.
                tracing::warn!(error = %err, "failed to publish queue notifications");
            }
        }
    }

    pub fn wake(&self, queue_id: i64) {
        if let Some(n) = self.queues.get(&queue_id) {
            n.notify_waiters();
        }
    }

    pub fn wake_all(&self) {
        for entry in self.queues.iter() {
            entry.value().notify_waiters();
        }
    }

    pub fn forget(&self, queue_id: i64) {
        if let Some((_, n)) = self.queues.remove(&queue_id) {
            n.notify_waiters();
        }
    }

    /// Runs the listener until shutdown, reconnecting on failure.
    pub async fn run(self: Arc<Self>, pool: PgPool, shutdown: CancellationToken) {
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            match self.listen(&pool, &shutdown).await {
                Ok(()) => {
                    self.connected.store(false, Ordering::Relaxed);
                    return;
                }
                Err(err) => {
                    self.connected.store(false, Ordering::Relaxed);
                    tracing::warn!(error = %err, "notification listener failed; reconnecting");
                    // Anything could have been missed while disconnected.
                    self.wake_all();
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }
        }
    }

    async fn listen(&self, pool: &PgPool, shutdown: &CancellationToken) -> Result<(), sqlx::Error> {
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen(CHANNEL).await?;
        tracing::debug!("listening for queue notifications");
        self.connected.store(true, Ordering::Relaxed);
        self.wake_all();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                msg = listener.try_recv() => match msg? {
                    Some(n) => {
                        if let Ok(id) = n.payload().parse::<i64>() {
                            self.wake(id);
                        }
                    }
                    // Connection dropped and was re-established by PgListener.
                    None => {
                        tracing::info!("notification listener reconnected");
                        self.wake_all();
                    }
                },
            }
        }
    }
}
