//! Queue model as stored in Postgres.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sqlx::types::Json;

use crate::attributes::QueueAttributes;
use crate::names::{QueueRef, queue_arn};

#[derive(Debug, Clone)]
pub struct Queue {
    pub id: i64,
    pub account_id: String,
    pub name: String,
    pub fifo: bool,
    pub attributes: QueueAttributes,
    pub tags: BTreeMap<String, String>,
    pub arn: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_purged_at: Option<DateTime<Utc>>,
}

impl Queue {
    pub fn queue_ref(&self) -> QueueRef {
        QueueRef::new(&self.account_id, &self.name)
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct QueueRow {
    pub id: i64,
    pub account_id: String,
    pub name: String,
    pub fifo: bool,
    pub attributes: Json<QueueAttributes>,
    pub tags: Json<BTreeMap<String, String>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_purged_at: Option<DateTime<Utc>>,
}

impl QueueRow {
    pub fn into_queue(self, region: &str) -> Queue {
        let arn = queue_arn(region, &self.account_id, &self.name);
        Queue {
            id: self.id,
            account_id: self.account_id,
            name: self.name,
            fifo: self.fifo,
            attributes: self.attributes.0,
            tags: self.tags.0,
            arn,
            created_at: self.created_at,
            updated_at: self.updated_at,
            last_purged_at: self.last_purged_at,
        }
    }
}

pub(crate) const QUEUE_COLUMNS: &str =
    "id, account_id, name, fifo, attributes, tags, created_at, updated_at, last_purged_at";

/// Message counts reported through GetQueueAttributes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueCounts {
    pub visible: i64,
    pub in_flight: i64,
    pub delayed: i64,
}
