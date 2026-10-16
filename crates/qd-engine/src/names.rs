//! Queue naming, URLs and ARNs.

use crate::error::{ErrorCode, Result, SqsError};

pub const FIFO_SUFFIX: &str = ".fifo";
pub const MAX_QUEUE_NAME_LEN: usize = 80;

/// Validates a queue name; `fifo` is whether the queue is being created as FIFO.
pub fn validate_queue_name(name: &str, fifo: bool) -> Result<()> {
    let base = name.strip_suffix(FIFO_SUFFIX);
    let valid_chars = |s: &str| s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    let ok_len = !name.is_empty() && name.len() <= MAX_QUEUE_NAME_LEN;
    match (fifo, base) {
        (true, Some(base)) if ok_len && !base.is_empty() && valid_chars(base) => Ok(()),
        (true, _) => Err(SqsError::invalid_parameter(
            "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length.",
        )),
        (false, _) if ok_len && valid_chars(name) => Ok(()),
        (false, _) => Err(SqsError::invalid_parameter(
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
        )),
    }
}

pub fn is_fifo_name(name: &str) -> bool {
    name.ends_with(FIFO_SUFFIX)
}

pub fn queue_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:sqs:{region}:{account_id}:{name}")
}

/// A parsed SQS queue ARN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueArn {
    pub region: String,
    pub account_id: String,
    pub name: String,
}

impl QueueArn {
    pub fn parse(arn: &str) -> Option<Self> {
        let mut parts = arn.splitn(6, ':');
        let (Some("arn"), Some(_partition), Some("sqs"), Some(region), Some(account), Some(name)) =
            (parts.next(), parts.next(), parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return None;
        };
        if account.is_empty() || name.is_empty() {
            return None;
        }
        Some(Self { region: region.into(), account_id: account.into(), name: name.into() })
    }
}

/// Identity of a queue as addressed by a client (account + name).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueRef {
    pub account_id: String,
    pub name: String,
}

impl QueueRef {
    pub fn new(account_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self { account_id: account_id.into(), name: name.into() }
    }

    /// Parses an SQS queue URL of the form `scheme://host[:port]/<account>/<name>`.
    ///
    /// The host is ignored on purpose: clients may reach us through any hostname.
    pub fn from_url(url: &str) -> Result<Self> {
        let path = match url.find("://") {
            Some(idx) => {
                let rest = &url[idx + 3..];
                rest.find('/').map(|p| &rest[p..]).unwrap_or("")
            }
            None => url,
        };
        let path = path.split(['?', '#']).next().unwrap_or("");
        Self::from_path(path).ok_or_else(|| {
            SqsError::new(ErrorCode::InvalidAddress, format!("The address {url} is not valid for this endpoint."))
        })
    }

    /// Parses `/<account>/<name>` (trailing slash tolerated).
    pub fn from_path(path: &str) -> Option<Self> {
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        match segments.as_slice() {
            [account, name] => Some(Self::new(*account, *name)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_names() {
        assert!(validate_queue_name("orders", false).is_ok());
        assert!(validate_queue_name("orders-1_A", false).is_ok());
        assert!(validate_queue_name("orders.fifo", true).is_ok());
        assert!(validate_queue_name("orders.fifo", false).is_err());
        assert!(validate_queue_name("orders", true).is_err());
        assert!(validate_queue_name(".fifo", true).is_err());
        assert!(validate_queue_name("", false).is_err());
        assert!(validate_queue_name(&"a".repeat(80), false).is_ok());
        assert!(validate_queue_name(&"a".repeat(81), false).is_err());
        assert!(validate_queue_name(&format!("{}.fifo", "a".repeat(75)), true).is_ok());
        assert!(validate_queue_name(&format!("{}.fifo", "a".repeat(76)), true).is_err());
        assert!(validate_queue_name("bad name", false).is_err());
    }

    #[test]
    fn urls() {
        let r = QueueRef::from_url("http://localhost:9324/000000000000/q1").unwrap();
        assert_eq!(r, QueueRef::new("000000000000", "q1"));
        let r = QueueRef::from_url("https://sqs.us-east-1.amazonaws.com/123456789012/q.fifo/").unwrap();
        assert_eq!(r, QueueRef::new("123456789012", "q.fifo"));
        assert!(QueueRef::from_url("http://localhost/q1").is_err());
        assert!(QueueRef::from_url("http://localhost/a/b/c").is_err());
        assert_eq!(QueueRef::from_url("/1/q").unwrap(), QueueRef::new("1", "q"));
    }

    #[test]
    fn arns() {
        let a = QueueArn::parse("arn:aws:sqs:us-east-1:123:dlq").unwrap();
        assert_eq!(a.account_id, "123");
        assert_eq!(a.name, "dlq");
        assert!(QueueArn::parse("arn:aws:sns:us-east-1:123:x").is_none());
        assert!(QueueArn::parse("nonsense").is_none());
    }
}
