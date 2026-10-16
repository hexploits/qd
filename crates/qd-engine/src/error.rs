//! Error model shared by every protocol frontend.
//!
//! Codes mirror the AWS SQS error catalogue so that frontends can render them in whatever wire
//! format the client expects (JSON `__type`, `x-amzn-query-error`, or Query/XML `<Code>`).

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    // Modeled SQS errors.
    QueueDoesNotExist,
    QueueNameExists,
    QueueDeletedRecently,
    PurgeQueueInProgress,
    MessageNotInflight,
    ReceiptHandleIsInvalid,
    InvalidAttributeName,
    InvalidAttributeValue,
    InvalidMessageContents,
    TooManyEntriesInBatchRequest,
    BatchEntryIdsNotDistinct,
    EmptyBatchRequest,
    InvalidBatchEntryId,
    BatchRequestTooLong,
    UnsupportedOperation,
    InvalidIdFormat,
    OverLimit,
    ResourceNotFound,
    InvalidAddress,
    InvalidSecurity,
    RequestThrottled,
    // Common (unmodeled) AWS errors.
    InvalidParameterValue,
    MissingParameter,
    InvalidAction,
    InvalidQueryParameter,
    MalformedQueryString,
    AccessDenied,
    InvalidClientTokenId,
    SignatureDoesNotMatch,
    IncompleteSignature,
    MissingAuthenticationToken,
    RequestExpired,
    InternalFailure,
    ServiceUnavailable,
}

impl ErrorCode {
    /// Shape name used as the `__type` in AWS JSON 1.0 responses.
    pub fn json_type(self) -> &'static str {
        use ErrorCode::*;
        match self {
            QueueDoesNotExist => "QueueDoesNotExist",
            QueueNameExists => "QueueNameExists",
            QueueDeletedRecently => "QueueDeletedRecently",
            PurgeQueueInProgress => "PurgeQueueInProgress",
            MessageNotInflight => "MessageNotInflight",
            ReceiptHandleIsInvalid => "ReceiptHandleIsInvalid",
            InvalidAttributeName => "InvalidAttributeName",
            InvalidAttributeValue => "InvalidAttributeValue",
            InvalidMessageContents => "InvalidMessageContents",
            TooManyEntriesInBatchRequest => "TooManyEntriesInBatchRequest",
            BatchEntryIdsNotDistinct => "BatchEntryIdsNotDistinct",
            EmptyBatchRequest => "EmptyBatchRequest",
            InvalidBatchEntryId => "InvalidBatchEntryId",
            BatchRequestTooLong => "BatchRequestTooLong",
            UnsupportedOperation => "UnsupportedOperation",
            InvalidIdFormat => "InvalidIdFormat",
            OverLimit => "OverLimit",
            ResourceNotFound => "ResourceNotFoundException",
            InvalidAddress => "InvalidAddress",
            InvalidSecurity => "InvalidSecurity",
            RequestThrottled => "RequestThrottled",
            other => other.query_code(),
        }
    }

    /// Legacy error code used by the Query protocol and the `x-amzn-query-error` header.
    pub fn query_code(self) -> &'static str {
        use ErrorCode::*;
        match self {
            QueueDoesNotExist => "AWS.SimpleQueueService.NonExistentQueue",
            QueueNameExists => "QueueAlreadyExists",
            QueueDeletedRecently => "AWS.SimpleQueueService.QueueDeletedRecently",
            PurgeQueueInProgress => "AWS.SimpleQueueService.PurgeQueueInProgress",
            MessageNotInflight => "AWS.SimpleQueueService.MessageNotInflight",
            ReceiptHandleIsInvalid => "ReceiptHandleIsInvalid",
            InvalidAttributeName => "InvalidAttributeName",
            InvalidAttributeValue => "InvalidAttributeValue",
            InvalidMessageContents => "InvalidMessageContents",
            TooManyEntriesInBatchRequest => "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
            BatchEntryIdsNotDistinct => "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
            EmptyBatchRequest => "AWS.SimpleQueueService.EmptyBatchRequest",
            InvalidBatchEntryId => "AWS.SimpleQueueService.InvalidBatchEntryId",
            BatchRequestTooLong => "AWS.SimpleQueueService.BatchRequestTooLong",
            UnsupportedOperation => "AWS.SimpleQueueService.UnsupportedOperation",
            InvalidIdFormat => "InvalidIdFormat",
            OverLimit => "OverLimit",
            ResourceNotFound => "ResourceNotFoundException",
            InvalidAddress => "InvalidAddress",
            InvalidSecurity => "InvalidSecurity",
            RequestThrottled => "RequestThrottled",
            InvalidParameterValue => "InvalidParameterValue",
            MissingParameter => "MissingParameter",
            InvalidAction => "InvalidAction",
            InvalidQueryParameter => "InvalidQueryParameter",
            MalformedQueryString => "MalformedQueryString",
            AccessDenied => "AccessDenied",
            InvalidClientTokenId => "InvalidClientTokenId",
            SignatureDoesNotMatch => "SignatureDoesNotMatch",
            IncompleteSignature => "IncompleteSignature",
            MissingAuthenticationToken => "MissingAuthenticationToken",
            RequestExpired => "RequestExpired",
            InternalFailure => "InternalFailure",
            ServiceUnavailable => "ServiceUnavailable",
        }
    }

    pub fn http_status(self) -> u16 {
        use ErrorCode::*;
        match self {
            OverLimit
            | AccessDenied
            | InvalidClientTokenId
            | SignatureDoesNotMatch
            | MissingAuthenticationToken
            | RequestExpired
            | InvalidSecurity => 403,
            ResourceNotFound => 404,
            InternalFailure => 500,
            ServiceUnavailable => 503,
            _ => 400,
        }
    }

    /// True when the fault lies with the caller ("Sender"), false for server faults ("Receiver").
    pub fn sender_fault(self) -> bool {
        !matches!(self, ErrorCode::InternalFailure | ErrorCode::ServiceUnavailable)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqsError {
    pub code: ErrorCode,
    pub message: String,
}

impl SqsError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }

    pub fn invalid_parameter(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidParameterValue, message)
    }

    /// The canonical AWS message for an out-of-range or malformed parameter.
    pub fn invalid_value(param: &str, value: impl fmt::Display, reason: &str) -> Self {
        Self::invalid_parameter(format!("Value {value} for parameter {param} is invalid. Reason: {reason}"))
    }

    pub fn missing(param: &str) -> Self {
        Self::new(ErrorCode::MissingParameter, format!("The request must contain the parameter {param}."))
    }

    pub fn queue_does_not_exist() -> Self {
        Self::new(ErrorCode::QueueDoesNotExist, "The specified queue does not exist.")
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InternalFailure, message)
    }
}

impl fmt::Display for SqsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.query_code(), self.message)
    }
}

impl std::error::Error for SqsError {}

impl From<sqlx::Error> for SqsError {
    fn from(err: sqlx::Error) -> Self {
        tracing::error!(error = %err, "database error");
        match err {
            sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_) => {
                metrics::counter!("qd_db_errors_total", "kind" => "unavailable").increment(1);
                SqsError::new(ErrorCode::ServiceUnavailable, "Service is temporarily unavailable.")
            }
            // Admin shutdown / failover in progress / read-only standby: transient, so report it
            // as unavailable (retryable) rather than as an internal error.
            sqlx::Error::Database(ref db)
                if db.code().is_some_and(|c| c.starts_with("57P") || c == "25006" || c.starts_with("08")) =>
            {
                metrics::counter!("qd_db_errors_total", "kind" => "unavailable").increment(1);
                SqsError::new(ErrorCode::ServiceUnavailable, "Service is temporarily unavailable.")
            }
            _ => {
                metrics::counter!("qd_db_errors_total", "kind" => "other").increment(1);
                SqsError::internal("We encountered an internal error. Please try again.")
            }
        }
    }
}

pub type Result<T, E = SqsError> = std::result::Result<T, E>;
