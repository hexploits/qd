//! Receipt handles.
//!
//! A receipt handle identifies one particular *receive* of a message: the queue, the internal
//! message row and a random token that is rotated every time the message is received. The token is
//! unguessable, so handles do not need to be signed.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use uuid::Uuid;

use crate::error::{ErrorCode, Result, SqsError};

const PREFIX: &str = "qd1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptHandle {
    pub queue_id: i64,
    pub row_id: i64,
    pub token: Uuid,
}

impl ReceiptHandle {
    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(format!("{PREFIX}:{}:{}:{}", self.queue_id, self.row_id, self.token.simple()))
    }

    pub fn decode(handle: &str) -> Result<Self> {
        let invalid = || {
            SqsError::new(
                ErrorCode::ReceiptHandleIsInvalid,
                format!("The input receipt handle \"{handle}\" is not a valid receipt handle."),
            )
        };
        let raw = URL_SAFE_NO_PAD.decode(handle.trim()).map_err(|_| invalid())?;
        let raw = String::from_utf8(raw).map_err(|_| invalid())?;
        let mut parts = raw.split(':');
        let (Some(PREFIX), Some(q), Some(r), Some(t), None) =
            (parts.next(), parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(invalid());
        };
        Ok(Self {
            queue_id: q.parse().map_err(|_| invalid())?,
            row_id: r.parse().map_err(|_| invalid())?,
            token: Uuid::parse_str(t).map_err(|_| invalid())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let h = ReceiptHandle { queue_id: 7, row_id: 123456789, token: Uuid::new_v4() };
        let s = h.encode();
        assert_eq!(ReceiptHandle::decode(&s).unwrap(), h);
    }

    #[test]
    fn rejects_garbage() {
        for bad in ["", "abc", "!!!", &URL_SAFE_NO_PAD.encode("qd1:1:2"), &URL_SAFE_NO_PAD.encode("xx:1:2:3")] {
            assert_eq!(ReceiptHandle::decode(bad).unwrap_err().code, ErrorCode::ReceiptHandleIsInvalid);
        }
    }
}
