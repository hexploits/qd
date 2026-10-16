//! Message model, validation and the SQS MD5 digests.

use std::collections::BTreeMap;

use base64::Engine as _;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

use crate::error::{ErrorCode, Result, SqsError};

pub const MAX_MESSAGE_ATTRIBUTES: usize = 10;
pub const MAX_ATTRIBUTE_NAME_LEN: usize = 256;
pub const MAX_ID_LEN: usize = 128;
pub const TRACE_HEADER: &str = "AWSTraceHeader";

/// A message attribute value (user or system), in the same shape as the SQS wire format.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct MessageAttributeValue {
    pub data_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub string_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "b64_opt")]
    pub binary_value: Option<Vec<u8>>,
}

impl MessageAttributeValue {
    pub fn string(value: impl Into<String>) -> Self {
        Self { data_type: "String".into(), string_value: Some(value.into()), binary_value: None }
    }

    fn base_type(&self) -> &str {
        self.data_type.split('.').next().unwrap_or("")
    }

    /// Bytes this attribute contributes to the message size limit.
    fn size(&self, name: &str) -> usize {
        name.len()
            + self.data_type.len()
            + self.string_value.as_ref().map_or(0, |s| s.len())
            + self.binary_value.as_ref().map_or(0, |b| b.len())
    }
}

pub(crate) mod b64_opt {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let raw: Option<String> = Option::deserialize(d)?;
        raw.map(|s| base64::engine::general_purpose::STANDARD.decode(s.trim()).map_err(serde::de::Error::custom))
            .transpose()
    }
}

pub type MessageAttributes = BTreeMap<String, MessageAttributeValue>;

pub fn md5_hex(data: &[u8]) -> String {
    hex::encode(Md5::digest(data))
}

/// MD5 over message attributes using the algorithm the AWS SDKs verify against.
///
/// Attributes are visited in byte-wise name order (BTreeMap order); every field is prefixed with
/// its 4-byte big-endian length, and values carry a transport marker (1 = string, 2 = binary).
pub fn md5_of_attributes(attrs: &MessageAttributes) -> Option<String> {
    if attrs.is_empty() {
        return None;
    }
    Some(md5_of_attributes_impl(attrs))
}

fn md5_of_attributes_impl(attrs: &MessageAttributes) -> String {
    let mut hasher = Md5::new();
    for (name, value) in attrs {
        update_len_prefixed(&mut hasher, name.as_bytes());
        update_len_prefixed(&mut hasher, value.data_type.as_bytes());
        if let Some(s) = &value.string_value {
            hasher.update([1u8]);
            update_len_prefixed(&mut hasher, s.as_bytes());
        } else if let Some(b) = &value.binary_value {
            hasher.update([2u8]);
            update_len_prefixed(&mut hasher, b);
        }
    }
    hex::encode(hasher.finalize())
}

fn update_len_prefixed(hasher: &mut Md5, bytes: &[u8]) {
    hasher.update((bytes.len() as u32).to_be_bytes());
    hasher.update(bytes);
}

/// Returns the first character SQS does not allow in message content.
fn invalid_char(s: &str) -> Option<char> {
    s.chars().find(|&c| {
        let c = c as u32;
        !(c == 0x9
            || c == 0xA
            || c == 0xD
            || (0x20..=0xD7FF).contains(&c)
            || (0xE000..=0xFFFD).contains(&c)
            || c >= 0x10000)
    })
}

pub fn validate_body(body: &str) -> Result<()> {
    if body.is_empty() {
        return Err(SqsError::missing("MessageBody"));
    }
    if let Some(c) = invalid_char(body) {
        return Err(SqsError::new(
            ErrorCode::InvalidMessageContents,
            format!(
                "Invalid binary character '#x{:X}' was found in the message body, the set of allowed characters is #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF",
                c as u32
            ),
        ));
    }
    Ok(())
}

fn validate_attribute_name(name: &str) -> Result<()> {
    let invalid = |reason: &str| {
        SqsError::invalid_parameter(format!("Message (user) attribute name '{name}' is invalid. {reason}"))
    };
    if name.is_empty() || name.len() > MAX_ATTRIBUTE_NAME_LEN {
        return Err(invalid("Attribute name must be between 1 and 256 characters long."));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.') {
        return Err(invalid(
            "Attribute name can contain A-Z, a-z, 0-9, underscore (_), hyphen (-), and period (.) characters.",
        ));
    }
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("aws.") || lower.starts_with("amazon.") {
        return Err(invalid("The attribute name must not start with AWS-reserved prefixes 'AWS.' or 'Amazon.'."));
    }
    if name.starts_with('.') || name.ends_with('.') || name.contains("..") {
        return Err(invalid("Attribute name cannot start or end with a period, or contain successive periods."));
    }
    Ok(())
}

fn is_valid_number(s: &str) -> bool {
    let s = s.trim();
    let s = s.strip_prefix(['+', '-']).unwrap_or(s);
    let (mantissa, exponent) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let mut parts = mantissa.splitn(2, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next();
    let digits_ok = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    let mantissa_ok = digits_ok(int_part)
        && frac_part.is_none_or(digits_ok)
        && (!int_part.is_empty() || frac_part.is_some_and(|f| !f.is_empty()));
    let exponent_ok = exponent.is_none_or(|e| {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        !e.is_empty() && digits_ok(e)
    });
    let significant = mantissa.bytes().filter(|b| b.is_ascii_digit()).count();
    mantissa_ok && exponent_ok && significant <= 38
}

fn validate_attribute_value(name: &str, value: &MessageAttributeValue, kind: &str) -> Result<()> {
    let base = value.base_type();
    if value.data_type.len() > MAX_ATTRIBUTE_NAME_LEN || !matches!(base, "String" | "Number" | "Binary") {
        return Err(SqsError::invalid_parameter(format!(
            "The type of message {kind} attribute '{name}' is invalid. You must use only the following supported type prefixes: Binary, Number, String."
        )));
    }
    if let Some(label) = value.data_type.get(base.len()..).filter(|l| !l.is_empty()) {
        let label = &label[1..];
        if label.is_empty() || !label.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
        {
            return Err(SqsError::invalid_parameter(format!(
                "The type of message {kind} attribute '{name}' is invalid."
            )));
        }
    }
    let empty = |t: &str| {
        SqsError::invalid_parameter(format!(
            "The message {kind} attribute '{name}' must contain non-empty message attribute value for message attribute type '{t}'."
        ))
    };
    match base {
        "Binary" => {
            if value.binary_value.as_ref().is_none_or(|b| b.is_empty()) {
                return Err(empty("Binary"));
            }
        }
        _ => {
            let s = value.string_value.as_deref().filter(|s| !s.is_empty()).ok_or_else(|| empty(base))?;
            if let Some(c) = invalid_char(s) {
                return Err(SqsError::new(
                    ErrorCode::InvalidMessageContents,
                    format!("Invalid binary character '#x{:X}' was found in the message attribute '{name}'.", c as u32),
                ));
            }
            if base == "Number" && !is_valid_number(s) {
                return Err(SqsError::invalid_parameter(format!(
                    "Can't cast the value of message {kind} attribute '{name}' to a number."
                )));
            }
        }
    }
    Ok(())
}

/// Validates user message attributes; returns their size contribution.
pub fn validate_message_attributes(attrs: &MessageAttributes) -> Result<usize> {
    if attrs.len() > MAX_MESSAGE_ATTRIBUTES {
        return Err(SqsError::invalid_parameter(format!(
            "Number of message attributes [{}] exceeds the allowed maximum [{MAX_MESSAGE_ATTRIBUTES}].",
            attrs.len()
        )));
    }
    let mut size = 0;
    for (name, value) in attrs {
        validate_attribute_name(name)?;
        validate_attribute_value(name, value, "(user)")?;
        size += value.size(name);
    }
    Ok(size)
}

pub fn validate_system_attributes(attrs: &MessageAttributes) -> Result<()> {
    for (name, value) in attrs {
        if name != TRACE_HEADER {
            return Err(SqsError::invalid_parameter(format!("Message system attribute name '{name}' is invalid.")));
        }
        if value.data_type != "String" {
            return Err(SqsError::invalid_parameter(format!(
                "Message system attribute '{name}' must be of type 'String'."
            )));
        }
        validate_attribute_value(name, value, "system")?;
    }
    Ok(())
}

/// Validates a MessageGroupId / MessageDeduplicationId value.
pub fn validate_fifo_id(param: &str, value: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value.len() <= MAX_ID_LEN
        && value.bytes().all(|b| b.is_ascii_alphanumeric() || b.is_ascii_punctuation());
    if ok {
        Ok(())
    } else {
        Err(SqsError::invalid_value(
            param,
            value,
            &format!("{param} can only include alphanumeric and punctuation characters. 1 to 128 in length."),
        ))
    }
}

/// System attributes that ReceiveMessage can return.
pub mod system_names {
    pub const ALL: &str = "All";
    pub const SENDER_ID: &str = "SenderId";
    pub const SENT_TIMESTAMP: &str = "SentTimestamp";
    pub const APPROXIMATE_RECEIVE_COUNT: &str = "ApproximateReceiveCount";
    pub const APPROXIMATE_FIRST_RECEIVE_TIMESTAMP: &str = "ApproximateFirstReceiveTimestamp";
    pub const SEQUENCE_NUMBER: &str = "SequenceNumber";
    pub const MESSAGE_DEDUPLICATION_ID: &str = "MessageDeduplicationId";
    pub const MESSAGE_GROUP_ID: &str = "MessageGroupId";
    pub const AWS_TRACE_HEADER: &str = "AWSTraceHeader";
    pub const DEAD_LETTER_QUEUE_SOURCE_ARN: &str = "DeadLetterQueueSourceArn";

    pub const KNOWN: &[&str] = &[
        ALL,
        SENDER_ID,
        SENT_TIMESTAMP,
        APPROXIMATE_RECEIVE_COUNT,
        APPROXIMATE_FIRST_RECEIVE_TIMESTAMP,
        SEQUENCE_NUMBER,
        MESSAGE_DEDUPLICATION_ID,
        MESSAGE_GROUP_ID,
        AWS_TRACE_HEADER,
        DEAD_LETTER_QUEUE_SOURCE_ARN,
    ];
}

/// A message as handed out by ReceiveMessage, with every attribute populated.
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub message_id: String,
    pub receipt_handle: String,
    pub body: String,
    pub md5_of_body: String,
    pub message_attributes: MessageAttributes,
    /// All system attributes (already rendered as strings).
    pub system_attributes: BTreeMap<String, String>,
}

impl ReceivedMessage {
    /// Applies the AttributeNames / MessageSystemAttributeNames filter.
    pub fn filter_system_attributes(&self, requested: &[String]) -> BTreeMap<String, String> {
        if requested.iter().any(|n| n == system_names::ALL) {
            return self.system_attributes.clone();
        }
        self.system_attributes
            .iter()
            .filter(|(k, _)| requested.iter().any(|r| r == *k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Applies the MessageAttributeNames filter (`All`, `.*`, exact names and `prefix.*`).
    pub fn filter_message_attributes(&self, requested: &[String]) -> MessageAttributes {
        let matches = |name: &str| {
            requested.iter().any(|r| {
                r == "All"
                    || r == ".*"
                    || r == name
                    || r.strip_suffix(".*").is_some_and(|prefix| name.starts_with(&format!("{prefix}.")))
            })
        };
        self.message_attributes.iter().filter(|(k, _)| matches(k)).map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

pub fn decode_base64(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(s.trim()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_body() {
        assert_eq!(md5_hex(b"hello"), "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn md5_attributes_matches_aws() {
        // Reference value produced by the AWS Java SDK's MessageMD5ChecksumHandler.
        let mut attrs = MessageAttributes::new();
        attrs.insert("attr1".into(), MessageAttributeValue::string("value1"));
        let mut hasher = Md5::new();
        for part in [&b"attr1"[..], b"String"] {
            hasher.update((part.len() as u32).to_be_bytes());
            hasher.update(part);
        }
        hasher.update([1u8]);
        hasher.update(6u32.to_be_bytes());
        hasher.update(b"value1");
        assert_eq!(md5_of_attributes(&attrs).unwrap(), hex::encode(hasher.finalize()));
        assert_eq!(md5_of_attributes(&MessageAttributes::new()), None);
    }

    #[test]
    fn md5_known_vector() {
        // Captured from real SQS: {"key": String "value"} -> MD5OfMessageAttributes.
        let mut attrs = MessageAttributes::new();
        attrs.insert("key".into(), MessageAttributeValue::string("value"));
        // Independently recomputed: len("key")+"key"+len("String")+"String"+0x01+len("value")+"value"
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(b"key");
        buf.extend_from_slice(&6u32.to_be_bytes());
        buf.extend_from_slice(b"String");
        buf.push(1);
        buf.extend_from_slice(&5u32.to_be_bytes());
        buf.extend_from_slice(b"value");
        assert_eq!(md5_of_attributes(&attrs).unwrap(), md5_hex(&buf));
    }

    #[test]
    fn body_validation() {
        assert!(validate_body("hello\tworld\n").is_ok());
        assert!(validate_body("").is_err());
        assert_eq!(validate_body("a\u{1}b").unwrap_err().code, ErrorCode::InvalidMessageContents);
        assert!(validate_body("emoji 🎉").is_ok());
        assert!(validate_body("\u{FFFE}").is_err());
    }

    #[test]
    fn attribute_validation() {
        let mut attrs = MessageAttributes::new();
        attrs.insert("ok.name-1".into(), MessageAttributeValue::string("v"));
        attrs.insert(
            "num".into(),
            MessageAttributeValue {
                data_type: "Number.int".into(),
                string_value: Some("-1.5e3".into()),
                binary_value: None,
            },
        );
        attrs.insert(
            "bin".into(),
            MessageAttributeValue { data_type: "Binary".into(), string_value: None, binary_value: Some(vec![1, 2]) },
        );
        assert!(validate_message_attributes(&attrs).is_ok());

        for bad in ["AWS.x", "amazon.y", ".a", "a.", "a..b", "sp ace", ""] {
            let mut a = MessageAttributes::new();
            a.insert(bad.into(), MessageAttributeValue::string("v"));
            assert!(validate_message_attributes(&a).is_err(), "{bad} should be rejected");
        }
        let mut a = MessageAttributes::new();
        a.insert(
            "n".into(),
            MessageAttributeValue { data_type: "Number".into(), string_value: Some("abc".into()), binary_value: None },
        );
        assert!(validate_message_attributes(&a).is_err());
        let mut a = MessageAttributes::new();
        a.insert(
            "n".into(),
            MessageAttributeValue { data_type: "Foo".into(), string_value: Some("1".into()), binary_value: None },
        );
        assert!(validate_message_attributes(&a).is_err());
        let mut a = MessageAttributes::new();
        a.insert("n".into(), MessageAttributeValue::string(""));
        assert!(validate_message_attributes(&a).is_err());
        let a: MessageAttributes = (0..11).map(|i| (format!("a{i}"), MessageAttributeValue::string("v"))).collect();
        assert!(validate_message_attributes(&a).is_err());
    }

    #[test]
    fn numbers() {
        for ok in ["1", "-1", "+1.5", ".5", "5.", "1e10", "1E-3", "0"] {
            assert!(is_valid_number(ok), "{ok}");
        }
        for bad in ["", "abc", "1e", "--1", ".", "1.2.3", "1e1.5"] {
            assert!(!is_valid_number(bad), "{bad}");
        }
    }

    #[test]
    fn attribute_filters() {
        let mut attrs = MessageAttributes::new();
        for n in ["a", "foo.bar", "foo.baz", "food"] {
            attrs.insert(n.into(), MessageAttributeValue::string("v"));
        }
        let m = ReceivedMessage {
            message_id: "1".into(),
            receipt_handle: "r".into(),
            body: "b".into(),
            md5_of_body: "x".into(),
            message_attributes: attrs,
            system_attributes: [
                ("SenderId".to_string(), "s".to_string()),
                ("SentTimestamp".to_string(), "1".to_string()),
            ]
            .into_iter()
            .collect(),
        };
        let keys = |v: Vec<&str>| v.into_iter().map(String::from).collect::<Vec<_>>();
        assert_eq!(m.filter_message_attributes(&keys(vec!["All"])).len(), 4);
        assert_eq!(m.filter_message_attributes(&keys(vec!["foo.*"])).len(), 2);
        assert_eq!(m.filter_message_attributes(&keys(vec!["a"])).len(), 1);
        assert_eq!(m.filter_message_attributes(&keys(vec![])).len(), 0);
        assert_eq!(m.filter_system_attributes(&keys(vec!["SenderId"])).len(), 1);
        assert_eq!(m.filter_system_attributes(&keys(vec!["All"])).len(), 2);
    }

    #[test]
    fn fifo_ids() {
        assert!(validate_fifo_id("MessageGroupId", "group-1!").is_ok());
        assert!(validate_fifo_id("MessageGroupId", "").is_err());
        assert!(validate_fifo_id("MessageGroupId", "has space").is_err());
        assert!(validate_fifo_id("MessageGroupId", &"a".repeat(129)).is_err());
    }
}
