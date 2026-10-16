//! Wire-format translation between the two SQS protocols and a common JSON shape.
//!
//! * **AWS JSON 1.0** (current SDKs): `X-Amz-Target: AmazonSQS.<Action>`, JSON bodies.
//! * **AWS Query** (older SDKs, `aws sqs` v1, many libraries): form-encoded `Action=...`
//!   parameters with flattened lists/maps, XML responses.
//!
//! Handlers only ever see the JSON shape: Query parameters are converted into it on the way in,
//! and JSON responses are rendered as XML on the way out.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

pub const XML_NAMESPACE: &str = "http://queue.amazonaws.com/doc/2012-11-05/";

/// Query map members: query name -> (JSON name, key field, value field).
const QUERY_MAPS: &[(&str, &str, &str, &str)] = &[
    ("Attribute", "Attributes", "Name", "Value"),
    ("MessageAttribute", "MessageAttributes", "Name", "Value"),
    ("MessageSystemAttribute", "MessageSystemAttributes", "Name", "Value"),
    ("Tag", "Tags", "Key", "Value"),
];

/// Query list members: query name -> JSON name.
const QUERY_LISTS: &[(&str, &str)] = &[
    ("AttributeName", "AttributeNames"),
    ("MessageAttributeName", "MessageAttributeNames"),
    ("MessageSystemAttributeName", "MessageSystemAttributeNames"),
    ("TagKey", "TagKeys"),
    ("AWSAccountId", "AWSAccountIds"),
    ("ActionName", "Actions"),
    ("SendMessageBatchRequestEntry", "Entries"),
    ("DeleteMessageBatchRequestEntry", "Entries"),
    ("ChangeMessageVisibilityBatchRequestEntry", "Entries"),
    ("StringListValue", "StringListValues"),
    ("BinaryListValue", "BinaryListValues"),
];

/// Parameters that belong to the protocol/auth layer rather than the operation.
const RESERVED_PARAMS: &[&str] = &[
    "Action",
    "Version",
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "AWSAccessKeyId",
    "SignatureVersion",
    "SignatureMethod",
    "Signature",
    "Timestamp",
    "Expires",
];

#[derive(Default, Debug)]
struct Node {
    value: Option<String>,
    children: BTreeMap<String, Node>,
}

impl Node {
    fn insert(&mut self, path: &[&str], value: String) {
        match path.split_first() {
            None => self.value = Some(value),
            Some((head, rest)) => self.children.entry((*head).to_string()).or_default().insert(rest, value),
        }
    }

    /// Children keyed by numeric index, in index order (`Foo.1`, `Foo.2`, ...).
    fn indexed(&self) -> Vec<&Node> {
        let mut items: Vec<(u64, &Node)> =
            self.children.iter().filter_map(|(k, v)| k.parse::<u64>().ok().map(|i| (i, v))).collect();
        items.sort_by_key(|(i, _)| *i);
        items.into_iter().map(|(_, n)| n).collect()
    }

    fn to_json(&self) -> Value {
        if self.children.is_empty() {
            return self.value.clone().map(Value::String).unwrap_or(Value::Null);
        }
        let mut out = Map::new();
        for (key, child) in &self.children {
            if let Some((_, json_name, key_field, value_field)) = QUERY_MAPS.iter().find(|(q, ..)| q == key) {
                let mut map = Map::new();
                for entry in child.indexed() {
                    let Some(k) = entry.children.get(*key_field).and_then(|n| n.value.clone()) else {
                        continue;
                    };
                    let v = entry.children.get(*value_field).map(Node::to_json).unwrap_or(Value::Null);
                    map.insert(k, v);
                }
                out.insert((*json_name).to_string(), Value::Object(map));
            } else if let Some((_, json_name)) = QUERY_LISTS.iter().find(|(q, _)| q == key) {
                let list: Vec<Value> = child.indexed().into_iter().map(Node::to_json).collect();
                out.insert((*json_name).to_string(), Value::Array(list));
            } else if child.children.is_empty() && child.value.as_deref() == Some("") && is_json_list(key) {
                // Some SDKs serialise an empty list as a bare `Entries=`.
                out.insert(key.clone(), Value::Array(Vec::new()));
            } else if child.children.is_empty() && child.value.as_deref() == Some("") && is_json_map(key) {
                out.insert(key.clone(), Value::Object(Map::new()));
            } else {
                out.insert(key.clone(), child.to_json());
            }
        }
        Value::Object(out)
    }
}

fn is_json_list(key: &str) -> bool {
    QUERY_LISTS.iter().any(|(_, json)| *json == key)
}

fn is_json_map(key: &str) -> bool {
    QUERY_MAPS.iter().any(|(_, json, ..)| *json == key) || key == "tags"
}

/// Converts flattened Query parameters into the JSON request shape for `action`.
pub fn query_to_json(action: &str, params: &[(String, String)]) -> Value {
    let mut root = Node::default();
    for (key, value) in params {
        if RESERVED_PARAMS.contains(&key.as_str()) {
            continue;
        }
        let path: Vec<&str> = key.split('.').collect();
        root.insert(&path, value.clone());
    }
    let mut json = match root.to_json() {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    // CreateQueue's tag member is lower-case in the JSON protocol.
    if action == "CreateQueue"
        && let Some(tags) = json.remove("Tags")
    {
        json.insert("tags".into(), tags);
    }
    Value::Object(json)
}

fn escape_xml(s: &str, out: &mut String) {
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            '\r' => out.push_str("&#xD;"),
            _ => out.push(c),
        }
    }
}

fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn element(name: &str, v: &Value, out: &mut String) {
    out.push('<');
    out.push_str(name);
    out.push('>');
    match scalar(v) {
        Some(s) => escape_xml(&s, out),
        None => {
            if let Value::Object(map) = v {
                members(map, None, out);
            }
        }
    }
    out.push_str("</");
    out.push_str(name);
    out.push('>');
}

fn batch_entry_name(action: Option<&str>) -> &'static str {
    match action {
        Some("SendMessageBatch") => "SendMessageBatchResultEntry",
        Some("DeleteMessageBatch") => "DeleteMessageBatchResultEntry",
        Some("ChangeMessageVisibilityBatch") => "ChangeMessageVisibilityBatchResultEntry",
        _ => "member",
    }
}

fn members(map: &Map<String, Value>, action: Option<&str>, out: &mut String) {
    for (key, value) in map {
        match (key.as_str(), value) {
            ("Attributes", Value::Object(m)) => {
                for (k, v) in m {
                    out.push_str("<Attribute><Name>");
                    escape_xml(k, out);
                    out.push_str("</Name>");
                    element("Value", v, out);
                    out.push_str("</Attribute>");
                }
            }
            ("MessageAttributes", Value::Object(m)) => {
                for (k, v) in m {
                    out.push_str("<MessageAttribute><Name>");
                    escape_xml(k, out);
                    out.push_str("</Name>");
                    element("Value", v, out);
                    out.push_str("</MessageAttribute>");
                }
            }
            ("Tags" | "tags", Value::Object(m)) => {
                for (k, v) in m {
                    out.push_str("<Tag><Key>");
                    escape_xml(k, out);
                    out.push_str("</Key>");
                    element("Value", v, out);
                    out.push_str("</Tag>");
                }
            }
            (key, Value::Array(items)) => {
                let item_name = match key {
                    "Messages" => "Message",
                    "QueueUrls" | "queueUrls" => "QueueUrl",
                    "Successful" => batch_entry_name(action),
                    "Failed" => "BatchResultErrorEntry",
                    "Results" => "ListMessageMoveTasksResultEntry",
                    "StringListValues" => "StringListValue",
                    "BinaryListValues" => "BinaryListValue",
                    other => other,
                };
                for item in items {
                    element(item_name, item, out);
                }
            }
            (_, Value::Null) => {}
            (key, v) => element(key, v, out),
        }
    }
}

/// Renders a successful response in the Query protocol's XML format.
pub fn json_to_xml(action: &str, result: &Value, request_id: &str) -> String {
    let mut out = String::with_capacity(512);
    out.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    out.push_str(&format!(r#"<{action}Response xmlns="{XML_NAMESPACE}">"#));
    if let Value::Object(map) = result
        && !map.is_empty()
    {
        out.push_str(&format!("<{action}Result>"));
        members(map, Some(action), &mut out);
        out.push_str(&format!("</{action}Result>"));
    } else if !matches!(
        action,
        "DeleteQueue"
            | "SetQueueAttributes"
            | "PurgeQueue"
            | "DeleteMessage"
            | "ChangeMessageVisibility"
            | "TagQueue"
            | "UntagQueue"
            | "AddPermission"
            | "RemovePermission"
    ) {
        // Operations with an output shape always carry an (possibly empty) Result element.
        out.push_str(&format!("<{action}Result/>"));
    }
    out.push_str("<ResponseMetadata><RequestId>");
    escape_xml(request_id, &mut out);
    out.push_str("</RequestId></ResponseMetadata>");
    out.push_str(&format!("</{action}Response>"));
    out
}

/// Renders an error in the Query protocol's XML format.
pub fn error_xml(code: &str, message: &str, sender_fault: bool, request_id: &str) -> String {
    let mut out = String::with_capacity(256);
    out.push_str(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    out.push_str(&format!(r#"<ErrorResponse xmlns="{XML_NAMESPACE}"><Error><Type>"#));
    out.push_str(if sender_fault { "Sender" } else { "Receiver" });
    out.push_str("</Type><Code>");
    escape_xml(code, &mut out);
    out.push_str("</Code><Message>");
    escape_xml(message, &mut out);
    out.push_str("</Message><Detail/></Error><RequestId>");
    escape_xml(request_id, &mut out);
    out.push_str("</RequestId></ErrorResponse>");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn p(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn query_create_queue() {
        let v = query_to_json(
            "CreateQueue",
            &p(&[
                ("Action", "CreateQueue"),
                ("QueueName", "q1"),
                ("Attribute.1.Name", "VisibilityTimeout"),
                ("Attribute.1.Value", "40"),
                ("Attribute.2.Name", "DelaySeconds"),
                ("Attribute.2.Value", "5"),
                ("Tag.1.Key", "env"),
                ("Tag.1.Value", "prod"),
            ]),
        );
        assert_eq!(
            v,
            json!({
                "QueueName": "q1",
                "Attributes": {"VisibilityTimeout": "40", "DelaySeconds": "5"},
                "tags": {"env": "prod"}
            })
        );
    }

    #[test]
    fn query_batch_with_attributes() {
        let v = query_to_json(
            "SendMessageBatch",
            &p(&[
                ("QueueUrl", "http://x/1/q"),
                ("SendMessageBatchRequestEntry.2.Id", "b"),
                ("SendMessageBatchRequestEntry.2.MessageBody", "two"),
                ("SendMessageBatchRequestEntry.1.Id", "a"),
                ("SendMessageBatchRequestEntry.1.MessageBody", "one"),
                ("SendMessageBatchRequestEntry.1.DelaySeconds", "3"),
                ("SendMessageBatchRequestEntry.1.MessageAttribute.1.Name", "color"),
                ("SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType", "String"),
                ("SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue", "red"),
                ("SendMessageBatchRequestEntry.10.Id", "c"),
                ("SendMessageBatchRequestEntry.10.MessageBody", "ten"),
            ]),
        );
        assert_eq!(
            v,
            json!({
                "QueueUrl": "http://x/1/q",
                "Entries": [
                    {"Id": "a", "MessageBody": "one", "DelaySeconds": "3",
                     "MessageAttributes": {"color": {"DataType": "String", "StringValue": "red"}}},
                    {"Id": "b", "MessageBody": "two"},
                    {"Id": "c", "MessageBody": "ten"}
                ]
            })
        );
    }

    #[test]
    fn query_bare_empty_collections() {
        let v = query_to_json("SendMessageBatch", &p(&[("QueueUrl", "u"), ("Entries", "")]));
        assert_eq!(v, json!({"QueueUrl": "u", "Entries": []}));
        let v = query_to_json("CreateQueue", &p(&[("QueueName", "q"), ("Attributes", "")]));
        assert_eq!(v, json!({"QueueName": "q", "Attributes": {}}));
    }

    #[test]
    fn query_lists() {
        let v = query_to_json(
            "ReceiveMessage",
            &p(&[("AttributeName.1", "All"), ("MessageAttributeName.1", "a"), ("MessageAttributeName.2", "b")]),
        );
        assert_eq!(v, json!({"AttributeNames": ["All"], "MessageAttributeNames": ["a", "b"]}));
    }

    #[test]
    fn xml_receive() {
        let body = json!({"Messages": [{
            "MessageId": "m1", "Body": "a<b", "Attributes": {"SenderId": "s"},
            "MessageAttributes": {"k": {"DataType": "String", "StringValue": "v"}}
        }]});
        let xml = json_to_xml("ReceiveMessage", &body, "rid");
        assert!(xml.contains("<ReceiveMessageResult><Message>"));
        assert!(xml.contains("<Body>a&lt;b</Body>"));
        assert!(xml.contains("<Attribute><Name>SenderId</Name><Value>s</Value></Attribute>"));
        assert!(xml.contains(
            "<MessageAttribute><Name>k</Name><Value><DataType>String</DataType><StringValue>v</StringValue></Value></MessageAttribute>"
        ));
        assert!(xml.contains("<RequestId>rid</RequestId>"));
    }

    #[test]
    fn xml_batch_and_empty() {
        let body = json!({"Successful": [{"Id": "a", "MessageId": "m"}], "Failed": []});
        let xml = json_to_xml("SendMessageBatch", &body, "r");
        assert!(
            xml.contains(
                "<SendMessageBatchResultEntry><Id>a</Id><MessageId>m</MessageId></SendMessageBatchResultEntry>"
            )
        );
        let xml = json_to_xml("DeleteQueue", &json!({}), "r");
        assert!(!xml.contains("DeleteQueueResult"));
        let xml = json_to_xml("ReceiveMessage", &json!({}), "r");
        assert!(xml.contains("<ReceiveMessageResult/>"));
    }

    #[test]
    fn xml_error() {
        let xml = error_xml("AWS.SimpleQueueService.NonExistentQueue", "nope & gone", true, "r");
        assert!(xml.contains(
            "<Type>Sender</Type><Code>AWS.SimpleQueueService.NonExistentQueue</Code><Message>nope &amp; gone</Message>"
        ));
    }
}
