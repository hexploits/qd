//! Wire-level checks: legacy Query/XML protocol, JSON error shape, and SigV4 enforcement.

mod common;

use common::*;

async fn query(endpoint: &str, path: &str, params: &[(&str, &str)]) -> (u16, String) {
    let mut form: Vec<(&str, &str)> = vec![("Version", "2012-11-05")];
    form.extend_from_slice(params);
    let resp = reqwest::Client::new().post(format!("{endpoint}{path}")).form(&form).send().await.unwrap();
    (resp.status().as_u16(), resp.text().await.unwrap())
}

fn xml_value(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{tag}>"))? + start;
    Some(xml[start..end].to_string())
}

#[tokio::test]
async fn query_protocol_roundtrip() {
    let h = Harness::start().await;
    let ep = h.endpoint();
    let name = qname("xml");
    let (status, body) = query(
        &ep,
        "/",
        &[
            ("Action", "CreateQueue"),
            ("QueueName", &name),
            ("Attribute.1.Name", "VisibilityTimeout"),
            ("Attribute.1.Value", "5"),
            ("Tag.1.Key", "env"),
            ("Tag.1.Value", "test"),
        ],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<CreateQueueResponse xmlns=\"http://queue.amazonaws.com/doc/2012-11-05/\">"));
    let url = xml_value(&body, "QueueUrl").unwrap();
    assert!(xml_value(&body, "RequestId").is_some());

    // Posting to the queue URL path (as old SDKs do) without QueueUrl.
    let path = format!("/000000000000/{name}");
    let (status, body) = query(
        &ep,
        &path,
        &[
            ("Action", "SendMessage"),
            ("MessageBody", "<xml & stuff>"),
            ("MessageAttribute.1.Name", "k"),
            ("MessageAttribute.1.Value.DataType", "String"),
            ("MessageAttribute.1.Value.StringValue", "v"),
        ],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        xml_value(&body, "MD5OfMessageBody").unwrap(),
        hex::encode(<md5::Md5 as md5::Digest>::digest(b"<xml & stuff>"))
    );
    assert!(xml_value(&body, "MD5OfMessageAttributes").is_some());

    let (status, body) = query(
        &ep,
        "/",
        &[
            ("Action", "ReceiveMessage"),
            ("QueueUrl", &url),
            ("AttributeName.1", "All"),
            ("MessageAttributeName.1", "All"),
        ],
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<Body>&lt;xml &amp; stuff&gt;</Body>"), "{body}");
    assert!(body.contains("<Attribute><Name>ApproximateReceiveCount</Name><Value>1</Value></Attribute>"), "{body}");
    assert!(body.contains("<MessageAttribute><Name>k</Name><Value>"), "{body}");
    let handle = xml_value(&body, "ReceiptHandle").unwrap();

    let (status, body) =
        query(&ep, "/", &[("Action", "DeleteMessage"), ("QueueUrl", &url), ("ReceiptHandle", &handle)]).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("<DeleteMessageResponse"));

    let (_, body) = query(
        &ep,
        "/",
        &[("Action", "GetQueueAttributes"), ("QueueUrl", &url), ("AttributeName.1", "VisibilityTimeout")],
    )
    .await;
    assert!(body.contains("<Attribute><Name>VisibilityTimeout</Name><Value>5</Value></Attribute>"), "{body}");

    let (_, body) = query(&ep, "/", &[("Action", "ListQueueTags"), ("QueueUrl", &url)]).await;
    assert!(body.contains("<Tag><Key>env</Key><Value>test</Value></Tag>"), "{body}");

    // Batch result element names.
    let (_, body) = query(
        &ep,
        "/",
        &[
            ("Action", "SendMessageBatch"),
            ("QueueUrl", &url),
            ("SendMessageBatchRequestEntry.1.Id", "a"),
            ("SendMessageBatchRequestEntry.1.MessageBody", "one"),
            ("SendMessageBatchRequestEntry.2.Id", "b"),
            ("SendMessageBatchRequestEntry.2.MessageBody", ""),
        ],
    )
    .await;
    assert!(body.contains("<SendMessageBatchResultEntry><Id>a</Id>"), "{body}");
    assert!(body.contains("<BatchResultErrorEntry>"), "{body}");

    // GET with query-string parameters also works.
    let resp = reqwest::get(format!("{ep}/?Action=ListQueues&QueueNamePrefix={name}")).await.unwrap();
    let body = resp.text().await.unwrap();
    assert!(body.contains(&format!("<QueueUrl>{url}</QueueUrl>")), "{body}");
    h.stop().await;
}

#[tokio::test]
async fn query_protocol_errors() {
    let h = Harness::start().await;
    let ep = h.endpoint();
    let (status, body) = query(&ep, "/", &[("Action", "GetQueueUrl"), ("QueueName", "missing")]).await;
    assert_eq!(status, 400);
    assert!(body.contains("<ErrorResponse"));
    assert!(body.contains("<Type>Sender</Type>"));
    assert!(body.contains("<Code>AWS.SimpleQueueService.NonExistentQueue</Code>"));

    let (status, body) = query(&ep, "/", &[("Action", "Frobnicate")]).await;
    assert_eq!(status, 400);
    assert!(body.contains("<Code>InvalidAction</Code>"));
    h.stop().await;
}

#[tokio::test]
async fn json_error_shape() {
    let h = Harness::start().await;
    let resp = reqwest::Client::new()
        .post(h.endpoint())
        .header("content-type", "application/x-amz-json-1.0")
        .header("x-amz-target", "AmazonSQS.GetQueueUrl")
        .body(r#"{"QueueName":"missing"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    assert_eq!(
        resp.headers()["x-amzn-query-error"].to_str().unwrap(),
        "AWS.SimpleQueueService.NonExistentQueue;Sender"
    );
    assert!(resp.headers().contains_key("x-amzn-requestid"));
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["__type"], "com.amazonaws.sqs#QueueDoesNotExist");
    h.stop().await;
}

#[tokio::test]
async fn sigv4_enforcement() {
    let opts = Options {
        auth_mode: "sigv4".into(),
        credentials: Some("AKIDALPHA:alpha-secret:111111111111,AKIDBETA:beta-secret:222222222222".into()),
        ..Default::default()
    };
    let h = Harness::start_with(1, opts).await;
    let ep = h.endpoint();

    let alpha = qd_e2e::client_for(&ep, "AKIDALPHA", "alpha-secret").await;
    let beta = qd_e2e::client_for(&ep, "AKIDBETA", "beta-secret").await;
    let name = qname("signed");
    let url = create(&alpha, &name, &[]).await;
    assert!(url.contains("/111111111111/"), "{url}");
    send(&alpha, &url, "signed").await;
    assert_eq!(receive(&alpha, &url, 1, None).await.len(), 1);

    // Accounts are isolated: beta has its own namespace and can't reach alpha's queue.
    let beta_url = create(&beta, &name, &[]).await;
    assert!(beta_url.contains("/222222222222/"));
    let err = beta.send_message().queue_url(&url).message_body("x").send().await.unwrap_err();
    assert_eq!(code(&err), "AccessDenied");

    let wrong = qd_e2e::client_for(&ep, "AKIDALPHA", "not-the-secret").await;
    let err = wrong.list_queues().send().await.unwrap_err();
    assert_eq!(code(&err), "SignatureDoesNotMatch");

    let unknown = qd_e2e::client_for(&ep, "AKIDNOBODY", "x").await;
    let err = unknown.list_queues().send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidClientTokenId");

    let resp = reqwest::Client::new().post(format!("{ep}/?Action=ListQueues")).send().await.unwrap();
    assert_eq!(resp.status().as_u16(), 403);
    assert!(resp.text().await.unwrap().contains("MissingAuthenticationToken"));
    h.stop().await;
}

#[tokio::test]
async fn admin_endpoints() {
    let h = Harness::start().await;
    let admin = format!("http://{}", h.servers[0].admin_addr);
    assert_eq!(reqwest::get(format!("{admin}/healthz")).await.unwrap().status().as_u16(), 200);
    assert_eq!(reqwest::get(format!("{admin}/readyz")).await.unwrap().text().await.unwrap(), "ready");
    let c = h.client().await;
    create(&c, &qname("metrics"), &[]).await;
    let metrics = reqwest::get(format!("{admin}/metrics")).await.unwrap().text().await.unwrap();
    assert!(metrics.contains("qd_requests_total"), "{metrics}");
    h.stop().await;
}
