//! Message operations on standard queues through the official AWS SDK for Rust.

mod common;

use std::time::{Duration, Instant};

use aws_sdk_sqs::primitives::Blob;
use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry, MessageAttributeValue,
    MessageSystemAttributeNameForSends, MessageSystemAttributeValue, SendMessageBatchRequestEntry,
};
use common::*;
use md5::{Digest, Md5};

/// Independent implementation of the SDK's message-attribute MD5.
fn attr_md5(attrs: &[(&str, &str, &[u8], u8)]) -> String {
    let mut sorted = attrs.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let mut h = Md5::new();
    for (name, dtype, value, transport) in sorted {
        for part in [name.as_bytes(), dtype.as_bytes()] {
            h.update((part.len() as u32).to_be_bytes());
            h.update(part);
        }
        h.update([transport]);
        h.update((value.len() as u32).to_be_bytes());
        h.update(value);
    }
    hex::encode(h.finalize())
}

#[tokio::test]
async fn send_receive_delete_with_md5s() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("basic"), &[]).await;

    let out = c
        .send_message()
        .queue_url(&url)
        .message_body("hello world")
        .message_attributes(
            "color",
            MessageAttributeValue::builder().data_type("String").string_value("blue").build().unwrap(),
        )
        .message_attributes(
            "count",
            MessageAttributeValue::builder().data_type("Number.int").string_value("42").build().unwrap(),
        )
        .message_attributes(
            "blob",
            MessageAttributeValue::builder()
                .data_type("Binary")
                .binary_value(Blob::new(vec![0u8, 1, 2, 255]))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let expected_attr_md5 = attr_md5(&[
        ("color", "String", b"blue", 1),
        ("count", "Number.int", b"42", 1),
        ("blob", "Binary", &[0, 1, 2, 255], 2),
    ]);
    assert_eq!(out.md5_of_message_body(), Some(hex::encode(Md5::digest(b"hello world")).as_str()));
    assert_eq!(out.md5_of_message_attributes(), Some(expected_attr_md5.as_str()));
    assert!(out.sequence_number().is_none());

    let msgs = receive(&c, &url, 10, None).await;
    assert_eq!(msgs.len(), 1);
    let m = &msgs[0];
    assert_eq!(m.message_id(), out.message_id());
    assert_eq!(m.body(), Some("hello world"));
    assert_eq!(m.md5_of_body(), out.md5_of_message_body());
    assert_eq!(m.md5_of_message_attributes(), Some(expected_attr_md5.as_str()));
    let ma = m.message_attributes().unwrap();
    assert_eq!(ma["color"].string_value(), Some("blue"));
    assert_eq!(ma["count"].data_type(), "Number.int");
    assert_eq!(ma["blob"].binary_value().unwrap().as_ref(), &[0, 1, 2, 255]);
    assert_eq!(sys(m, "ApproximateReceiveCount").as_deref(), Some("1"));
    assert_eq!(sys(m, "SenderId").as_deref(), Some("AKIDTEST"));
    assert!(sys(m, "SentTimestamp").unwrap().parse::<i64>().unwrap() > 1_600_000_000_000);
    assert!(sys(m, "ApproximateFirstReceiveTimestamp").is_some());

    // In flight: not visible to a second receive.
    assert!(receive(&c, &url, 10, None).await.is_empty());
    let a = attrs(&c, &url).await;
    assert_eq!(a["ApproximateNumberOfMessagesNotVisible"], "1");

    delete(&c, &url, m).await;
    // Deleting twice is fine.
    delete(&c, &url, m).await;
    let a = attrs(&c, &url).await;
    assert_eq!(a["ApproximateNumberOfMessagesNotVisible"], "0");
    assert_eq!(a["ApproximateNumberOfMessages"], "0");
    h.stop().await;
}

#[tokio::test]
async fn attribute_name_filters() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("filters"), &[]).await;
    let mut req = c.send_message().queue_url(&url).message_body("x");
    for n in ["a.one", "a.two", "b"] {
        req = req.message_attributes(
            n,
            MessageAttributeValue::builder().data_type("String").string_value(n).build().unwrap(),
        );
    }
    req.send().await.unwrap();
    let msgs = c
        .receive_message()
        .queue_url(&url)
        .message_attribute_names("a.*")
        .visibility_timeout(0)
        .send()
        .await
        .unwrap()
        .messages
        .unwrap();
    let ma = msgs[0].message_attributes().unwrap();
    assert_eq!(ma.len(), 2);
    assert_eq!(
        msgs[0].md5_of_message_attributes(),
        Some(attr_md5(&[("a.one", "String", b"a.one", 1), ("a.two", "String", b"a.two", 1)]).as_str())
    );
    // No attribute names requested: no attributes and no system attributes.
    let msgs = c.receive_message().queue_url(&url).send().await.unwrap().messages.unwrap();
    assert!(msgs[0].message_attributes().is_none_or(|m| m.is_empty()));
    assert!(msgs[0].attributes().is_none_or(|m| m.is_empty()));
    h.stop().await;
}

#[tokio::test]
async fn trace_header_system_attribute() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("trace"), &[]).await;
    let trace = "Root=1-5759e988-bd862e3fe1be46a994272793;Sampled=1";
    let out = c
        .send_message()
        .queue_url(&url)
        .message_body("x")
        .message_system_attributes(
            MessageSystemAttributeNameForSends::AwsTraceHeader,
            MessageSystemAttributeValue::builder().data_type("String").string_value(trace).build().unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        out.md5_of_message_system_attributes(),
        Some(attr_md5(&[("AWSTraceHeader", "String", trace.as_bytes(), 1)]).as_str())
    );
    let msgs = receive(&c, &url, 1, None).await;
    assert_eq!(sys(&msgs[0], "AWSTraceHeader").as_deref(), Some(trace));
    h.stop().await;
}

#[tokio::test]
async fn message_validation() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("validation"), &[("MaximumMessageSize", "1024")]).await;

    let err = c.send_message().queue_url(&url).message_body("a\u{1}b").send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidMessageContents");
    let err = c.send_message().queue_url(&url).message_body("x".repeat(1025)).send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    c.send_message().queue_url(&url).message_body("x".repeat(1024)).send().await.unwrap();
    let err = c.send_message().queue_url(&url).message_body("").send().await.unwrap_err();
    assert_eq!(code(&err), "MissingParameter");
    let err = c
        .send_message()
        .queue_url(&url)
        .message_body("x")
        .message_attributes(
            "AWS.reserved",
            MessageAttributeValue::builder().data_type("String").string_value("v").build().unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    let err = c
        .send_message()
        .queue_url(&url)
        .message_body("x")
        .message_attributes(
            "n",
            MessageAttributeValue::builder().data_type("Number").string_value("abc").build().unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    let err = c.send_message().queue_url(&url).message_body("x").delay_seconds(901).send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    let err =
        c.send_message().queue_url(&url).message_body("x").message_deduplication_id("d").send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");

    // Unicode outside the BMP is fine.
    let body = "emoji 🎉 and ünïcödé";
    let big = create(&c, &qname("unicode"), &[]).await;
    send(&c, &big, body).await;
    assert_eq!(receive(&c, &big, 1, None).await[0].body(), Some(body));
    h.stop().await;
}

#[tokio::test]
async fn max_size_message() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("big"), &[]).await;
    let body = "y".repeat(1_048_576);
    send(&c, &url, &body).await;
    let msgs = receive(&c, &url, 1, None).await;
    assert_eq!(msgs[0].body().unwrap().len(), 1_048_576);
    let err = c.send_message().queue_url(&url).message_body("y".repeat(1_048_577)).send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    h.stop().await;
}

#[tokio::test]
async fn batch_operations() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("batch"), &[]).await;

    let mut req = c.send_message_batch().queue_url(&url);
    for i in 0..9 {
        req = req.entries(
            SendMessageBatchRequestEntry::builder()
                .id(format!("e{i}"))
                .message_body(format!("body-{i}"))
                .build()
                .unwrap(),
        );
    }
    // One invalid entry: reported in Failed, others succeed.
    req = req.entries(
        SendMessageBatchRequestEntry::builder().id("bad").message_body("x").delay_seconds(1000).build().unwrap(),
    );
    let out = req.send().await.unwrap();
    assert_eq!(out.successful().len(), 9);
    assert_eq!(out.failed().len(), 1);
    assert_eq!(out.failed()[0].id(), "bad");
    assert_eq!(out.failed()[0].code(), "InvalidParameterValue");
    assert!(out.failed()[0].sender_fault());
    for s in out.successful() {
        let i: usize = s.id()[1..].parse().unwrap();
        assert_eq!(s.md5_of_message_body(), hex::encode(Md5::digest(format!("body-{i}"))));
    }

    let msgs = receive_n(&c, &url, 9, 30, Duration::from_secs(10)).await;
    assert_eq!(msgs.len(), 9);

    // Change visibility in batch (one bogus handle), then delete in batch.
    let mut cv = c.change_message_visibility_batch().queue_url(&url);
    for (i, m) in msgs.iter().enumerate().take(3) {
        cv = cv.entries(
            ChangeMessageVisibilityBatchRequestEntry::builder()
                .id(format!("c{i}"))
                .receipt_handle(m.receipt_handle().unwrap())
                .visibility_timeout(0)
                .build()
                .unwrap(),
        );
    }
    cv = cv.entries(
        ChangeMessageVisibilityBatchRequestEntry::builder()
            .id("nope")
            .receipt_handle("garbage")
            .visibility_timeout(5)
            .build()
            .unwrap(),
    );
    let out = cv.send().await.unwrap();
    assert_eq!(out.successful().len(), 3);
    assert_eq!(out.failed().len(), 1);
    assert_eq!(out.failed()[0].code(), "ReceiptHandleIsInvalid");

    let mut del = c.delete_message_batch().queue_url(&url);
    for (i, m) in msgs.iter().enumerate().skip(3) {
        del = del.entries(
            DeleteMessageBatchRequestEntry::builder()
                .id(format!("d{i}"))
                .receipt_handle(m.receipt_handle().unwrap())
                .build()
                .unwrap(),
        );
    }
    let out = del.send().await.unwrap();
    assert_eq!(out.successful().len(), 6);
    assert!(out.failed().is_empty());

    // The three made visible again come back.
    let again = receive_n(&c, &url, 3, 30, Duration::from_secs(10)).await;
    assert_eq!(again.len(), 3);
    h.stop().await;
}

#[tokio::test]
async fn batch_request_errors() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("batcherr"), &[]).await;

    let mut req = c.send_message_batch().queue_url(&url);
    for i in 0..11 {
        req =
            req.entries(SendMessageBatchRequestEntry::builder().id(format!("e{i}")).message_body("x").build().unwrap());
    }
    let err = req.send().await.unwrap_err();
    assert!(err.as_service_error().unwrap().is_too_many_entries_in_batch_request(), "{err:?}");

    let err = c
        .send_message_batch()
        .queue_url(&url)
        .entries(SendMessageBatchRequestEntry::builder().id("same").message_body("x").build().unwrap())
        .entries(SendMessageBatchRequestEntry::builder().id("same").message_body("y").build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert!(err.as_service_error().unwrap().is_batch_entry_ids_not_distinct(), "{err:?}");

    let err = c.send_message_batch().queue_url(&url).send().await.unwrap_err();
    assert!(err.as_service_error().unwrap().is_empty_batch_request(), "{err:?}");

    let err = c
        .send_message_batch()
        .queue_url(&url)
        .entries(SendMessageBatchRequestEntry::builder().id("bad id!").message_body("x").build().unwrap())
        .send()
        .await
        .unwrap_err();
    assert!(err.as_service_error().unwrap().is_invalid_batch_entry_id(), "{err:?}");

    let big = "z".repeat(300_000);
    let mut req = c.send_message_batch().queue_url(&url);
    for i in 0..4 {
        req = req
            .entries(SendMessageBatchRequestEntry::builder().id(format!("b{i}")).message_body(&big).build().unwrap());
    }
    let err = req.send().await.unwrap_err();
    assert!(err.as_service_error().unwrap().is_batch_request_too_long(), "{err:?}");
    h.stop().await;
}

#[tokio::test]
async fn visibility_timeout_redelivery() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("vis"), &[]).await;
    send(&c, &url, "again").await;

    let first = receive(&c, &url, 1, Some(1)).await;
    assert_eq!(first.len(), 1);
    assert!(receive(&c, &url, 1, None).await.is_empty());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let second = receive(&c, &url, 1, Some(30)).await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].message_id(), first[0].message_id());
    assert_ne!(second[0].receipt_handle(), first[0].receipt_handle());
    assert_eq!(sys(&second[0], "ApproximateReceiveCount").as_deref(), Some("2"));
    assert_eq!(sys(&second[0], "ApproximateFirstReceiveTimestamp"), sys(&first[0], "ApproximateFirstReceiveTimestamp"));

    // The stale handle can no longer change visibility...
    let err = c
        .change_message_visibility()
        .queue_url(&url)
        .receipt_handle(first[0].receipt_handle().unwrap())
        .visibility_timeout(10)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    // ...but the current one can, and visibility 0 makes it immediately available.
    c.change_message_visibility()
        .queue_url(&url)
        .receipt_handle(second[0].receipt_handle().unwrap())
        .visibility_timeout(0)
        .send()
        .await
        .unwrap();
    let third = receive(&c, &url, 1, None).await;
    assert_eq!(third.len(), 1);
    assert_eq!(sys(&third[0], "ApproximateReceiveCount").as_deref(), Some("3"));

    let err = c.delete_message().queue_url(&url).receipt_handle("not-a-handle").send().await.unwrap_err();
    assert!(err.as_service_error().unwrap().is_receipt_handle_is_invalid(), "{err:?}");
    h.stop().await;
}

#[tokio::test]
async fn queue_default_visibility_timeout_is_used() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("defvis"), &[("VisibilityTimeout", "1")]).await;
    send(&c, &url, "x").await;
    assert_eq!(receive(&c, &url, 1, None).await.len(), 1);
    assert!(receive(&c, &url, 1, None).await.is_empty());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(receive(&c, &url, 1, None).await.len(), 1);
    h.stop().await;
}

#[tokio::test]
async fn delays() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("delay"), &[("DelaySeconds", "2")]).await;
    send(&c, &url, "queue-delay").await;
    c.send_message().queue_url(&url).message_body("no-delay").delay_seconds(0).send().await.unwrap();

    let a = attrs(&c, &url).await;
    assert_eq!(a["ApproximateNumberOfMessagesDelayed"], "1");
    assert_eq!(a["ApproximateNumberOfMessages"], "1");
    let now = receive(&c, &url, 10, None).await;
    assert_eq!(bodies(&now), vec!["no-delay"]);

    // Long poll picks the delayed message up as soon as it becomes visible.
    let started = Instant::now();
    let later = receive_wait(&c, &url, 10, 10, None).await;
    assert_eq!(bodies(&later), vec!["queue-delay"]);
    assert!(started.elapsed() < Duration::from_secs(4), "took {:?}", started.elapsed());
    h.stop().await;
}

#[tokio::test]
async fn long_polling() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("longpoll"), &[]).await;

    // Empty queue: waits for roughly WaitTimeSeconds.
    let started = Instant::now();
    assert!(receive_wait(&c, &url, 1, 2, None).await.is_empty());
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(1900) && waited < Duration::from_secs(4), "waited {waited:?}");

    // A send during the wait releases it immediately.
    let c2 = c.clone();
    let url2 = url.clone();
    let poller = tokio::spawn(async move {
        let started = Instant::now();
        let msgs = receive_wait(&c2, &url2, 1, 15, None).await;
        (msgs, started.elapsed())
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    send(&c, &url, "wake up").await;
    let (msgs, elapsed) = poller.await.unwrap();
    assert_eq!(bodies(&msgs), vec!["wake up"]);
    assert!(elapsed < Duration::from_secs(3), "long poll took {elapsed:?}");

    // Queue-level ReceiveMessageWaitTimeSeconds applies when the request omits it.
    c.set_queue_attributes()
        .queue_url(&url)
        .attributes(aws_sdk_sqs::types::QueueAttributeName::ReceiveMessageWaitTimeSeconds, "1")
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await; // queue cache TTL
    let started = Instant::now();
    assert!(receive(&c, &url, 1, None).await.is_empty());
    assert!(started.elapsed() >= Duration::from_millis(900));

    let err = c.receive_message().queue_url(&url).wait_time_seconds(21).send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    let err = c.receive_message().queue_url(&url).max_number_of_messages(11).send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    h.stop().await;
}

#[tokio::test]
async fn retention_expires_messages() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("retention"), &[("MessageRetentionPeriod", "60")]).await;
    send(&c, &url, "old").await;
    // Age the message artificially instead of waiting a minute.
    sqlx::query("UPDATE qd_messages SET expires_at = now() - interval '1 second'")
        .execute(h.servers[0].engine.pool())
        .await
        .unwrap();
    assert!(receive(&c, &url, 10, None).await.is_empty());
    assert_eq!(attrs(&c, &url).await["ApproximateNumberOfMessages"], "0");
    h.servers[0].engine.janitor_pass().await.unwrap();
    let (left,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM qd_messages").fetch_one(h.servers[0].engine.pool()).await.unwrap();
    assert_eq!(left, 0);
    h.stop().await;
}

#[tokio::test]
async fn concurrent_consumers_never_share_a_message() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("concurrent"), &[]).await;
    for chunk in 0..20 {
        let mut req = c.send_message_batch().queue_url(&url);
        for i in 0..10 {
            req = req.entries(
                SendMessageBatchRequestEntry::builder()
                    .id(format!("e{i}"))
                    .message_body(format!("{}", chunk * 10 + i))
                    .build()
                    .unwrap(),
            );
        }
        req.send().await.unwrap();
    }
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let c = c.clone();
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            loop {
                let msgs = receive_wait(&c, &url, 10, 1, Some(60)).await;
                if msgs.is_empty() {
                    break;
                }
                for m in &msgs {
                    got.push(m.body().unwrap().to_string());
                    delete(&c, &url, m).await;
                }
            }
            got
        }));
    }
    let mut all: Vec<String> = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let total = all.len();
    all.sort();
    all.dedup();
    assert_eq!(total, 200, "every message delivered exactly once within its visibility timeout");
    assert_eq!(all.len(), 200);
    h.stop().await;
}
