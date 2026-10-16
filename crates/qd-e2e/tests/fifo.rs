//! FIFO queue semantics through the official AWS SDK for Rust.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use common::*;

async fn fifo(c: &aws_sdk_sqs::Client, prefix: &str, extra: &[(&str, &str)]) -> String {
    let mut attrs = vec![("FifoQueue", "true")];
    attrs.extend_from_slice(extra);
    create(c, &format!("{}.fifo", qname(prefix)), &attrs).await
}

#[tokio::test]
async fn fifo_attributes_and_send_rules() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = fifo(&c, "rules", &[]).await;
    let a = attrs(&c, &url).await;
    assert_eq!(a["FifoQueue"], "true");
    assert_eq!(a["ContentBasedDeduplication"], "false");
    assert_eq!(a["DeduplicationScope"], "queue");
    assert_eq!(a["FifoThroughputLimit"], "perQueue");

    // Group id required.
    let err =
        c.send_message().queue_url(&url).message_body("x").message_deduplication_id("d").send().await.unwrap_err();
    assert_eq!(code(&err), "MissingParameter");
    // Dedup id required without content-based dedup.
    let err = c.send_message().queue_url(&url).message_body("x").message_group_id("g").send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    // Per-message delay is not supported on FIFO queues.
    let err = c
        .send_message()
        .queue_url(&url)
        .message_body("x")
        .message_group_id("g")
        .message_deduplication_id("d")
        .delay_seconds(1)
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    // Cannot be turned into a standard queue.
    let err = c
        .set_queue_attributes()
        .queue_url(&url)
        .attributes(aws_sdk_sqs::types::QueueAttributeName::FifoQueue, "false")
        .send()
        .await
        .unwrap_err();
    assert!(code(&err).starts_with("InvalidAttribute"), "{}", code(&err));
    h.stop().await;
}

#[tokio::test]
async fn ordering_within_and_across_groups() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = fifo(&c, "order", &[]).await;
    let mut seqs = Vec::new();
    for i in 0..6 {
        let group = if i % 2 == 0 { "even" } else { "odd" };
        let out = c
            .send_message()
            .queue_url(&url)
            .message_body(format!("{group}-{i}"))
            .message_group_id(group)
            .message_deduplication_id(format!("d{i}"))
            .send()
            .await
            .unwrap();
        seqs.push(out.sequence_number().unwrap().parse::<u128>().unwrap());
    }
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "sequence numbers increase: {seqs:?}");

    let mut got: Vec<String> = Vec::new();
    loop {
        let msgs = receive(&c, &url, 10, None).await;
        if msgs.is_empty() {
            break;
        }
        for m in &msgs {
            assert!(sys(m, "SequenceNumber").is_some());
            assert!(sys(m, "MessageDeduplicationId").is_some());
            got.push(m.body().unwrap().to_string());
            delete(&c, &url, m).await;
        }
    }
    let even: Vec<&String> = got.iter().filter(|b| b.starts_with("even")).collect();
    let odd: Vec<&String> = got.iter().filter(|b| b.starts_with("odd")).collect();
    assert_eq!(even, vec!["even-0", "even-2", "even-4"]);
    assert_eq!(odd, vec!["odd-1", "odd-3", "odd-5"]);
    h.stop().await;
}

#[tokio::test]
async fn group_is_blocked_while_in_flight() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = fifo(&c, "block", &[]).await;
    for i in 0..3 {
        send_fifo(&c, &url, &format!("a{i}"), "A", &format!("a{i}")).await;
    }
    send_fifo(&c, &url, "b0", "B", "b0").await;

    let first = receive(&c, &url, 1, Some(30)).await;
    assert_eq!(bodies(&first), vec!["a0"]);
    // Group A is blocked by a0 in flight; only group B is deliverable.
    let second = receive(&c, &url, 10, Some(30)).await;
    assert_eq!(bodies(&second), vec!["b0"]);
    assert!(receive(&c, &url, 10, None).await.is_empty());

    // Deleting a0 unblocks the group, and the rest arrive in order.
    delete(&c, &url, &first[0]).await;
    let rest = receive(&c, &url, 10, Some(30)).await;
    assert_eq!(bodies(&rest), vec!["a1", "a2"]);

    // A failed message (visibility expiry) is redelivered before its successors.
    send_fifo(&c, &url, "c0", "C", "c0").await;
    send_fifo(&c, &url, "c1", "C", "c1").await;
    let c0 = receive(&c, &url, 1, Some(1)).await;
    assert_eq!(bodies(&c0), vec!["c0"]);
    assert!(receive(&c, &url, 10, None).await.is_empty());
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let again = receive(&c, &url, 10, Some(30)).await;
    assert_eq!(bodies(&again), vec!["c0", "c1"]);
    assert_eq!(sys(&again[0], "ApproximateReceiveCount").as_deref(), Some("2"));
    h.stop().await;
}

#[tokio::test]
async fn deduplication_explicit_and_content_based() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = fifo(&c, "dedup", &[]).await;
    let id1 = send_fifo(&c, &url, "one", "g", "same").await;
    let id2 = send_fifo(&c, &url, "one-retry", "g", "same").await;
    assert_eq!(id1, id2, "duplicate send returns the original message id");
    let msgs = receive(&c, &url, 10, None).await;
    assert_eq!(bodies(&msgs), vec!["one"]);

    let url = fifo(&c, "cbd", &[("ContentBasedDeduplication", "true")]).await;
    let a = c.send_message().queue_url(&url).message_body("payload").message_group_id("g").send().await.unwrap();
    let b = c.send_message().queue_url(&url).message_body("payload").message_group_id("g").send().await.unwrap();
    let other = c.send_message().queue_url(&url).message_body("different").message_group_id("g").send().await.unwrap();
    assert_eq!(a.message_id(), b.message_id());
    assert_eq!(a.sequence_number(), b.sequence_number());
    assert_ne!(a.message_id(), other.message_id());
    let msgs = receive(&c, &url, 10, None).await;
    assert_eq!(bodies(&msgs), vec!["payload", "different"]);

    // Dedup window expiry allows the id to be reused.
    sqlx::query("UPDATE qd_fifo_dedup SET expires_at = now() - interval '1 second'")
        .execute(h.servers[0].engine.pool())
        .await
        .unwrap();
    let c2 = c.send_message().queue_url(&url).message_body("payload").message_group_id("g").send().await.unwrap();
    assert_ne!(c2.message_id(), a.message_id());
    h.stop().await;
}

#[tokio::test]
async fn dedup_scope_message_group() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url =
        fifo(&c, "scope", &[("DeduplicationScope", "messageGroup"), ("FifoThroughputLimit", "perMessageGroupId")])
            .await;
    let a = send_fifo(&c, &url, "x", "g1", "same").await;
    let b = send_fifo(&c, &url, "x", "g2", "same").await;
    let dup = send_fifo(&c, &url, "x", "g1", "same").await;
    assert_ne!(a, b, "same dedup id in different groups is not a duplicate");
    assert_eq!(a, dup);
    h.stop().await;
}

#[tokio::test]
async fn batch_send_preserves_order() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = fifo(&c, "batch", &[]).await;
    let mut req = c.send_message_batch().queue_url(&url);
    for i in 0..10 {
        req = req.entries(
            SendMessageBatchRequestEntry::builder()
                .id(format!("e{i}"))
                .message_body(format!("m{i}"))
                .message_group_id("g")
                .message_deduplication_id(format!("d{i}"))
                .build()
                .unwrap(),
        );
    }
    let out = req.send().await.unwrap();
    assert_eq!(out.successful().len(), 10);
    let msgs = receive(&c, &url, 10, None).await;
    assert_eq!(bodies(&msgs), (0..10).map(|i| format!("m{i}")).collect::<Vec<_>>());
    h.stop().await;
}

#[tokio::test]
async fn receive_request_attempt_id_replays() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = fifo(&c, "attempt", &[]).await;
    for i in 0..3 {
        send_fifo(&c, &url, &format!("m{i}"), "g", &format!("d{i}")).await;
    }
    let recv = |attempt: &'static str| {
        let c = c.clone();
        let url = url.clone();
        async move {
            c.receive_message()
                .queue_url(&url)
                .max_number_of_messages(2)
                .receive_request_attempt_id(attempt)
                .send()
                .await
                .unwrap()
                .messages
                .unwrap_or_default()
        }
    };
    let first = recv("attempt-1").await;
    let retry = recv("attempt-1").await;
    assert_eq!(bodies(&first), vec!["m0", "m1"]);
    assert_eq!(bodies(&retry), bodies(&first));
    assert_eq!(
        first.iter().map(|m| m.receipt_handle().unwrap()).collect::<Vec<_>>(),
        retry.iter().map(|m| m.receipt_handle().unwrap()).collect::<Vec<_>>()
    );
    // A new attempt id sees the group as blocked.
    assert!(recv("attempt-2").await.is_empty());
    h.stop().await;
}

#[tokio::test]
async fn concurrent_fifo_consumers_preserve_group_order() {
    let h = Harness::start_with(2, Options::default()).await;
    let url = fifo(&h.client().await, "concurrent", &[]).await;
    let c = h.client().await;
    let groups = 5;
    let per_group = 20;
    for g in 0..groups {
        for chunk in 0..(per_group / 10) {
            let mut req = c.send_message_batch().queue_url(&url);
            for i in 0..10 {
                let n = chunk * 10 + i;
                req = req.entries(
                    SendMessageBatchRequestEntry::builder()
                        .id(format!("e{i}"))
                        .message_body(format!("{g}:{n}"))
                        .message_group_id(format!("g{g}"))
                        .message_deduplication_id(format!("{g}-{n}"))
                        .build()
                        .unwrap(),
                );
            }
            req.send().await.unwrap();
        }
    }
    // Consumers on both replicas; each records (group, n) in the order processed.
    let log = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<(usize, usize)>::new()));
    let mut tasks = Vec::new();
    for w in 0..6 {
        let c = h.client_for_replica(w % 2).await;
        let url = url.clone();
        let log = log.clone();
        tasks.push(tokio::spawn(async move {
            let mut empties = 0;
            while empties < 3 {
                let msgs = receive_wait(&c, &url, 3, 1, Some(30)).await;
                if msgs.is_empty() {
                    empties += 1;
                    continue;
                }
                empties = 0;
                for m in &msgs {
                    let (g, n) = m.body().unwrap().split_once(':').unwrap();
                    log.lock().await.push((g.parse().unwrap(), n.parse().unwrap()));
                    delete(&c, &url, m).await;
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let log = log.lock().await;
    assert_eq!(log.len(), groups * per_group);
    let mut per: HashMap<usize, Vec<usize>> = HashMap::new();
    for (g, n) in log.iter() {
        per.entry(*g).or_default().push(*n);
    }
    for (g, seq) in per {
        assert_eq!(seq, (0..per_group).collect::<Vec<_>>(), "group {g} processed out of order");
    }
    h.stop().await;
}
