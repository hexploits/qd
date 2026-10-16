//! Dead-letter queues and redrive (message move tasks).

mod common;

use std::time::{Duration, Instant};

use aws_sdk_sqs::types::QueueAttributeName;
use common::*;

fn redrive(dlq_arn: &str, max: u32) -> String {
    format!(r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":{max}}}"#)
}

/// Receives and abandons (visibility 0 via short timeout) until the message disappears.
async fn fail_message(c: &aws_sdk_sqs::Client, url: &str, times: usize) {
    for _ in 0..times {
        let msgs = receive_wait(c, url, 1, 2, Some(1)).await;
        assert_eq!(msgs.len(), 1);
        c.change_message_visibility()
            .queue_url(url)
            .receipt_handle(msgs[0].receipt_handle().unwrap())
            .visibility_timeout(0)
            .send()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn messages_move_to_dlq_after_max_receives() {
    let h = Harness::start().await;
    let c = h.client().await;
    let dlq = create(&c, &qname("dlq"), &[]).await;
    let dlq_arn = arn(&c, &dlq).await;
    let src = create(&c, &qname("src"), &[("RedrivePolicy", &redrive(&dlq_arn, 2))]).await;
    let src_arn = arn(&c, &src).await;
    assert_eq!(attrs(&c, &src).await["RedrivePolicy"], redrive(&dlq_arn, 2));

    let id = send(&c, &src, "poison").await;
    fail_message(&c, &src, 2).await;
    // Third receive attempt exceeds maxReceiveCount: the message is dead-lettered instead.
    assert!(receive(&c, &src, 1, None).await.is_empty());
    let dead = receive_wait(&c, &dlq, 1, 2, None).await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].message_id(), Some(id.as_str()));
    assert_eq!(dead[0].body(), Some("poison"));
    assert_eq!(sys(&dead[0], "DeadLetterQueueSourceArn").as_deref(), Some(src_arn.as_str()));

    let sources = c.list_dead_letter_source_queues().queue_url(&dlq).send().await.unwrap();
    assert_eq!(sources.queue_urls(), std::slice::from_ref(&src));
    h.stop().await;
}

#[tokio::test]
async fn redrive_policy_validation() {
    let h = Harness::start().await;
    let c = h.client().await;
    let std_dlq = create(&c, &qname("stddlq"), &[]).await;
    let std_arn = arn(&c, &std_dlq).await;

    // Missing target.
    let err = c
        .create_queue()
        .queue_name(qname("x"))
        .attributes(QueueAttributeName::RedrivePolicy, redrive("arn:aws:sqs:us-east-1:000000000000:nope", 3))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    // Bad maxReceiveCount.
    let err = c
        .create_queue()
        .queue_name(qname("x"))
        .attributes(QueueAttributeName::RedrivePolicy, redrive(&std_arn, 0))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidAttributeValue");
    // FIFO source needs a FIFO DLQ.
    let err = c
        .create_queue()
        .queue_name(format!("{}.fifo", qname("x")))
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::RedrivePolicy, redrive(&std_arn, 3))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");

    // RedriveAllowPolicy denyAll blocks sources; byQueue allows the listed source only.
    c.set_queue_attributes()
        .queue_url(&std_dlq)
        .attributes(QueueAttributeName::RedriveAllowPolicy, r#"{"redrivePermission":"denyAll"}"#)
        .send()
        .await
        .unwrap();
    let err = c
        .create_queue()
        .queue_name(qname("denied"))
        .attributes(QueueAttributeName::RedrivePolicy, redrive(&std_arn, 3))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");

    let allowed_name = qname("allowed");
    let allowed_arn = format!("arn:aws:sqs:us-east-1:000000000000:{allowed_name}");
    c.set_queue_attributes()
        .queue_url(&std_dlq)
        .attributes(
            QueueAttributeName::RedriveAllowPolicy,
            format!(r#"{{"redrivePermission":"byQueue","sourceQueueArns":["{allowed_arn}"]}}"#),
        )
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await; // queue cache TTL
    create(&c, &allowed_name, &[("RedrivePolicy", &redrive(&std_arn, 3))]).await;

    // Removing a redrive policy with an empty string.
    let src = create(&c, &qname("rm"), &[]).await;
    c.set_queue_attributes()
        .queue_url(&src)
        .attributes(QueueAttributeName::RedrivePolicy, redrive(&std_arn, 3))
        .send()
        .await
        .unwrap_err();
    c.set_queue_attributes().queue_url(&src).attributes(QueueAttributeName::RedrivePolicy, "").send().await.unwrap();
    assert!(!attrs(&c, &src).await.contains_key("RedrivePolicy"));
    h.stop().await;
}

#[tokio::test]
async fn fifo_dlq_preserves_group() {
    let h = Harness::start().await;
    let c = h.client().await;
    let dlq = create(&c, &format!("{}.fifo", qname("fdlq")), &[("FifoQueue", "true")]).await;
    let dlq_arn = arn(&c, &dlq).await;
    let src = create(
        &c,
        &format!("{}.fifo", qname("fsrc")),
        &[("FifoQueue", "true"), ("RedrivePolicy", &redrive(&dlq_arn, 1))],
    )
    .await;
    send_fifo(&c, &src, "bad", "g1", "d1").await;
    send_fifo(&c, &src, "good", "g1", "d2").await;
    fail_message(&c, &src, 1).await;
    // "bad" is dead-lettered; "good" becomes deliverable in the same receive.
    let msgs = receive(&c, &src, 10, None).await;
    assert_eq!(bodies(&msgs), vec!["good"]);
    let dead = receive_wait(&c, &dlq, 10, 2, None).await;
    assert_eq!(bodies(&dead), vec!["bad"]);
    assert_eq!(sys(&dead[0], "MessageGroupId").as_deref(), Some("g1"));
    h.stop().await;
}

#[tokio::test]
async fn message_move_task_redrives_to_source() {
    let h = Harness::start().await;
    let c = h.client().await;
    let dlq = create(&c, &qname("mdlq"), &[]).await;
    let dlq_arn = arn(&c, &dlq).await;
    let src = create(&c, &qname("msrc"), &[("RedrivePolicy", &redrive(&dlq_arn, 1))]).await;

    for i in 0..5 {
        send(&c, &src, &format!("m{i}")).await;
    }
    // Fail every message once, then trigger dead-lettering.
    let first = receive_n(&c, &src, 5, 1, Duration::from_secs(10)).await;
    assert_eq!(first.len(), 5);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(receive(&c, &src, 10, None).await.is_empty());
    assert_eq!(attrs(&c, &dlq).await["ApproximateNumberOfMessages"], "5");

    // Not a DLQ -> rejected.
    let err = c.start_message_move_task().source_arn(arn(&c, &src).await).send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");

    let handle = c
        .start_message_move_task()
        .source_arn(&dlq_arn)
        .max_number_of_messages_per_second(100)
        .send()
        .await
        .unwrap()
        .task_handle
        .unwrap();
    // Only one active task per source.
    let err = c.start_message_move_task().source_arn(&dlq_arn).send().await.unwrap_err();
    assert_eq!(code(&err), "AWS.SimpleQueueService.UnsupportedOperation");

    let started = Instant::now();
    loop {
        let tasks = c.list_message_move_tasks().source_arn(&dlq_arn).max_results(10).send().await.unwrap();
        let t = &tasks.results()[0];
        if t.status() == Some("COMPLETED") {
            assert_eq!(t.approximate_number_of_messages_moved(), 5);
            assert_eq!(t.approximate_number_of_messages_to_move(), Some(5));
            assert!(t.task_handle().is_none());
            break;
        }
        assert_eq!(t.status(), Some("RUNNING"));
        assert_eq!(t.task_handle(), Some(handle.as_str()));
        assert!(started.elapsed() < Duration::from_secs(15), "move task did not complete");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let back = receive_n(&c, &src, 5, 30, Duration::from_secs(10)).await;
    let mut got = bodies(&back);
    got.sort();
    assert_eq!(got, vec!["m0", "m1", "m2", "m3", "m4"]);
    assert_eq!(sys(&back[0], "ApproximateReceiveCount").as_deref(), Some("1"));
    assert_eq!(attrs(&c, &dlq).await["ApproximateNumberOfMessages"], "0");

    let err = c.cancel_message_move_task().task_handle(&handle).send().await.unwrap_err();
    assert!(err.as_service_error().unwrap().is_resource_not_found_exception(), "{err:?}");
    h.stop().await;
}

#[tokio::test]
async fn message_move_task_to_explicit_destination_and_cancel() {
    let h = Harness::start().await;
    let c = h.client().await;
    let dlq = create(&c, &qname("cdlq"), &[]).await;
    let dlq_arn = arn(&c, &dlq).await;
    let _src = create(&c, &qname("csrc"), &[("RedrivePolicy", &redrive(&dlq_arn, 1))]).await;
    let dest = create(&c, &qname("cdest"), &[]).await;
    let dest_arn = arn(&c, &dest).await;
    // Messages placed directly on the DLQ have no origin; an explicit destination works.
    for i in 0..30 {
        send(&c, &dlq, &format!("m{i}")).await;
    }
    let handle = c
        .start_message_move_task()
        .source_arn(&dlq_arn)
        .destination_arn(&dest_arn)
        .max_number_of_messages_per_second(1)
        .send()
        .await
        .unwrap()
        .task_handle
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let moved =
        c.cancel_message_move_task().task_handle(&handle).send().await.unwrap().approximate_number_of_messages_moved();
    assert!((1..30).contains(&moved), "rate-limited task moved {moved}");
    let tasks = c.list_message_move_tasks().source_arn(&dlq_arn).send().await.unwrap();
    assert_eq!(tasks.results()[0].status(), Some("CANCELLED"));
    assert_eq!(tasks.results()[0].destination_arn(), Some(dest_arn.as_str()));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let a = attrs(&c, &dest).await;
    assert_eq!(a["ApproximateNumberOfMessages"].parse::<i64>().unwrap(), moved);
    h.stop().await;
}
