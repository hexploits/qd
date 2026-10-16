//! Queue management through the official AWS SDK for Rust (JSON protocol).

mod common;

use aws_sdk_sqs::types::QueueAttributeName;
use common::*;

#[tokio::test]
async fn create_get_list_delete() {
    let h = Harness::start().await;
    let c = h.client().await;
    let name = qname("lifecycle");

    let url = create(&c, &name, &[]).await;
    assert_eq!(url, format!("{}/000000000000/{name}", h.endpoint()));

    let got = c.get_queue_url().queue_name(&name).send().await.unwrap();
    assert_eq!(got.queue_url(), Some(url.as_str()));

    let listed = c.list_queues().send().await.unwrap();
    assert!(listed.queue_urls().contains(&url));

    c.delete_queue().queue_url(&url).send().await.unwrap();
    let err = c.get_queue_url().queue_name(&name).send().await.unwrap_err();
    assert!(err.as_service_error().unwrap().is_queue_does_not_exist(), "{err:?}");
    assert_eq!(code(&err), "AWS.SimpleQueueService.NonExistentQueue");

    let err = c.delete_queue().queue_url(&url).send().await.unwrap_err();
    assert!(err.as_service_error().unwrap().is_queue_does_not_exist());
    h.stop().await;
}

#[tokio::test]
async fn create_is_idempotent_but_rejects_conflicts() {
    let h = Harness::start().await;
    let c = h.client().await;
    let name = qname("idem");
    let url1 = create(&c, &name, &[("VisibilityTimeout", "45")]).await;
    let url2 = create(&c, &name, &[("VisibilityTimeout", "45")]).await;
    assert_eq!(url1, url2);
    // Omitting attributes is not a conflict.
    let url3 = create(&c, &name, &[]).await;
    assert_eq!(url1, url3);

    let err = c
        .create_queue()
        .queue_name(&name)
        .attributes(QueueAttributeName::VisibilityTimeout, "46")
        .send()
        .await
        .unwrap_err();
    assert!(err.as_service_error().unwrap().is_queue_name_exists(), "{err:?}");
    assert_eq!(code(&err), "QueueAlreadyExists");
    h.stop().await;
}

#[tokio::test]
async fn queue_name_validation() {
    let h = Harness::start().await;
    let c = h.client().await;
    for bad in ["has space", "dot.name", &"x".repeat(81)] {
        let err = c.create_queue().queue_name(bad).send().await.unwrap_err();
        assert_eq!(code(&err), "InvalidParameterValue", "{bad}");
    }
    // .fifo suffix requires FifoQueue=true and vice versa.
    let err = c.create_queue().queue_name("plain.fifo").send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    let err = c
        .create_queue()
        .queue_name("nofifosuffix")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    h.stop().await;
}

#[tokio::test]
async fn default_and_custom_attributes() {
    let h = Harness::start().await;
    let c = h.client().await;
    let name = qname("attrs");
    let url = create(&c, &name, &[("DelaySeconds", "5"), ("MessageRetentionPeriod", "120")]).await;
    let a = attrs(&c, &url).await;
    assert_eq!(a["DelaySeconds"], "5");
    assert_eq!(a["MessageRetentionPeriod"], "120");
    assert_eq!(a["VisibilityTimeout"], "30");
    assert_eq!(a["MaximumMessageSize"], "1048576");
    assert_eq!(a["ReceiveMessageWaitTimeSeconds"], "0");
    assert_eq!(a["ApproximateNumberOfMessages"], "0");
    assert_eq!(a["QueueArn"], format!("arn:aws:sqs:us-east-1:000000000000:{name}"));
    assert!(a.contains_key("CreatedTimestamp"));
    assert!(!a.contains_key("FifoQueue"));

    c.set_queue_attributes()
        .queue_url(&url)
        .attributes(QueueAttributeName::VisibilityTimeout, "7")
        .attributes(QueueAttributeName::ReceiveMessageWaitTimeSeconds, "3")
        .send()
        .await
        .unwrap();
    let a = attrs(&c, &url).await;
    assert_eq!(a["VisibilityTimeout"], "7");
    assert_eq!(a["ReceiveMessageWaitTimeSeconds"], "3");

    // Selective retrieval.
    let only = c
        .get_queue_attributes()
        .queue_url(&url)
        .attribute_names(QueueAttributeName::VisibilityTimeout)
        .send()
        .await
        .unwrap();
    let only = only.attributes().unwrap();
    assert_eq!(only.len(), 1);
    assert_eq!(only[&QueueAttributeName::VisibilityTimeout], "7");
    h.stop().await;
}

#[tokio::test]
async fn attribute_validation_errors() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("attrval"), &[]).await;
    let err = c
        .set_queue_attributes()
        .queue_url(&url)
        .attributes(QueueAttributeName::VisibilityTimeout, "43201")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidAttributeValue");
    let err = c
        .set_queue_attributes()
        .queue_url(&url)
        .attributes(QueueAttributeName::from("NotAThing"), "1")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidAttributeName");
    let err = c
        .set_queue_attributes()
        .queue_url(&url)
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidAttributeName");
    let err = c
        .get_queue_attributes()
        .queue_url(&url)
        .attribute_names(QueueAttributeName::from("Bogus"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidAttributeName");
    h.stop().await;
}

#[tokio::test]
async fn list_queues_prefix_and_pagination() {
    let h = Harness::start().await;
    let c = h.client().await;
    let prefix = qname("page");
    let mut urls = Vec::new();
    for i in 0..5 {
        urls.push(create(&c, &format!("{prefix}-{i}"), &[]).await);
    }
    create(&c, &qname("other"), &[]).await;

    let all = c.list_queues().queue_name_prefix(&prefix).send().await.unwrap();
    assert_eq!(all.queue_urls().len(), 5);
    assert!(all.next_token().is_none());

    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = c
            .list_queues()
            .queue_name_prefix(&prefix)
            .max_results(2)
            .set_next_token(token.clone())
            .send()
            .await
            .unwrap();
        assert!(page.queue_urls().len() <= 2);
        seen.extend(page.queue_urls().iter().cloned());
        token = page.next_token().map(str::to_string);
        if token.is_none() {
            break;
        }
    }
    assert_eq!(seen, urls);

    let none = c.list_queues().queue_name_prefix("does-not-exist-").send().await.unwrap();
    assert!(none.queue_urls().is_empty());
    h.stop().await;
}

#[tokio::test]
async fn tags() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url =
        c.create_queue().queue_name(qname("tags")).tags("team", "payments").send().await.unwrap().queue_url.unwrap();
    c.tag_queue().queue_url(&url).tags("env", "prod").tags("team", "core").send().await.unwrap();
    let tags = c.list_queue_tags().queue_url(&url).send().await.unwrap().tags.unwrap();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags["team"], "core");
    assert_eq!(tags["env"], "prod");
    c.untag_queue().queue_url(&url).tag_keys("team").send().await.unwrap();
    let tags = c.list_queue_tags().queue_url(&url).send().await.unwrap().tags.unwrap_or_default();
    assert_eq!(tags.len(), 1);
    h.stop().await;
}

#[tokio::test]
async fn permissions_are_reflected_in_policy() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("perm"), &[]).await;
    c.add_permission()
        .queue_url(&url)
        .label("allow-producer")
        .aws_account_ids("111122223333")
        .actions("SendMessage")
        .send()
        .await
        .unwrap();
    let policy: serde_json::Value = serde_json::from_str(&attrs(&c, &url).await["Policy"]).unwrap();
    assert_eq!(policy["Statement"][0]["Sid"], "allow-producer");
    assert_eq!(policy["Statement"][0]["Action"][0], "SQS:SendMessage");

    let err = c
        .add_permission()
        .queue_url(&url)
        .label("allow-producer")
        .aws_account_ids("111122223333")
        .actions("SendMessage")
        .send()
        .await
        .unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");

    c.remove_permission().queue_url(&url).label("allow-producer").send().await.unwrap();
    assert!(!attrs(&c, &url).await.contains_key("Policy"));
    let err = c.remove_permission().queue_url(&url).label("allow-producer").send().await.unwrap_err();
    assert_eq!(code(&err), "InvalidParameterValue");
    h.stop().await;
}

#[tokio::test]
async fn purge_queue_and_cooldown() {
    let h = Harness::start_with(1, Options { purge_cooldown_secs: 60, ..Default::default() }).await;
    let c = h.client().await;
    let url = create(&c, &qname("purge"), &[]).await;
    for i in 0..5 {
        send(&c, &url, &format!("m{i}")).await;
    }
    c.purge_queue().queue_url(&url).send().await.unwrap();
    assert_eq!(attrs(&c, &url).await["ApproximateNumberOfMessages"], "0");
    assert!(receive(&c, &url, 10, None).await.is_empty());
    let err = c.purge_queue().queue_url(&url).send().await.unwrap_err();
    assert!(err.as_service_error().unwrap().is_purge_queue_in_progress(), "{err:?}");
    h.stop().await;
}

#[tokio::test]
async fn deleted_queue_messages_are_reclaimed() {
    let h = Harness::start().await;
    let c = h.client().await;
    let name = qname("reclaim");
    let url = create(&c, &name, &[]).await;
    for i in 0..20 {
        send(&c, &url, &format!("m{i}")).await;
    }
    c.delete_queue().queue_url(&url).send().await.unwrap();
    // Recreating gives a fresh, empty queue.
    let url = create(&c, &name, &[]).await;
    assert!(receive(&c, &url, 10, None).await.is_empty());
    h.servers[0].engine.janitor_pass().await.unwrap();
    let (orphans,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM qd_messages m WHERE NOT EXISTS (SELECT 1 FROM qd_queues q WHERE q.id = m.queue_id)",
    )
    .fetch_one(h.servers[0].engine.pool())
    .await
    .unwrap();
    assert_eq!(orphans, 0);
    h.stop().await;
}
