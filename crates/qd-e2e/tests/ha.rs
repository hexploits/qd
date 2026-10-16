//! High availability and durability: several replicas on one database, restarts.

mod common;

use std::time::{Duration, Instant};

use common::*;

#[tokio::test]
async fn replicas_share_state_and_wake_each_other() {
    let h = Harness::start_with(3, Options::default()).await;
    let a = h.client_for_replica(0).await;
    let b = h.client_for_replica(1).await;
    let c = h.client_for_replica(2).await;
    let name = qname("shared");
    let url_a = create(&a, &name, &[]).await;

    // Queue created through A is visible through B (URL host differs per replica).
    let url_b = b.get_queue_url().queue_name(&name).send().await.unwrap().queue_url.unwrap();
    assert!(url_b.ends_with(&format!("/000000000000/{name}")));

    // Long poll on B is woken by a send through C (Postgres LISTEN/NOTIFY).
    let poller = {
        let b = b.clone();
        let url_b = url_b.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            (receive_wait(&b, &url_b, 1, 15, None).await, started.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    send(&c, &url_a, "cross-replica").await;
    let (msgs, elapsed) = poller.await.unwrap();
    assert_eq!(bodies(&msgs), vec!["cross-replica"]);
    assert!(elapsed < Duration::from_millis(2500), "cross-replica wakeup took {elapsed:?}");

    // Delete through A, using the handle B received.
    delete(&a, &url_a, &msgs[0]).await;
    assert_eq!(attrs(&c, &url_a).await["ApproximateNumberOfMessagesNotVisible"], "0");
    h.stop().await;
}

#[tokio::test]
async fn messages_survive_restart() {
    let db = qd_e2e::TestDb::create().await;
    let opts = Options::default();
    let server = qd_e2e::start_server(&db.url, &opts).await;
    let c = qd_e2e::client_for(&server.endpoint(), "AKIDTEST", "secret").await;
    let url = create(&c, &qname("durable"), &[]).await;
    for i in 0..10 {
        send(&c, &url, &format!("m{i}")).await;
    }
    let inflight = receive(&c, &url, 1, Some(2)).await;
    assert_eq!(inflight.len(), 1);
    server.shutdown().await;

    // A brand-new process on the same database sees everything, including the in-flight
    // message once its visibility timeout lapses.
    let server = qd_e2e::start_server(&db.url, &opts).await;
    let c = qd_e2e::client_for(&server.endpoint(), "AKIDTEST", "secret").await;
    let path = url.split_once("://").unwrap().1.split_once('/').unwrap().1;
    let url = format!("{}/{path}", server.endpoint());
    let msgs = receive_n(&c, &url, 10, 30, Duration::from_secs(10)).await;
    let mut got = bodies(&msgs);
    got.sort();
    assert_eq!(got, (0..10).map(|i| format!("m{i}")).collect::<Vec<_>>());
    server.shutdown().await;
    db.drop_db().await;
}

#[tokio::test]
async fn replica_failure_does_not_lose_messages() {
    let db = qd_e2e::TestDb::create().await;
    let opts = Options::default();
    let a = qd_e2e::start_server(&db.url, &opts).await;
    let b = qd_e2e::start_server(&db.url, &opts).await;
    let ca = qd_e2e::client_for(&a.endpoint(), "AKIDTEST", "secret").await;
    let cb = qd_e2e::client_for(&b.endpoint(), "AKIDTEST", "secret").await;
    let url = create(&ca, &qname("failover"), &[]).await;
    for i in 0..20 {
        send(&ca, &url, &format!("m{i}")).await;
    }
    // A consumer on A takes some messages and then A dies before deleting them.
    let taken = receive(&ca, &url, 10, Some(1)).await;
    assert_eq!(taken.len(), 10);
    a.shutdown().await;

    let path = url.split_once("://").unwrap().1.split_once('/').unwrap().1;
    let url_b = format!("{}/{path}", b.endpoint());
    let msgs = receive_n(&cb, &url_b, 20, 30, Duration::from_secs(15)).await;
    let mut got = bodies(&msgs);
    got.sort();
    got.dedup();
    assert_eq!(got.len(), 20);
    b.shutdown().await;
    db.drop_db().await;
}

#[tokio::test]
async fn graceful_shutdown_releases_long_polls() {
    let h = Harness::start().await;
    let c = h.client().await;
    let url = create(&c, &qname("drain"), &[]).await;
    let poller = {
        let c = c.clone();
        let url = url.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let r = c.receive_message().queue_url(&url).wait_time_seconds(20).send().await;
            (r.is_ok(), started.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let stop = Instant::now();
    h.stop().await;
    let (ok, elapsed) = poller.await.unwrap();
    assert!(ok, "in-flight long poll completes successfully (empty) during shutdown");
    assert!(elapsed < Duration::from_secs(5), "long poll held shutdown for {elapsed:?}");
    assert!(stop.elapsed() < Duration::from_secs(10));
}
