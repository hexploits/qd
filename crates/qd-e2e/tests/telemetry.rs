//! Metrics publishing: scrape endpoint contents, Prometheus Pushgateway push, and DogStatsD.
//!
//! Kept in its own test binary (= its own process) because the global metrics recorder — which
//! decides whether DogStatsD fan-out is active — is installed once per process.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::Method;
use qd_e2e::chaos::wait_until;

#[derive(Default)]
struct Gateway {
    pushes: Vec<(Method, String, String)>,
}

async fn record(
    State(gw): State<Arc<Mutex<Gateway>>>,
    method: Method,
    uri: axum::http::Uri,
    body: Bytes,
) -> &'static str {
    gw.lock().unwrap().pushes.push((method, uri.path().to_string(), String::from_utf8_lossy(&body).into_owned()));
    ""
}

#[tokio::test]
async fn metrics_are_scraped_pushed_and_streamed() {
    // Fake Pushgateway.
    let gateway = Arc::new(Mutex::new(Gateway::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gw_addr = listener.local_addr().unwrap();
    let app = Router::new().fallback(record).with_state(gateway.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Fake DogStatsD agent.
    let udp = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let statsd_addr = udp.local_addr().unwrap();
    let packets = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        let (udp, packets) = (udp.clone(), packets.clone());
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            while let Ok(n) = udp.recv(&mut buf).await {
                packets.lock().unwrap().push(String::from_utf8_lossy(&buf[..n]).into_owned());
            }
        });
    }

    let db = qd_e2e::TestDb::create().await;
    let mut config = qd::Config::for_tests(&db.url);
    config.db_max_connections = 4;
    config.queue_metrics_interval_secs = 1;
    config.metrics_pushgateway_url = Some(format!("http://{gw_addr}"));
    config.metrics_push_interval_secs = 1;
    config.metrics_instance = Some("replica-a".into());
    config.metrics_dogstatsd_addr = Some(statsd_addr.to_string());
    config.metrics_dogstatsd_prefix = Some("qd".into());
    let server = qd::start(config).await.unwrap();

    // Traffic, including a dead-letter queue and an error.
    let c = qd_e2e::client_for(&server.endpoint(), "AKIDTEST", "secret").await;
    let dlq = c.create_queue().queue_name("metrics-dlq").send().await.unwrap().queue_url.unwrap();
    let _ = dlq;
    let url = c
        .create_queue()
        .queue_name("metrics-src")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy,
            r#"{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:metrics-dlq","maxReceiveCount":3}"#,
        )
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    for i in 0..5 {
        c.send_message().queue_url(&url).message_body(format!("m{i}")).send().await.unwrap();
    }
    let got = c.receive_message().queue_url(&url).max_number_of_messages(10).send().await.unwrap();
    for m in got.messages() {
        c.delete_message().queue_url(&url).receipt_handle(m.receipt_handle().unwrap()).send().await.unwrap();
    }
    let _ = c.get_queue_url().queue_name("nope").send().await;

    // 1. Scrape endpoint has the full metric set.
    let admin = format!("http://{}", server.admin_addr);
    let expected = [
        "qd_requests_total{action=\"SendMessage\",protocol=\"json\",code=\"OK\"} 5",
        "qd_requests_total{action=\"GetQueueUrl\",protocol=\"json\",code=\"AWS.SimpleQueueService.NonExistentQueue\"} 1",
        "qd_messages_sent_total{queue=\"metrics-src\"} 5",
        "qd_messages_deleted_total{queue=\"metrics-src\"} 5",
        "qd_message_size_bytes_count{queue=\"metrics-src\"} 5",
        "qd_queue_messages{account=\"000000000000\",queue=\"metrics-src\",state=\"visible\"} 0",
        "qd_queue_info{account=\"000000000000\",queue=\"metrics-dlq\",type=\"standard\",is_dlq=\"true\"} 1",
        "qd_queue_oldest_message_age_seconds{account=\"000000000000\",queue=\"metrics-src\"}",
        "qd_queues 2",
        "qd_ready 1",
        "qd_notifier_connected 1",
        "qd_build_info{version=",
        "qd_db_pool_connections{state=\"in_use\"}",
        "qd_db_pool_max_connections 4",
        "qd_janitor_runs_total{result=\"ok\"}",
        "qd_long_polls_in_progress",
        "# HELP qd_queue_oldest_message_age_seconds",
    ];
    let scrape_ok = wait_until(Duration::from_secs(15), || async {
        let body = reqwest::get(format!("{admin}/metrics")).await.unwrap().text().await.unwrap();
        expected.iter().all(|needle| body.contains(needle))
    })
    .await;
    let body = reqwest::get(format!("{admin}/metrics")).await.unwrap().text().await.unwrap();
    let missing: Vec<&&str> = expected.iter().filter(|n| !body.contains(**n)).collect();
    assert!(scrape_ok, "scrape is missing {missing:?}");
    assert!(!body.contains("datadog_dogstatsd_client"), "exporter self-telemetry leaked into the scrape");

    // 2. Pushgateway receives the same exposition under job/instance.
    let pushed = wait_until(Duration::from_secs(10), || async {
        gateway.lock().unwrap().pushes.iter().any(|(m, path, body)| {
            *m == Method::PUT
                && path == "/metrics/job/qd/instance/replica-a"
                && body.contains("qd_messages_sent_total{queue=\"metrics-src\"} 5")
        })
    })
    .await;
    assert!(
        pushed,
        "pushgateway did not receive metrics: {:?}",
        gateway.lock().unwrap().pushes.iter().map(|p| &p.1).collect::<Vec<_>>()
    );

    // 3. DogStatsD receives tagged, prefixed metrics.
    let streamed = wait_until(Duration::from_secs(15), || async {
        let all = packets.lock().unwrap().join("\n");
        all.contains("qd.qd_messages_sent_total:") && all.contains("queue:metrics-src")
    })
    .await;
    assert!(streamed, "dogstatsd packets: {:?}", packets.lock().unwrap());

    // 4. Shutdown removes this instance from the Pushgateway (no stale series after a pod dies).
    server.shutdown().await;
    let deleted = wait_until(Duration::from_secs(5), || async {
        gateway
            .lock()
            .unwrap()
            .pushes
            .iter()
            .any(|(m, path, _)| *m == Method::DELETE && path.ends_with("/instance/replica-a"))
    })
    .await;
    assert!(deleted, "pushgateway group not deleted on shutdown");
    db.drop_db().await;
}
