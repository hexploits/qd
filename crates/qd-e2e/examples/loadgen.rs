//! Load generator using the official AWS SDK.
//!
//! ```text
//! cargo run --release -p qd-e2e --example loadgen -- <endpoint> [seconds] [producers] [consumers] [fifo]
//! ```
//!
//! Producers send batches of 10; consumers long-poll batches of 10 and batch-delete them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aws_sdk_sqs::types::{DeleteMessageBatchRequestEntry, SendMessageBatchRequestEntry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args.get(1).cloned().unwrap_or_else(|| "http://127.0.0.1:9324".into());
    let seconds: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20);
    let producers: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(16);
    let consumers: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(16);
    let fifo = args.get(5).is_some_and(|s| s == "fifo");

    let client = qd_e2e::client_for(&endpoint, "AKIDLOAD", "secret").await;
    let name = if fifo { format!("{}.fifo", qd_e2e::qname("load")) } else { qd_e2e::qname("load") };
    let mut create = client.create_queue().queue_name(&name);
    if fifo {
        create = create.attributes(aws_sdk_sqs::types::QueueAttributeName::FifoQueue, "true");
    }
    let url = create.send().await?.queue_url.unwrap();

    let sent = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let send_latency_us = Arc::new(AtomicU64::new(0));
    let send_calls = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(seconds);

    let mut tasks = Vec::new();
    for p in 0..producers {
        let (client, url, sent, lat, calls) =
            (client.clone(), url.clone(), sent.clone(), send_latency_us.clone(), send_calls.clone());
        tasks.push(tokio::spawn(async move {
            let mut n = 0u64;
            while Instant::now() < deadline {
                let mut req = client.send_message_batch().queue_url(&url);
                for i in 0..10 {
                    let mut e = SendMessageBatchRequestEntry::builder()
                        .id(format!("e{i}"))
                        .message_body(format!("producer {p} message {n} {}", "x".repeat(200)));
                    if fifo {
                        e = e.message_group_id(format!("g{}", n % 50)).message_deduplication_id(format!("{p}-{n}"));
                    }
                    req = req.entries(e.build().unwrap());
                    n += 1;
                }
                let started = Instant::now();
                if let Ok(out) = req.send().await {
                    sent.fetch_add(out.successful().len() as u64, Ordering::Relaxed);
                    lat.fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
                    calls.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for _ in 0..consumers {
        let (client, url, received) = (client.clone(), url.clone(), received.clone());
        tasks.push(tokio::spawn(async move {
            while Instant::now() < deadline + Duration::from_secs(5) {
                let Ok(out) = client
                    .receive_message()
                    .queue_url(&url)
                    .max_number_of_messages(10)
                    .wait_time_seconds(1)
                    .send()
                    .await
                else {
                    continue;
                };
                let msgs = out.messages.unwrap_or_default();
                if msgs.is_empty() {
                    if Instant::now() > deadline {
                        break;
                    }
                    continue;
                }
                let mut del = client.delete_message_batch().queue_url(&url);
                for (i, m) in msgs.iter().enumerate() {
                    del = del.entries(
                        DeleteMessageBatchRequestEntry::builder()
                            .id(format!("d{i}"))
                            .receipt_handle(m.receipt_handle().unwrap())
                            .build()
                            .unwrap(),
                    );
                }
                if del.send().await.is_ok() {
                    received.fetch_add(msgs.len() as u64, Ordering::Relaxed);
                }
            }
        }));
    }
    let started = Instant::now();
    let reporter = {
        let (sent, received) = (sent.clone(), received.clone());
        tokio::spawn(async move {
            let mut last = (0, 0);
            while Instant::now() < deadline {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let now = (sent.load(Ordering::Relaxed), received.load(Ordering::Relaxed));
                println!("  +5s: sent {}/s, received {}/s", (now.0 - last.0) / 5, (now.1 - last.1) / 5);
                last = now;
            }
        })
    };
    for t in tasks {
        let _ = t.await;
    }
    reporter.abort();
    let elapsed = started.elapsed().as_secs_f64();
    let s = sent.load(Ordering::Relaxed);
    let r = received.load(Ordering::Relaxed);
    let calls = send_calls.load(Ordering::Relaxed).max(1);
    println!(
        "queue={name} producers={producers} consumers={consumers} duration={seconds}s\n\
         sent={s} ({:.0} msg/s) received+deleted={r} ({:.0} msg/s) avg SendMessageBatch latency={:.1}ms",
        s as f64 / seconds as f64,
        r as f64 / elapsed,
        send_latency_us.load(Ordering::Relaxed) as f64 / calls as f64 / 1000.0
    );
    client.delete_queue().queue_url(&url).send().await?;
    Ok(())
}
