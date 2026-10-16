//! Delivery-guarantee verifier: runs a standard and a FIFO workload against any SQS endpoint
//! for a while, then drains and checks that no acknowledged message was lost and FIFO order
//! held. Exits non-zero on violation. Used by the Kubernetes resilience test (run in-cluster
//! while pods are restarted, upgraded and killed) and usable against staging environments.
//!
//! ```text
//! verify <endpoint> [seconds=120] [--allow-client-errors]
//! ```

use std::time::{Duration, Instant};

use qd_e2e::chaos::workload::{Workload, WorkloadConfig};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args.get(1).cloned().unwrap_or_else(|| "http://127.0.0.1:9324".into());
    let seconds: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(120);
    let allow_errors = args.iter().any(|a| a == "--allow-client-errors");

    let started = Instant::now();
    let mut std_cfg = WorkloadConfig::standard(&qd_e2e::qname("verify"));
    let mut fifo_cfg = WorkloadConfig::fifo(&qd_e2e::qname("verify"));
    // Generous retries: in Kubernetes a failover can take tens of seconds.
    std_cfg.max_attempts = 20;
    fifo_cfg.max_attempts = 20;
    let std_load = Workload::start(&endpoint, std_cfg).await;
    let fifo_load = Workload::start(&endpoint, fifo_cfg).await;

    let deadline = started + Duration::from_secs(seconds);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!(
            "[{:>4}s] sent std={} fifo={} received std={} fifo={} client-errors={}",
            started.elapsed().as_secs(),
            std_load.sent(),
            fifo_load.sent(),
            std_load.received_count().await,
            fifo_load.received_count().await,
            std_load.error_count().await + fifo_load.error_count().await
        );
    }

    let reports = [
        ("standard", std_load.finish(Duration::from_secs(180), started).await),
        ("fifo", fifo_load.finish(Duration::from_secs(180), started).await),
    ];
    let mut ok = true;
    for (name, r) in &reports {
        println!(
            "{name}: acked={} unknown={} received={} duplicates={} lost={} fifo_violations={} client_errors={}",
            r.acked,
            r.unknown,
            r.received_unique,
            r.duplicates,
            r.lost.len(),
            r.fifo_violations.len(),
            r.errors.len()
        );
        for (at, e) in r.errors.iter().take(5) {
            println!("  error at {at:?}: {e}");
        }
        ok &= r.acked > 0 && r.lost.is_empty() && r.fifo_violations.is_empty() && (allow_errors || r.errors.is_empty());
    }
    if ok {
        println!("VERIFY PASSED");
    } else {
        println!("VERIFY FAILED");
        std::process::exit(1);
    }
}
