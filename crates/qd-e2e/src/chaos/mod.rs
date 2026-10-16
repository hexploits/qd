//! Chaos / resilience harness.
//!
//! Everything here runs *real processes*: qd server binaries (so crashes are real
//! `SIGKILL`s and upgrades can mix binaries from different releases) and real PostgreSQL
//! instances (so failover is a real primary loss and standby promotion). A small TCP proxy plays
//! the role of a Kubernetes Service in front of the replicas, and another plays the role of
//! CloudNativePG's `-rw` Service in front of the current Postgres primary.

pub mod pg;
pub mod proc;
pub mod proxy;
pub mod workload;

use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Repository root (two levels above this crate).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("repo root")
}

/// The server binary under test: `QD_BINARY`, else `target/{release,debug}/qd`.
pub fn server_binary() -> PathBuf {
    if let Ok(p) = std::env::var("QD_BINARY") {
        return PathBuf::from(p);
    }
    for profile in ["debug", "release"] {
        let p = repo_root().join("target").join(profile).join("qd");
        if p.exists() {
            return p;
        }
    }
    panic!("qd binary not found: run `cargo build -p qd` or set QD_BINARY");
}

/// The binary of the release being upgraded *from* (`QD_UPGRADE_FROM_BINARY`), if provided.
pub fn previous_binary() -> Option<PathBuf> {
    std::env::var("QD_UPGRADE_FROM_BINARY").ok().map(PathBuf::from)
}

/// A free TCP port on localhost.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").expect("bind").local_addr().expect("addr").port()
}

/// Scratch directory for one chaos scenario; kept on failure for post-mortems.
pub fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("qd-chaos-{name}-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Timestamped progress output (visible with `--nocapture`).
pub fn log(msg: impl AsRef<str>) {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let t = START.get_or_init(Instant::now).elapsed();
    eprintln!("[chaos {:>7.2}s] {}", t.as_secs_f64(), msg.as_ref());
}

/// Polls `check` until it returns true or the timeout passes.
pub async fn wait_until<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
