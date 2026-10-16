//! qd server processes.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

use super::{free_port, log};

pub struct ServerProc {
    pub name: String,
    pub binary: PathBuf,
    pub port: u16,
    pub admin_port: u16,
    pub log_path: PathBuf,
    child: Option<Child>,
}

impl ServerProc {
    /// Spawns a process running `binary <args>` (does not wait for readiness). `args` selects
    /// the role: `["server"]`, `["scheduler"]`, `["migrate"]`, `["wait-for-schema"]`, or `[]`
    /// for all-in-one.
    pub fn spawn(
        name: &str,
        binary: &Path,
        args: &[&str],
        db_url: &str,
        scratch: &Path,
        extra_env: &[(&str, String)],
    ) -> Self {
        let port = free_port();
        let admin_port = free_port();
        let log_path = scratch.join(format!("{name}.log"));
        let log_file = std::fs::OpenOptions::new().create(true).append(true).open(&log_path).expect("open server log");
        let mut cmd = Command::new(binary);
        cmd.args(args)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("QD_DATABASE_URL", db_url)
            .env("QD_BIND", format!("127.0.0.1:{port}"))
            .env("QD_ADMIN_BIND", format!("127.0.0.1:{admin_port}"))
            .env("QD_DB_MAX_CONNECTIONS", "8")
            .env("QD_DB_MIN_CONNECTIONS", "1")
            .env("QD_DB_ACQUIRE_TIMEOUT_SECS", "3")
            .env("QD_SHUTDOWN_GRACE_SECS", "10")
            .env("QD_QUEUE_METRICS_INTERVAL_SECS", "5")
            .env("QD_JANITOR_INTERVAL_SECS", "2")
            .env("QD_LOG", "info,sqlx=warn")
            .stdout(Stdio::from(log_file.try_clone().expect("clone log")))
            .stderr(Stdio::from(log_file))
            .kill_on_drop(true);
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().unwrap_or_else(|e| panic!("spawn {}: {e}", binary.display()));
        log(format!("spawned {name} [{}] on :{port} (admin :{admin_port})", args.join(" ")));
        Self { name: name.into(), binary: binary.into(), port, admin_port, log_path, child: Some(child) }
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        ([127, 0, 0, 1], self.port).into()
    }

    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn admin(&self) -> String {
        format!("http://127.0.0.1:{}", self.admin_port)
    }

    /// Whether the admin endpoint reports the process alive (liveness).
    pub async fn is_alive(&self) -> bool {
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
        matches!(client.get(format!("{}/healthz", self.admin())).send().await, Ok(r) if r.status().is_success())
    }

    /// Whether the process is still running.
    pub fn is_running(&mut self) -> bool {
        self.child.as_mut().is_some_and(|c| matches!(c.try_wait(), Ok(None)))
    }

    /// Waits for a one-shot command to exit; `Some(success)` or `None` on timeout.
    pub async fn wait_exit(&mut self, timeout: Duration) -> Option<bool> {
        let child = self.child.as_mut()?;
        match tokio::time::timeout(timeout, child.wait()).await {
            Ok(Ok(status)) => {
                log(format!("{} exited: {status}", self.name));
                self.child = None;
                Some(status.success())
            }
            _ => None,
        }
    }

    pub async fn is_ready(&self) -> bool {
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
        matches!(client.get(format!("{}/readyz", self.admin())).send().await, Ok(r) if r.status().is_success())
    }

    pub async fn wait_ready(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(child) = self.child.as_mut()
                && let Ok(Some(status)) = child.try_wait()
            {
                panic!("{} exited during startup with {status}; log:\n{}", self.name, self.tail_log(40));
            }
            if self.is_ready().await {
                log(format!("{} ready", self.name));
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{} not ready after {timeout:?}; log:\n{}", self.name, self.tail_log(40));
    }

    pub async fn metrics(&self) -> String {
        match reqwest::get(format!("{}/metrics", self.admin())).await {
            Ok(r) => r.text().await.unwrap_or_default(),
            Err(_) => String::new(),
        }
    }

    fn pid(&self) -> Option<i32> {
        self.child.as_ref().and_then(|c| c.id()).map(|p| p as i32)
    }

    /// Graceful stop: SIGTERM, then wait. Returns (exit success, time taken).
    pub async fn terminate(&mut self, timeout: Duration) -> (bool, Duration) {
        let started = Instant::now();
        if let Some(pid) = self.pid() {
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
        let mut child = self.child.take().expect("process running");
        let status = match tokio::time::timeout(timeout, child.wait()).await {
            Ok(Ok(s)) => s.success(),
            _ => {
                let _ = child.kill().await;
                false
            }
        };
        log(format!("{} terminated gracefully={status} in {:?}", self.name, started.elapsed()));
        (status, started.elapsed())
    }

    /// Hard crash: SIGKILL.
    pub async fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
            log(format!("{} killed (SIGKILL)", self.name));
        }
    }

    /// Resident memory of the process in KiB (Linux only).
    pub fn rss_kib(&self) -> Option<u64> {
        let status = std::fs::read_to_string(format!("/proc/{}/status", self.pid()?)).ok()?;
        status.lines().find(|l| l.starts_with("VmRSS:"))?.split_whitespace().nth(1)?.parse().ok()
    }

    pub fn tail_log(&self, lines: usize) -> String {
        let content = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        let all: Vec<&str> = content.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}
