//! Real PostgreSQL instances (primary / streaming standby) managed with the PG binaries.

use std::path::{Path, PathBuf};
use std::process::Command;

use sqlx::Connection;
use sqlx::postgres::PgConnection;

use super::{free_port, log};

/// Directory with initdb/pg_ctl/pg_basebackup: `QD_PG_BIN`, `pg_config --bindir`, or the newest
/// `/usr/lib/postgresql/*/bin`.
pub fn pg_bin_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("QD_PG_BIN") {
        return PathBuf::from(dir);
    }
    if let Ok(out) = Command::new("pg_config").arg("--bindir").output()
        && out.status.success()
    {
        let dir = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
        if dir.join("pg_ctl").exists() {
            return dir;
        }
    }
    let mut candidates: Vec<PathBuf> = std::fs::read_dir("/usr/lib/postgresql")
        .map(|rd| rd.flatten().map(|e| e.path().join("bin")).filter(|p| p.join("pg_ctl").exists()).collect())
        .unwrap_or_default();
    candidates.sort();
    candidates.pop().expect("PostgreSQL server binaries not found; set QD_PG_BIN")
}

pub struct PgNode {
    pub name: String,
    pub port: u16,
    data: PathBuf,
    socket_dir: PathBuf,
    log_path: PathBuf,
    bin: PathBuf,
    running: bool,
}

/// PostgreSQL refuses to run as root (common in CI containers); run its tools as `postgres` then.
fn is_root() -> bool {
    unsafe { libc::geteuid() == 0 }
}

fn pg_command(bin: &Path, tool: &str) -> Command {
    if is_root() {
        let mut c = Command::new("runuser");
        c.args(["-u", "postgres", "--"]).arg(bin.join(tool));
        c
    } else {
        Command::new(bin.join(tool))
    }
}

/// Lets the `postgres` user write into scratch directories when running as root.
fn open_up(dir: &Path) {
    if is_root() {
        let _ = Command::new("chmod").args(["0777", dir.to_str().unwrap()]).status();
    }
}

fn run(bin: &Path, tool: &str, args: &[&str]) {
    let out = pg_command(bin, tool).args(args).output().unwrap_or_else(|e| panic!("run {tool}: {e}"));
    assert!(
        out.status.success(),
        "{tool} {args:?} failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

impl PgNode {
    fn paths(scratch: &Path, name: &str) -> (PathBuf, PathBuf, PathBuf) {
        let data = scratch.join(format!("{name}-data"));
        let socket_dir = scratch.join(format!("{name}-sock"));
        std::fs::create_dir_all(&socket_dir).unwrap();
        open_up(scratch);
        open_up(&socket_dir);
        (data, socket_dir, scratch.join(format!("{name}-postgres.log")))
    }

    fn write_conf(&self, extra: &str) {
        let conf = format!(
            "port = {}\nlisten_addresses = '127.0.0.1'\nunix_socket_directories = '{}'\n\
             wal_level = replica\nmax_wal_senders = 10\nhot_standby = on\nmax_connections = 200\n\
             wal_keep_size = '256MB'\nfsync = on\nsynchronous_commit = on\nshared_buffers = '64MB'\n{extra}\n",
            self.port,
            self.socket_dir.display()
        );
        let path = self.data.join("postgresql.conf");
        let mut base = std::fs::read_to_string(&path).unwrap_or_default();
        base.push_str("\n# --- qd-chaos ---\n");
        base.push_str(&conf);
        std::fs::write(path, base).unwrap();
    }

    /// Initialises and starts a new primary.
    pub fn primary(scratch: &Path, name: &str) -> Self {
        let bin = pg_bin_dir();
        let (data, socket_dir, log_path) = Self::paths(scratch, name);
        run(&bin, "initdb", &["-D", data.to_str().unwrap(), "-U", "qd", "--auth=trust", "-E", "UTF8", "--no-sync"]);
        std::fs::write(
            data.join("pg_hba.conf"),
            "local all all trust\nhost all all 127.0.0.1/32 trust\nhost replication all 127.0.0.1/32 trust\n",
        )
        .unwrap();
        let mut node = Self { name: name.into(), port: free_port(), data, socket_dir, log_path, bin, running: false };
        node.write_conf("");
        node.start();
        node
    }

    /// Clones `primary` with pg_basebackup and starts it as a streaming standby.
    pub fn standby(scratch: &Path, name: &str, primary: &PgNode) -> Self {
        let bin = pg_bin_dir();
        let (data, socket_dir, log_path) = Self::paths(scratch, name);
        run(
            &bin,
            "pg_basebackup",
            &[
                "-h",
                "127.0.0.1",
                "-p",
                &primary.port.to_string(),
                "-U",
                "qd",
                "-D",
                data.to_str().unwrap(),
                "-R",
                "-X",
                "stream",
                "-c",
                "fast",
                "--no-sync",
            ],
        );
        let mut node = Self { name: name.into(), port: free_port(), data, socket_dir, log_path, bin, running: false };
        node.write_conf(&format!(
            "primary_conninfo = 'host=127.0.0.1 port={} user=qd application_name={name}'",
            primary.port
        ));
        node.start();
        node
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        ([127, 0, 0, 1], self.port).into()
    }

    pub fn url(&self, db: &str) -> String {
        format!("postgres://qd@127.0.0.1:{}/{db}", self.port)
    }

    pub fn start(&mut self) {
        run(
            &self.bin,
            "pg_ctl",
            &["-D", self.data.to_str().unwrap(), "-l", self.log_path.to_str().unwrap(), "-w", "-t", "60", "start"],
        );
        self.running = true;
        log(format!("postgres {} up on :{}", self.name, self.port));
    }

    /// Clean shutdown (`-m fast`): disconnects clients, checkpoints.
    pub fn stop(&mut self) {
        run(&self.bin, "pg_ctl", &["-D", self.data.to_str().unwrap(), "-m", "fast", "-w", "stop"]);
        self.running = false;
        log(format!("postgres {} stopped", self.name));
    }

    /// Crash (`-m immediate`): like pulling the plug — no checkpoint, clients cut off.
    pub fn crash(&mut self) {
        run(&self.bin, "pg_ctl", &["-D", self.data.to_str().unwrap(), "-m", "immediate", "-w", "stop"]);
        self.running = false;
        log(format!("postgres {} CRASHED", self.name));
    }

    pub fn promote(&mut self) {
        run(&self.bin, "pg_ctl", &["-D", self.data.to_str().unwrap(), "-w", "-t", "60", "promote"]);
        log(format!("postgres {} promoted to primary", self.name));
    }

    pub async fn execute(&self, db: &str, sql: &str) {
        let mut conn = PgConnection::connect(&self.url(db)).await.expect("connect to postgres");
        sqlx::raw_sql(sql).execute(&mut conn).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Requires every commit on this primary to be flushed by a standby before it is acknowledged.
    pub async fn require_synchronous_standby(&self) {
        self.execute("postgres", "ALTER SYSTEM SET synchronous_standby_names = '*'").await;
        self.execute("postgres", "SELECT pg_reload_conf()").await;
        // Wait until a standby is attached in sync mode.
        let mut conn = PgConnection::connect(&self.url("postgres")).await.unwrap();
        for _ in 0..100 {
            let n: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_replication WHERE sync_state = 'sync'")
                .fetch_one(&mut conn)
                .await
                .unwrap();
            if n > 0 {
                log(format!("postgres {}: synchronous standby attached", self.name));
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("no synchronous standby attached to {}", self.name);
    }
}

impl Drop for PgNode {
    fn drop(&mut self) {
        if self.running {
            let _ = pg_command(&self.bin, "pg_ctl")
                .args(["-D", self.data.to_str().unwrap(), "-m", "immediate", "stop"])
                .output();
        }
    }
}
