use clap::Parser;
use qd::{Command, Config, Role};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    init_tracing(&config.log_format);

    let role = match config.command.clone().unwrap_or(Command::All) {
        Command::Migrate => return qd::migrate(&config).await,
        Command::WaitForSchema { timeout_secs } => {
            return qd::wait_for_schema_command(&config, timeout_secs).await;
        }
        Command::All => Role::All,
        Command::Server => Role::Server,
        Command::Scheduler => Role::Scheduler,
    };

    // A signal while still starting (e.g. waiting for migrations) exits immediately.
    let server = tokio::select! {
        s = qd::start_role(config, role) => s?,
        _ = wait_for_signal() => {
            tracing::info!("shutdown signal received during startup");
            return Ok(());
        }
    };
    let token = server.shutdown_token();
    let engine = server.engine.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        tracing::info!("shutdown signal received; draining");
        token.cancel();
        engine.shutdown();
    });
    server.wait().await;
    tracing::info!("bye");
    Ok(())
}

fn init_tracing(format: &str) {
    let filter = EnvFilter::try_from_env("QD_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if format.eq_ignore_ascii_case("json") {
        builder.json().flatten_event(true).init();
    } else {
        builder.init();
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
