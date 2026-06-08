//! PolarGraph server binary (`polargraphd`).
//!
//! # Configuration
//!
//! All options can be set via CLI flags **or** environment variables.
//! CLI flags take priority over environment variables.
//!
//! | Flag                  | Env variable              | Default         | Description              |
//! |-----------------------|---------------------------|-----------------|--------------------------|
//! | `--data-dir <PATH>`   | `POLARGRAPH_DATA_DIR`     | `/data`         | RocksDB data directory   |
//! | `--listen <ADDR>`     | `POLARGRAPH_LISTEN_ADDR`  | `0.0.0.0:50051` | gRPC listen address      |
//!
//! # Examples
//!
//! ```bash
//! # Flags
//! polargraphd --data-dir /var/lib/polargraph --listen 127.0.0.1:9090
//!
//! # Environment variables (same effect)
//! POLARGRAPH_DATA_DIR=/var/lib/polargraph POLARGRAPH_LISTEN_ADDR=127.0.0.1:9090 polargraphd
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use polargraph_storage::TripleStore;
use proto::polar_graph_service_server::PolarGraphServiceServer;
use service::PolarGraphServer;
use std::{net::SocketAddr, path::PathBuf};
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

use polargraph_server::{proto, service};

// ── CLI ───────────────────────────────────────────────────────────────────────

/// PolarGraph graph database server.
#[derive(Debug, Parser)]
#[command(name = "polargraphd", version, about, long_about = None)]
struct Cli {
    /// Directory where RocksDB stores its data files.
    ///
    /// Created automatically if it does not exist.
    #[arg(
        long = "data-dir",
        env = "POLARGRAPH_DATA_DIR",
        default_value = "/data",
        value_name = "PATH"
    )]
    data_dir: PathBuf,

    /// Socket address the gRPC server will listen on.
    #[arg(
        long = "listen",
        env = "POLARGRAPH_LISTEN_ADDR",
        default_value = "0.0.0.0:50051",
        value_name = "ADDR"
    )]
    listen_addr: SocketAddr,

    /// Log filter directive (same syntax as `RUST_LOG`).
    ///
    /// Examples: `info`, `polargraph_server=debug`, `warn,polargraph_storage=trace`
    #[arg(
        long = "log",
        env = "RUST_LOG",
        default_value = "info",
        value_name = "FILTER"
    )]
    log_filter: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // ── Tracing ───────────────────────────────────────────────────────────────
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_new(&cli.log_filter)
                .with_context(|| format!("invalid log filter: {:?}", cli.log_filter))?,
        )
        .init();

    info!(
        data_dir  = %cli.data_dir.display(),
        listen    = %cli.listen_addr,
        "polargraphd starting"
    );

    // ── Storage ───────────────────────────────────────────────────────────────
    std::fs::create_dir_all(&cli.data_dir)
        .with_context(|| format!("failed to create data dir: {}", cli.data_dir.display()))?;

    let store = TripleStore::open(&cli.data_dir)
        .with_context(|| format!("failed to open TripleStore at {}", cli.data_dir.display()))?;

    info!("TripleStore ready");

    // ── gRPC server ───────────────────────────────────────────────────────────
    let svc = PolarGraphServiceServer::new(
        PolarGraphServer::new(store).context("failed to initialise PolarGraphServer")?,
    );

    info!(addr = %cli.listen_addr, "listening");

    Server::builder()
        .add_service(svc)
        .serve_with_shutdown(cli.listen_addr, shutdown_signal())
        .await
        .context("gRPC server error")?;

    info!("polargraphd stopped");
    Ok(())
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────

/// Resolves when SIGTERM or Ctrl-C is received.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c  => info!("received Ctrl-C, shutting down"),
        _ = sigterm => info!("received SIGTERM, shutting down"),
    }
}
