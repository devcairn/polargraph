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
//! | `--backup-dir <PATH>` | `POLARGRAPH_BACKUP_DIR`   | *(none)*        | Backup directory (opt.)  |
//! | `--replica-of <URL>`  | `POLARGRAPH_REPLICA_OF`   | *(none)*        | Primary gRPC address     |
//!
//! # Examples
//!
//! ```bash
//! # Primary:
//! polargraphd --data-dir /var/lib/polargraph --listen 127.0.0.1:9090
//!
//! # Replica:
//! polargraphd --data-dir /var/lib/replica --listen 127.0.0.1:9091 \
//!   --replica-of http://primary-host:50051
//! ```

use anyhow::{Context, Result};
use clap::Parser;
use polargraph_core::schema::RetentionPolicy;
use polargraph_storage::TripleStore;
use polargraph_server::wal_client;
use proto::polar_graph_service_server::PolarGraphServiceServer;
use service::PolarGraphServer;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
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

    /// Directory for storing RocksDB backup files.
    ///
    /// Created automatically if it does not exist. When not set, the
    /// `CreateBackup`, `ListBackups`, and `PurgeOldBackups` RPCs return
    /// `FAILED_PRECONDITION`. Restore is an offline operation — see
    /// `docs/architecture.md` for the restore runbook.
    #[arg(
        long = "backup-dir",
        env = "POLARGRAPH_BACKUP_DIR",
        value_name = "PATH"
    )]
    backup_dir: Option<PathBuf>,

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

    /// Run retention on startup: delete triples whose transaction time is older
    /// than this many seconds. Requires the store to be opened but runs before
    /// accepting gRPC connections. Omit to skip startup retention.
    #[arg(
        long = "retention-tx-age-secs",
        env = "POLARGRAPH_RETENTION_TX_AGE_SECS",
        value_name = "SECS"
    )]
    retention_tx_age_secs: Option<u64>,

    /// When used with --retention-tx-age-secs, also delete triples whose
    /// vt_end is more than this many seconds in the past.
    #[arg(
        long = "retention-vt-lookback-secs",
        env = "POLARGRAPH_RETENTION_VT_LOOKBACK_SECS",
        value_name = "SECS"
    )]
    retention_vt_lookback_secs: Option<u64>,

    /// Open this instance as a streaming WAL replica of the primary at this
    /// gRPC address.
    ///
    /// The replica opens its own independent RocksDB at `--data-dir` and
    /// connects to the primary over gRPC to receive write batches in real time.
    /// All write RPCs on the replica return FAILED_PRECONDITION. The
    /// replication stream reconnects automatically on disconnect.
    ///
    /// Example: `http://primary-host:50051`
    #[arg(
        long = "replica-of",
        env = "POLARGRAPH_REPLICA_OF",
        value_name = "URL"
    )]
    replica_of: Option<String>,
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
        data_dir   = %cli.data_dir.display(),
        listen     = %cli.listen_addr,
        backup_dir = ?cli.backup_dir,
        replica_of = ?cli.replica_of,
        "polargraphd starting"
    );

    // ── Storage ───────────────────────────────────────────────────────────────
    std::fs::create_dir_all(&cli.data_dir)
        .with_context(|| format!("failed to create data dir: {}", cli.data_dir.display()))?;

    let (store, replica_address) = if let Some(primary_addr) = &cli.replica_of {
        let store = TripleStore::open_as_replica(&cli.data_dir, primary_addr.clone())
            .with_context(|| {
                format!(
                    "failed to open replica TripleStore at {} (primary={})",
                    cli.data_dir.display(),
                    primary_addr
                )
            })?;
        info!(primary = %primary_addr, "TripleStore opened as WAL replica");
        (store, Some(primary_addr.clone()))
    } else {
        let store = TripleStore::open(&cli.data_dir)
            .with_context(|| format!("failed to open TripleStore at {}", cli.data_dir.display()))?;
        info!("TripleStore ready");
        (store, None)
    };

    // ── Startup retention (primary only) ──────────────────────────────────────
    if replica_address.is_none() {
        if let Some(tx_age_secs) = cli.retention_tx_age_secs {
            let policy = RetentionPolicy {
                tx_age_secs,
                vt_lookback_secs: cli.retention_vt_lookback_secs,
            };
            info!(
                tx_age_secs,
                vt_lookback_secs = ?policy.vt_lookback_secs,
                "running startup retention"
            );
            let mgr = polargraph_storage::CompactionManager::new(store.clone());
            let stats = mgr
                .run_retention(&policy)
                .context("startup retention failed")?;
            info!(
                triples_scanned = stats.triples_scanned,
                triples_deleted = stats.triples_deleted,
                duration_ms = stats.duration_ms,
                "startup retention complete"
            );
        }
    }

    // ── gRPC server ───────────────────────────────────────────────────────────
    let svc = if let Some(primary_addr) = &replica_address {
        let (server, rs) = service::PolarGraphServer::new_replica(store.clone(), primary_addr)
            .context("failed to initialise replica PolarGraphServer")?;

        // Background WAL replication task.
        let repl_store = store.clone();
        let repl_state = Arc::clone(&rs);
        tokio::spawn(async move {
            wal_client::run_replication(repl_store, repl_state).await;
        });

        PolarGraphServiceServer::new(server)
    } else {
        PolarGraphServiceServer::new(
            PolarGraphServer::new_with_backup_dir(store, cli.backup_dir.as_deref())
                .context("failed to initialise PolarGraphServer")?,
        )
    };

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
