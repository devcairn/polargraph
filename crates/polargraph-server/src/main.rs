//! PolarGraph server binary (`polargraphd`).
//!
//! # Configuration
//!
//! All options can be set via CLI flags **or** environment variables.
//! CLI flags take priority over environment variables.
//!
//! | Flag                  | Env variable              | Default         | Description                      |
//! |-----------------------|---------------------------|-----------------|----------------------------------|
//! | `--data-dir <PATH>`   | `POLARGRAPH_DATA_DIR`     | `/data`         | RocksDB data directory           |
//! | `--listen <ADDR>`     | `POLARGRAPH_LISTEN_ADDR`  | `0.0.0.0:50051` | gRPC listen address              |
//! | `--backup-dir <PATH>` | `POLARGRAPH_BACKUP_DIR`   | *(none)*        | Backup directory (opt.)          |
//! | `--replica-of <URL>`  | `POLARGRAPH_REPLICA_OF`   | *(none)*        | Primary gRPC address             |
//! | `--metrics-port <N>`  | `POLARGRAPH_METRICS_PORT` | `9090`          | Prometheus /metrics HTTP port    |
//! | `--no-metrics`        | —                         | false           | Disable metrics endpoint         |
//! | `--log-level <LEVEL>` | `RUST_LOG`                | `info`          | Log level / filter directive     |
//! | `--log-format <FMT>`  | `LOG_FORMAT`              | `pretty`        | Log format: `pretty` or `json`   |
//!
//! # Examples
//!
//! ```bash
//! # Primary:
//! polargraphd --data-dir /var/lib/polargraph --listen 127.0.0.1:50051
//!
//! # Replica:
//! polargraphd --data-dir /var/lib/replica --listen 127.0.0.1:9091 \
//!   --replica-of http://primary-host:50051
//!
//! # Production with JSON logging and Prometheus:
//! LOG_FORMAT=json RUST_LOG=info polargraphd --data-dir /data
//! ```

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use polargraph_core::schema::RetentionPolicy;
use polargraph_storage::TripleStore;
use polargraph_server::wal_client;
use proto::polar_graph_service_server::PolarGraphServiceServer;
use service::PolarGraphServer;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use polargraph_server::{auth::ApiKeyLayer, proto, service, telemetry, ui_api};

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

    /// Log level / filter directive (same syntax as `RUST_LOG`).
    ///
    /// Examples: `info`, `polargraph_server=debug`, `warn,polargraph_storage=trace`
    #[arg(
        long = "log-level",
        env = "RUST_LOG",
        default_value = "info",
        value_name = "LEVEL"
    )]
    log_level: String,

    /// Log output format.
    ///
    /// `pretty` — human-readable output (default for development).
    /// `json`   — newline-delimited JSON (recommended for production/containers).
    #[arg(
        long = "log-format",
        env = "LOG_FORMAT",
        default_value = "pretty",
        value_name = "FORMAT"
    )]
    log_format: String,

    /// TCP port for the Prometheus `/metrics` HTTP endpoint.
    #[arg(
        long = "metrics-port",
        env = "POLARGRAPH_METRICS_PORT",
        default_value_t = 9090u16,
        value_name = "PORT"
    )]
    metrics_port: u16,

    /// Disable the Prometheus metrics endpoint entirely.
    #[arg(long = "no-metrics", default_value_t = false)]
    no_metrics: bool,

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

    /// API key required on all gRPC and HTTP management requests.
    ///
    /// Repeat to allow multiple keys (enables zero-downtime rotation).
    /// `POLARGRAPH_API_KEY` accepts a comma-separated list.
    #[arg(
        long = "api-key",
        env = "POLARGRAPH_API_KEY",
        value_name = "KEY",
        value_delimiter = ','
    )]
    api_keys: Vec<String>,

    /// Suppress the "no API key configured" warning at startup.
    #[arg(long = "no-auth", default_value_t = false)]
    no_auth: bool,

    /// TCP port for the web management UI.
    ///
    /// The UI is served separately from the gRPC port and the Prometheus
    /// metrics port. Disable with `--no-ui`.
    #[arg(
        long = "ui-port",
        env = "POLARGRAPH_UI_PORT",
        default_value_t = 8080u16,
        value_name = "PORT"
    )]
    ui_port: u16,

    /// Disable the web management UI entirely.
    #[arg(long = "no-ui", default_value_t = false)]
    no_ui: bool,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // ── Tracing ───────────────────────────────────────────────────────────────
    let filter = EnvFilter::try_new(&cli.log_level)
        .with_context(|| format!("invalid log filter: {:?}", cli.log_level))?;

    match cli.log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        _ => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .init();
        }
    }

    // ── Prometheus metrics ────────────────────────────────────────────────────
    let metrics_handle = if !cli.no_metrics {
        let handle = PrometheusBuilder::new()
            .install_recorder()
            .context("failed to install Prometheus metrics recorder")?;
        Some(handle)
    } else {
        None
    };

    if cli.api_keys.is_empty() && !cli.no_auth {
        warn!("no API key configured — all requests are unauthenticated; use --api-key or POLARGRAPH_API_KEY to enable auth");
    }

    info!(
        data_dir   = %cli.data_dir.display(),
        listen     = %cli.listen_addr,
        backup_dir = ?cli.backup_dir,
        replica_of = ?cli.replica_of,
        replica_mode = cli.replica_of.is_some(),
        metrics_enabled = !cli.no_metrics,
        ui_enabled = !cli.no_ui,
        ui_port = cli.ui_port,
        auth_enabled = !cli.api_keys.is_empty(),
        log_format = %cli.log_format,
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

    // ── Metrics HTTP server ───────────────────────────────────────────────────
    if let Some(handle) = metrics_handle {
        let metrics_addr = SocketAddr::from(([0, 0, 0, 0], cli.metrics_port));
        tokio::spawn(async move {
            let app = Router::new().route(
                "/metrics",
                get(move || {
                    let h = handle.clone();
                    async move { h.render() }
                }),
            );
            axum::Server::bind(&metrics_addr)
                .serve(app.into_make_service())
                .await
                .expect("metrics HTTP server error");
        });
        info!(port = cli.metrics_port, "Prometheus /metrics endpoint listening");
    }

    // ── gRPC server ───────────────────────────────────────────────────────────
    let api_keys = Arc::new(cli.api_keys.clone());
    let auth_layer = if cli.api_keys.is_empty() {
        ApiKeyLayer::disabled()
    } else {
        ApiKeyLayer::new(cli.api_keys.clone())
    };

    let pg_server = if let Some(primary_addr) = &replica_address {
        let (server, rs) = service::PolarGraphServer::new_replica(store.clone(), primary_addr)
            .context("failed to initialise replica PolarGraphServer")?;

        // Background WAL replication task.
        let repl_store = store.clone();
        let repl_state = Arc::clone(&rs);
        tokio::spawn(async move {
            wal_client::run_replication(repl_store, repl_state).await;
        });

        server
    } else {
        PolarGraphServer::new_with_backup_dir(store, cli.backup_dir.as_deref())
            .context("failed to initialise PolarGraphServer")?
    };

    // ── Management UI HTTP server ─────────────────────────────────────────────
    if !cli.no_ui {
        let ui_state = Arc::new(ui_api::UiState {
            service: pg_server.clone(),
            api_keys: Arc::clone(&api_keys),
            start_time: std::time::Instant::now(),
            data_dir: cli.data_dir.display().to_string(),
            grpc_addr: cli.listen_addr.to_string(),
        });
        let ui_addr = SocketAddr::from(([0, 0, 0, 0], cli.ui_port));
        tokio::spawn(async move {
            let app = ui_api::build_ui_router(ui_state);
            axum::Server::bind(&ui_addr)
                .serve(app.into_make_service())
                .await
                .expect("UI HTTP server error");
        });
        info!(port = cli.ui_port, "management UI listening at http://0.0.0.0:{}", cli.ui_port);
    }

    let svc = PolarGraphServiceServer::new(pg_server);

    info!(addr = %cli.listen_addr, "listening");

    Server::builder()
        .layer(auth_layer)
        .layer(telemetry::TelemetryLayer)
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
