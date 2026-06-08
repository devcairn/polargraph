//! PolarGraph server binary (`polargraphd`).
//!
//! # Configuration
//!
//! All options can be set via CLI flags **or** environment variables.
//! CLI flags take priority over environment variables.
//!
//! | Flag                         | Env variable                    | Default         | Description                         |
//! |------------------------------|---------------------------------|-----------------|-------------------------------------|
//! | `--data-dir <PATH>`          | `POLARGRAPH_DATA_DIR`           | `/data`         | RocksDB data directory              |
//! | `--listen <ADDR>`            | `POLARGRAPH_LISTEN_ADDR`        | `0.0.0.0:50051` | gRPC listen address                 |
//! | `--backup-dir <PATH>`        | `POLARGRAPH_BACKUP_DIR`         | *(none)*        | Backup directory (opt.)             |
//! | `--replica-of <URL>`         | `POLARGRAPH_REPLICA_OF`         | *(none)*        | Primary gRPC address                |
//! | `--metrics-port <N>`         | `POLARGRAPH_METRICS_PORT`       | `9090`          | Prometheus /metrics HTTP port       |
//! | `--no-metrics`               | —                               | false           | Disable metrics endpoint            |
//! | `--log-level <LEVEL>`        | `RUST_LOG`                      | `info`          | Log level / filter directive        |
//! | `--log-format <FMT>`         | `LOG_FORMAT`                    | `pretty`        | Log format: `pretty` or `json`      |
//! | `--shutdown-timeout-ms <MS>` | `POLARGRAPH_SHUTDOWN_TIMEOUT_MS`| `10000`         | Max ms to drain in-flight RPCs      |
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
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
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

    /// Maximum time in milliseconds a single query may run.
    ///
    /// Applies to Query, VectorSeedQuery, and Reachable RPCs. When the limit is
    /// exceeded the RPC returns DEADLINE_EXCEEDED. Set to 0 to disable the
    /// timeout entirely.
    #[arg(
        long = "query-timeout-ms",
        env = "POLARGRAPH_QUERY_TIMEOUT_MS",
        default_value_t = 30_000u64,
        value_name = "MS"
    )]
    query_timeout_ms: u64,

    /// Maximum time in milliseconds to wait for in-flight requests to complete
    /// after a shutdown signal is received.  If the drain takes longer than
    /// this the process force-exits with a non-zero status.
    #[arg(
        long = "shutdown-timeout-ms",
        env = "POLARGRAPH_SHUTDOWN_TIMEOUT_MS",
        default_value_t = 10_000u64,
        value_name = "MS"
    )]
    shutdown_timeout_ms: u64,

    /// Emit a warning when any query RPC (Query, VectorSeedQuery, Reachable)
    /// takes longer than this many milliseconds.  Also increments the
    /// `polargraph_slow_queries_total{method}` Prometheus counter.
    /// Set to 0 to disable slow-query logging entirely.
    #[arg(
        long = "slow-query-ms",
        env = "POLARGRAPH_SLOW_QUERY_MS",
        default_value_t = 1_000u64,
        value_name = "MS"
    )]
    slow_query_ms: u64,
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
        shutdown_timeout_ms = cli.shutdown_timeout_ms,
        "polargraphd starting"
    );

    // ── Shutdown coordination ─────────────────────────────────────────────────
    let token = CancellationToken::new();
    let shutdown_timeout = Duration::from_millis(cli.shutdown_timeout_ms);

    // Watchdog: if the drain takes longer than the timeout, force-exit.
    let watchdog_token = token.clone();
    tokio::spawn(async move {
        watchdog_token.cancelled().await;
        tokio::time::sleep(shutdown_timeout).await;
        tracing::error!(
            timeout_ms = shutdown_timeout.as_millis() as u64,
            "shutdown timeout exceeded, forcing exit"
        );
        std::process::exit(1);
    });

    // OS signal handler: log and trip the token.
    let signal_token = token.clone();
    let timeout_secs = cli.shutdown_timeout_ms / 1000;
    tokio::spawn(async move {
        let signal_name = wait_for_signal().await;
        info!(signal = signal_name, "received shutdown signal");
        info!(max_wait_secs = timeout_secs, "draining in-flight requests");
        signal_token.cancel();
    });

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
    let metrics_join: Option<tokio::task::JoinHandle<()>> = if let Some(handle) = metrics_handle {
        let metrics_addr = SocketAddr::from(([0, 0, 0, 0], cli.metrics_port));
        let metrics_token = token.clone();
        let jh = tokio::spawn(async move {
            let app = Router::new().route(
                "/metrics",
                get(move || {
                    let h = handle.clone();
                    async move { h.render() }
                }),
            );
            axum::Server::bind(&metrics_addr)
                .serve(app.into_make_service())
                .with_graceful_shutdown(async move { metrics_token.cancelled().await })
                .await
                .expect("metrics HTTP server error");
        });
        info!(port = cli.metrics_port, "Prometheus /metrics endpoint listening");
        Some(jh)
    } else {
        None
    };

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

        // Background WAL replication task — cancelled by the shutdown token.
        let repl_store = store.clone();
        let repl_state = Arc::clone(&rs);
        let wal_token = token.clone();
        tokio::spawn(async move {
            wal_client::run_replication(repl_store, repl_state, wal_token).await;
        });

        server
            .with_query_timeout_ms(cli.query_timeout_ms)
            .with_slow_query_ms(cli.slow_query_ms)
    } else {
        PolarGraphServer::new_with_backup_dir(store, cli.backup_dir.as_deref())
            .context("failed to initialise PolarGraphServer")?
            .with_query_timeout_ms(cli.query_timeout_ms)
            .with_slow_query_ms(cli.slow_query_ms)
    };

    // ── Management UI HTTP server ─────────────────────────────────────────────
    let ui_join: Option<tokio::task::JoinHandle<()>> = if !cli.no_ui {
        let ui_state = Arc::new(ui_api::UiState {
            service: pg_server.clone(),
            api_keys: Arc::clone(&api_keys),
            start_time: std::time::Instant::now(),
            data_dir: cli.data_dir.display().to_string(),
            grpc_addr: cli.listen_addr.to_string(),
        });
        let ui_addr = SocketAddr::from(([0, 0, 0, 0], cli.ui_port));
        let ui_token = token.clone();
        let jh = tokio::spawn(async move {
            let app = ui_api::build_ui_router(ui_state);
            axum::Server::bind(&ui_addr)
                .serve(app.into_make_service())
                .with_graceful_shutdown(async move { ui_token.cancelled().await })
                .await
                .expect("UI HTTP server error");
        });
        info!(port = cli.ui_port, "management UI listening at http://0.0.0.0:{}", cli.ui_port);
        Some(jh)
    } else {
        None
    };

    let svc = PolarGraphServiceServer::new(pg_server);

    info!(addr = %cli.listen_addr, "listening");

    // ── gRPC serve loop (blocks until shutdown token fires + in-flight RPCs drain) ──
    let grpc_token = token.clone();
    Server::builder()
        .layer(auth_layer)
        .layer(telemetry::TelemetryLayer)
        .add_service(svc)
        .serve_with_shutdown(cli.listen_addr, async move { grpc_token.cancelled().await })
        .await
        .context("gRPC server error")?;

    info!("gRPC server stopped");

    // ── Wait for WAL replication to stop ─────────────────────────────────────
    // Token was already cancelled (that's what triggered the gRPC drain).
    // Give the WAL task a moment to exit its current stream iteration.
    if replica_address.is_some() {
        info!("WAL replication stopped");
    }

    // ── Wait for HTTP servers to stop ─────────────────────────────────────────
    let mut http_stopped = false;
    if let Some(jh) = metrics_join {
        let _ = tokio::time::timeout(Duration::from_millis(2_000), jh).await;
        http_stopped = true;
    }
    if let Some(jh) = ui_join {
        let _ = tokio::time::timeout(Duration::from_millis(2_000), jh).await;
        http_stopped = true;
    }
    if http_stopped {
        info!("HTTP servers stopped");
    }

    // ── RocksDB closes when `store` Arc drops at end of main ──────────────────
    info!("RocksDB closed");
    info!("shutdown complete");

    Ok(())
}

// ── Signal handling ───────────────────────────────────────────────────────────

/// Waits for SIGTERM or SIGINT and returns a short name for the signal.
async fn wait_for_signal() -> &'static str {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
        "SIGINT"
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
        "SIGTERM"
    };

    #[cfg(not(unix))]
    let sigterm = std::future::pending::<&'static str>();

    tokio::select! {
        name = ctrl_c  => name,
        name = sigterm => name,
    }
}
