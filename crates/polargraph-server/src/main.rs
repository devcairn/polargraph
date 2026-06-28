//! PolarGraph server binary (`polargraphd`).
//!
//! # Configuration
//!
//! Settings can be supplied via a TOML config file, environment variables, or
//! CLI flags.  Priority (highest wins):
//!
//!   **CLI flag > environment variable > config file > built-in default**
//!
//! | Flag                         | Env variable                    | Default         | Description                         |
//! |------------------------------|---------------------------------|-----------------|-------------------------------------|
//! | `--config <PATH>`            | `POLARGRAPH_CONFIG`             | *(auto-detect)* | TOML config file path               |
//! | `--data-dir <PATH>`          | `POLARGRAPH_DATA_DIR`           | `/data`         | RocksDB data directory              |
//! | `--listen <ADDR>`            | `POLARGRAPH_LISTEN_ADDR`        | `0.0.0.0:50051` | gRPC listen address                 |
//! | `--backup-dir <PATH>`        | `POLARGRAPH_BACKUP_DIR`         | *(none)*        | Backup directory (opt.)             |
//! | `--replica-of <URL>`         | `POLARGRAPH_REPLICA_OF`         | *(none)*        | Primary gRPC address                |
//! | `--metrics-port <N>`         | `POLARGRAPH_METRICS_PORT`       | `9090`          | Prometheus /metrics HTTP port       |
//! | `--no-metrics`               | —                               | false           | Disable metrics endpoint            |
//! | `--log-level <LEVEL>`        | `RUST_LOG`                      | `info`          | Log level / filter directive        |
//! | `--log-format <FMT>`         | `LOG_FORMAT`                    | `pretty`        | Log format: `pretty` or `json`      |
//! | `--shutdown-timeout-ms <MS>` | `POLARGRAPH_SHUTDOWN_TIMEOUT_MS`| `10000`         | Max ms to drain in-flight RPCs      |
//! | `--tls-cert <PATH>`          | `POLARGRAPH_TLS_CERT`           | *(none)*        | PEM certificate file (enables TLS)  |
//! | `--tls-key <PATH>`           | `POLARGRAPH_TLS_KEY`            | *(none)*        | PEM private key file (enables TLS)  |
//! | `--replica-tls-ca <PATH>`    | `POLARGRAPH_REPLICA_TLS_CA`     | *(none)*        | CA cert for verifying the primary   |
//!
//! # Examples
//!
//! ```bash
//! # Primary (plaintext):
//! polargraphd --data-dir /var/lib/polargraph --listen 127.0.0.1:50051
//!
//! # Primary (config file):
//! polargraphd --config /etc/polargraph/polargraph.toml
//!
//! # Primary (TLS):
//! polargraphd --data-dir /var/lib/polargraph \
//!   --tls-cert /etc/pg/server.crt --tls-key /etc/pg/server.key
//!
//! # Replica connecting to TLS primary:
//! polargraphd --data-dir /var/lib/replica --replica-of https://primary:50051 \
//!   --replica-tls-ca /etc/pg/ca.crt
//!
//! # Production with JSON logging and Prometheus:
//! LOG_FORMAT=json RUST_LOG=info polargraphd --data-dir /data
//! ```

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use clap::Parser;
use hyper::server::accept;
use metrics_exporter_prometheus::PrometheusBuilder;
use polargraph_core::schema::RetentionPolicy;
use polargraph_storage::{MigrationRunner, TripleStore};
use polargraph_server::config::{self, Config};
use polargraph_server::wal_client;
use proto::polar_graph_service_server::PolarGraphServiceServer;
use service::PolarGraphServer;
use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio_stream::{wrappers::TcpListenerStream, StreamExt as _};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use polargraph_server::{auth::ApiKeyLayer, proto, rate_limit::RateLimitLayer, retention_scheduler, service, telemetry, ui_api};

// ── CLI ───────────────────────────────────────────────────────────────────────

/// PolarGraph graph database server.
#[derive(Debug, Parser)]
#[command(name = "polargraphd", version, about, long_about = None)]
struct Cli {
    /// Path to a TOML configuration file.  When omitted the server tries
    /// `./polargraph.toml` then `~/.config/polargraph/config.toml`.
    #[arg(
        long = "config",
        env = "POLARGRAPH_CONFIG",
        value_name = "PATH"
    )]
    config: Option<PathBuf>,

    /// Directory where RocksDB stores its data files.
    #[arg(
        long = "data-dir",
        env = "POLARGRAPH_DATA_DIR",
        value_name = "PATH"
    )]
    data_dir: Option<PathBuf>,

    /// Socket address the gRPC server will listen on.
    #[arg(
        long = "listen",
        env = "POLARGRAPH_LISTEN_ADDR",
        value_name = "ADDR"
    )]
    listen_addr: Option<SocketAddr>,

    /// Directory for storing RocksDB backup files.
    #[arg(
        long = "backup-dir",
        env = "POLARGRAPH_BACKUP_DIR",
        value_name = "PATH"
    )]
    backup_dir: Option<PathBuf>,

    /// Log level / filter directive (same syntax as `RUST_LOG`).
    #[arg(
        long = "log-level",
        env = "RUST_LOG",
        value_name = "LEVEL"
    )]
    log_level: Option<String>,

    /// Log output format (`pretty` or `json`).
    #[arg(
        long = "log-format",
        env = "LOG_FORMAT",
        value_name = "FORMAT"
    )]
    log_format: Option<String>,

    /// TCP port for the Prometheus `/metrics` HTTP endpoint.
    #[arg(
        long = "metrics-port",
        env = "POLARGRAPH_METRICS_PORT",
        value_name = "PORT"
    )]
    metrics_port: Option<u16>,

    /// Disable the Prometheus metrics endpoint entirely.
    #[arg(long = "no-metrics", default_value_t = false)]
    no_metrics: bool,

    /// Run retention on startup: delete triples whose transaction time is older
    /// than this many seconds.
    #[arg(
        long = "retention-tx-age-secs",
        env = "POLARGRAPH_RETENTION_TX_AGE_SECS",
        value_name = "SECS"
    )]
    retention_tx_age_secs: Option<u64>,

    /// Companion to --retention-tx-age-secs; also deletes triples with old vt_end.
    #[arg(
        long = "retention-vt-lookback-secs",
        env = "POLARGRAPH_RETENTION_VT_LOOKBACK_SECS",
        value_name = "SECS"
    )]
    retention_vt_lookback_secs: Option<u64>,

    /// Enable the scheduled (periodic) retention background task.
    #[arg(
        long = "retention-schedule",
        env = "POLARGRAPH_RETENTION_SCHEDULE",
        default_value_t = false
    )]
    retention_schedule: bool,

    /// Interval in seconds between scheduled retention passes (default: 3600).
    #[arg(
        long = "retention-interval-secs",
        env = "POLARGRAPH_RETENTION_INTERVAL_SECS",
        value_name = "SECS"
    )]
    retention_interval_secs: Option<u64>,

    /// Open as a WAL replica of the primary at this gRPC address.
    #[arg(
        long = "replica-of",
        env = "POLARGRAPH_REPLICA_OF",
        value_name = "URL"
    )]
    replica_of: Option<String>,

    /// API key required on all gRPC and HTTP management requests.
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
    #[arg(
        long = "ui-port",
        env = "POLARGRAPH_UI_PORT",
        value_name = "PORT"
    )]
    ui_port: Option<u16>,

    /// Disable the web management UI entirely.
    #[arg(long = "no-ui", default_value_t = false)]
    no_ui: bool,

    /// Maximum time in milliseconds a single query may run (0 = disabled).
    #[arg(
        long = "query-timeout-ms",
        env = "POLARGRAPH_QUERY_TIMEOUT_MS",
        value_name = "MS"
    )]
    query_timeout_ms: Option<u64>,

    /// Maximum time in milliseconds to wait for in-flight requests after shutdown.
    #[arg(
        long = "shutdown-timeout-ms",
        env = "POLARGRAPH_SHUTDOWN_TIMEOUT_MS",
        value_name = "MS"
    )]
    shutdown_timeout_ms: Option<u64>,

    /// Emit a warning when any query RPC takes longer than this many ms (0 = disabled).
    #[arg(
        long = "slow-query-ms",
        env = "POLARGRAPH_SLOW_QUERY_MS",
        value_name = "MS"
    )]
    slow_query_ms: Option<u64>,

    /// Maximum requests per second allowed per client IP (0 = disabled).
    #[arg(
        long = "rate-limit-rps",
        env = "POLARGRAPH_RATE_LIMIT_RPS",
        value_name = "N"
    )]
    rate_limit_rps: Option<u32>,

    /// Idle timeout in milliseconds for open wire transactions (0 = disabled). Default: 300000.
    #[arg(
        long = "tx-idle-timeout-ms",
        env = "POLARGRAPH_TX_IDLE_TIMEOUT_MS",
        value_name = "MS"
    )]
    tx_idle_timeout_ms: Option<u64>,

    /// Default HNSW exploration factor for vector searches (higher = better recall, slower).
    /// Per-request `ef` fields override this value when non-zero. Default: 50.
    #[arg(
        long = "default-vector-ef",
        env = "POLARGRAPH_DEFAULT_VECTOR_EF",
        value_name = "N"
    )]
    default_vector_ef: Option<u32>,

    /// Maximum number of compiled Cypher query plans to cache. 0 disables caching.
    /// Default: 1000.
    #[arg(
        long = "query-cache-size",
        env = "POLARGRAPH_QUERY_CACHE_SIZE",
        value_name = "N"
    )]
    query_cache_size: Option<usize>,

    /// Path to PEM certificate file. When combined with --tls-key, enables TLS
    /// on the gRPC server, management UI, and metrics endpoint.
    #[arg(
        long = "tls-cert",
        env = "POLARGRAPH_TLS_CERT",
        value_name = "PATH"
    )]
    tls_cert: Option<PathBuf>,

    /// Path to PEM private key file. Must be supplied together with --tls-cert.
    #[arg(
        long = "tls-key",
        env = "POLARGRAPH_TLS_KEY",
        value_name = "PATH"
    )]
    tls_key: Option<PathBuf>,

    /// CA certificate (PEM) used to verify the primary's TLS certificate when
    /// running in replica mode. If not set, the replica connects without TLS.
    #[arg(
        long = "replica-tls-ca",
        env = "POLARGRAPH_REPLICA_TLS_CA",
        value_name = "PATH"
    )]
    replica_tls_ca: Option<PathBuf>,

    /// Run OWL 2 RL forward-chaining materialization at startup (primary only).
    /// Derives RDFS/OWL entailments and writes them to the DRV column family.
    #[arg(
        long = "auto-materialize",
        env = "POLARGRAPH_AUTO_MATERIALIZE",
        default_value_t = false
    )]
    auto_materialize: bool,
}

// ── Merge helpers ─────────────────────────────────────────────────────────────

/// Resolve a setting using the priority chain: CLI > env (already baked into
/// `cli_val` by clap) > config file > built-in default.
#[inline]
fn resolve<T>(cli_val: Option<T>, config_val: Option<T>, default: T) -> T {
    cli_val.or(config_val).unwrap_or(default)
}

/// Merge two optional `PathBuf`s, converting the config string to a path.
#[inline]
fn resolve_path(
    cli_val: Option<PathBuf>,
    config_val: Option<String>,
    default: &str,
) -> PathBuf {
    cli_val
        .or_else(|| config_val.map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(default))
}

/// Merge an optional `PathBuf` with no built-in default.
#[inline]
fn resolve_path_opt(cli_val: Option<PathBuf>, config_val: Option<String>) -> Option<PathBuf> {
    cli_val.or_else(|| config_val.map(PathBuf::from))
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // ── Config file ───────────────────────────────────────────────────────────
    let cfg: Config = config::load_config(cli.config.as_deref())
        .context("failed to load configuration file")?;

    // ── Merge: CLI/env > config file > built-in defaults ─────────────────────
    let data_dir = resolve_path(cli.data_dir, cfg.server.data_dir, "/data");
    let listen_addr: SocketAddr = cli.listen_addr.unwrap_or_else(|| {
        cfg.server
            .grpc_port
            .map(|p| SocketAddr::from(([0, 0, 0, 0], p)))
            .unwrap_or_else(|| "0.0.0.0:50051".parse().unwrap())
    });
    let backup_dir = resolve_path_opt(cli.backup_dir, cfg.storage.backup_dir);
    let log_level = resolve(cli.log_level, cfg.observability.log_level, "info".to_string());
    let log_format = resolve(cli.log_format, cfg.observability.log_format, "pretty".to_string());
    let metrics_port = resolve(cli.metrics_port, cfg.server.metrics_port, 9090u16);
    let no_metrics = cli.no_metrics || cfg.observability.no_metrics.unwrap_or(false);
    let retention_tx_age_secs = cli.retention_tx_age_secs.or(cfg.storage.retention_tx_age_secs);
    let retention_vt_lookback_secs = cli
        .retention_vt_lookback_secs
        .or(cfg.storage.retention_vt_lookback_secs);
    let retention_schedule_enabled = cli.retention_schedule
        || cfg.storage.retention_schedule.enabled.unwrap_or(false);
    let retention_interval_secs = cli
        .retention_interval_secs
        .or(cfg.storage.retention_schedule.interval_secs)
        .unwrap_or(3600);
    let replica_of = cli.replica_of.or(cfg.replication.replica_of);
    let api_keys: Vec<String> = if !cli.api_keys.is_empty() {
        cli.api_keys
    } else {
        cfg.auth.api_keys.unwrap_or_default()
    };
    let no_auth = cli.no_auth || cfg.auth.no_auth.unwrap_or(false);
    let ui_port = resolve(cli.ui_port, cfg.server.ui_port, 8080u16);
    let no_ui = cli.no_ui || cfg.observability.no_ui.unwrap_or(false);
    let query_timeout_ms = resolve(cli.query_timeout_ms, cfg.query.timeout_ms, 30_000u64);
    let shutdown_timeout_ms = resolve(cli.shutdown_timeout_ms, cfg.server.shutdown_timeout_ms, 10_000u64);
    let slow_query_ms = resolve(cli.slow_query_ms, cfg.query.slow_query_ms, 1_000u64);
    let tls_cert = resolve_path_opt(cli.tls_cert, cfg.tls.cert);
    let tls_key = resolve_path_opt(cli.tls_key, cfg.tls.key);
    let replica_tls_ca = resolve_path_opt(cli.replica_tls_ca, cfg.replication.tls_ca);
    let rate_limit_rps = resolve(cli.rate_limit_rps, cfg.rate_limit.max_rps, 0u32);
    let default_vector_ef = resolve(cli.default_vector_ef, cfg.query.default_vector_ef, 50u32);
    let tx_idle_timeout_ms = resolve(cli.tx_idle_timeout_ms, None::<u64>, 300_000u64);
    let query_cache_size = resolve(cli.query_cache_size, cfg.query.cache_size, 1000usize);
    let auto_materialize = cli.auto_materialize || cfg.storage.auto_materialize.unwrap_or(false);

    // ── Tracing ───────────────────────────────────────────────────────────────
    let filter = EnvFilter::try_new(&log_level)
        .with_context(|| format!("invalid log filter: {:?}", log_level))?;

    match log_format.as_str() {
        "json" => {
            tracing_subscriber::fmt().json().with_env_filter(filter).init();
        }
        _ => {
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
    }

    // ── Prometheus metrics ────────────────────────────────────────────────────
    let metrics_handle = if !no_metrics {
        let handle = PrometheusBuilder::new()
            .install_recorder()
            .context("failed to install Prometheus metrics recorder")?;
        Some(handle)
    } else {
        None
    };

    if api_keys.is_empty() && !no_auth {
        warn!("no API key configured — all requests are unauthenticated; use --api-key or POLARGRAPH_API_KEY to enable auth");
    }

    // ── TLS setup ─────────────────────────────────────────────────────────────
    let tls_pem: Option<(Vec<u8>, Vec<u8>)> = match (&tls_cert, &tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let cert = tokio::fs::read(cert_path)
                .await
                .with_context(|| format!("failed to read TLS cert: {}", cert_path.display()))?;
            let key = tokio::fs::read(key_path)
                .await
                .with_context(|| format!("failed to read TLS key: {}", key_path.display()))?;
            info!(cert = %cert_path.display(), "TLS enabled");
            Some((cert, key))
        }
        (None, None) => None,
        _ => anyhow::bail!("--tls-cert and --tls-key must be supplied together"),
    };

    // CA cert for replica→primary TLS.
    let replica_tls_ca_bytes: Option<Vec<u8>> = if let Some(ca_path) = &replica_tls_ca {
        let ca = tokio::fs::read(ca_path)
            .await
            .with_context(|| format!("failed to read replica TLS CA: {}", ca_path.display()))?;
        Some(ca)
    } else {
        None
    };

    info!(
        data_dir   = %data_dir.display(),
        listen     = %listen_addr,
        backup_dir = ?backup_dir,
        replica_of = ?replica_of,
        replica_mode = replica_of.is_some(),
        metrics_enabled = !no_metrics,
        ui_enabled = !no_ui,
        ui_port = ui_port,
        auth_enabled = !api_keys.is_empty(),
        tls_enabled = tls_pem.is_some(),
        log_format = %log_format,
        shutdown_timeout_ms = shutdown_timeout_ms,
        rate_limit_rps = rate_limit_rps,
        "polargraphd starting"
    );

    // ── Shutdown coordination ─────────────────────────────────────────────────
    let token = CancellationToken::new();
    let shutdown_timeout = Duration::from_millis(shutdown_timeout_ms);

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

    let signal_token = token.clone();
    let timeout_secs = shutdown_timeout_ms / 1000;
    tokio::spawn(async move {
        let signal_name = wait_for_signal().await;
        info!(signal = signal_name, "received shutdown signal");
        info!(max_wait_secs = timeout_secs, "draining in-flight requests");
        signal_token.cancel();
    });

    // ── Storage ───────────────────────────────────────────────────────────────
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create data dir: {}", data_dir.display()))?;

    let (store, replica_address) = if let Some(primary_addr) = &replica_of {
        let store = TripleStore::open_as_replica(&data_dir, primary_addr.clone())
            .with_context(|| {
                format!(
                    "failed to open replica TripleStore at {} (primary={})",
                    data_dir.display(),
                    primary_addr
                )
            })?;
        info!(primary = %primary_addr, "TripleStore opened as WAL replica");
        (store, Some(primary_addr.clone()))
    } else {
        let store = TripleStore::open(&data_dir)
            .with_context(|| format!("failed to open TripleStore at {}", data_dir.display()))?;
        info!("TripleStore ready");
        (store, None)
    };

    // ── Schema migrations (primary only) ─────────────────────────────────────
    if replica_address.is_none() {
        let runner = MigrationRunner::new(store.clone());
        let stats = runner
            .run_pending()
            .context("schema migration failed")?;
        if stats.applied.is_empty() {
            info!("schema migrations: database is up to date");
        } else {
            info!(applied = ?stats.applied, "schema migrations applied");
        }
    }

    // ── Startup OWL 2 RL materialization (primary only) ──────────────────────
    if replica_address.is_none() && auto_materialize {
        info!("running OWL 2 RL materialization at startup");
        let mat_stats = polargraph_storage::owl_rl::materialize(&store, true)
            .context("startup OWL 2 RL materialization failed")?;
        info!(
            rules_fired = mat_stats.rules_fired,
            derived_triples = mat_stats.derived_triples,
            iterations = mat_stats.iterations,
            "startup OWL 2 RL materialization complete"
        );
    }

    // ── Startup retention (primary only) ──────────────────────────────────────
    if replica_address.is_none() {
        if let Some(tx_age_secs) = retention_tx_age_secs {
            let policy = RetentionPolicy {
                tx_age_secs,
                vt_lookback_secs: retention_vt_lookback_secs,
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

    // ── Scheduled retention (primary only) ────────────────────────────────────
    if replica_address.is_none() && retention_schedule_enabled {
        if let Some(tx_age_secs) = retention_tx_age_secs {
            let policy = RetentionPolicy {
                tx_age_secs,
                vt_lookback_secs: retention_vt_lookback_secs,
            };
            let interval = Duration::from_secs(retention_interval_secs);
            let sched_store = store.clone();
            let sched_token = token.clone();
            info!(
                interval_secs = retention_interval_secs,
                "spawning scheduled retention task"
            );
            tokio::spawn(async move {
                retention_scheduler::run_retention_scheduler(
                    sched_store,
                    policy,
                    interval,
                    sched_token,
                )
                .await;
            });
        } else {
            warn!("--retention-schedule enabled but no --retention-tx-age-secs set; scheduler not started");
        }
    }

    // ── gRPC health check ─────────────────────────────────────────────────────
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_service_status("polargraph.v1.PolarGraphService", ServingStatus::Serving)
        .await;

    // ── Metrics HTTP server ───────────────────────────────────────────────────
    let metrics_join: Option<tokio::task::JoinHandle<()>> = if let Some(handle) = metrics_handle {
        let metrics_addr = SocketAddr::from(([0, 0, 0, 0], metrics_port));
        let metrics_token = token.clone();
        let tls = tls_pem.clone();
        let jh = tokio::spawn(async move {
            let app = Router::new().route(
                "/metrics",
                get(move || {
                    let h = handle.clone();
                    async move { h.render() }
                }),
            );
            if let Some((cert, key)) = tls {
                serve_axum_tls(app, metrics_addr, cert, key, metrics_token)
                    .await
                    .expect("metrics HTTPS server error");
            } else {
                axum::Server::bind(&metrics_addr)
                    .serve(app.into_make_service())
                    .with_graceful_shutdown(async move { metrics_token.cancelled().await })
                    .await
                    .expect("metrics HTTP server error");
            }
        });
        info!(port = metrics_port, "Prometheus /metrics endpoint listening");
        Some(jh)
    } else {
        None
    };

    // ── gRPC server setup ─────────────────────────────────────────────────────
    let (auth_layer, key_store) = ApiKeyLayer::new(api_keys);
    let rate_layer = if rate_limit_rps > 0 {
        info!(max_rps = rate_limit_rps, "per-client rate limiting enabled");
        RateLimitLayer::new(rate_limit_rps)
    } else {
        RateLimitLayer::disabled()
    };

    let (pg_server, replica_state_for_ui) = if let Some(primary_addr) = &replica_address {
        let (server, rs) = service::PolarGraphServer::new_replica(store.clone(), primary_addr)
            .context("failed to initialise replica PolarGraphServer")?;

        // Background WAL replication task.
        let repl_store = store.clone();
        let repl_state = Arc::clone(&rs);
        let wal_token = token.clone();
        let wal_health = health_reporter.clone();
        let wal_ca = replica_tls_ca_bytes.clone();
        tokio::spawn(async move {
            wal_client::run_replication(repl_store, repl_state, wal_token, Some(wal_health), wal_ca).await;
        });

        let server = server
            .with_query_timeout_ms(query_timeout_ms)
            .with_slow_query_ms(slow_query_ms)
            .with_default_vector_ef(default_vector_ef)
            .with_tx_idle_timeout_ms(tx_idle_timeout_ms)
            .with_query_cache_size(query_cache_size)
            .with_key_store(key_store.clone());

        (server, Some(rs))
    } else {
        let server = PolarGraphServer::new_with_backup_dir(store, backup_dir.as_deref())
            .context("failed to initialise PolarGraphServer")?
            .with_query_timeout_ms(query_timeout_ms)
            .with_slow_query_ms(slow_query_ms)
            .with_default_vector_ef(default_vector_ef)
            .with_tx_idle_timeout_ms(tx_idle_timeout_ms)
            .with_query_cache_size(query_cache_size)
            .with_key_store(key_store.clone());
        (server, None)
    };

    // ── Wire transaction TTL task ─────────────────────────────────────────────
    pg_server.spawn_tx_ttl_task(token.clone(), tx_idle_timeout_ms);

    // ── Management UI HTTP server ─────────────────────────────────────────────
    let ui_join: Option<tokio::task::JoinHandle<()>> = if !no_ui {
        let ui_state = Arc::new(ui_api::UiState {
            service: pg_server.clone(),
            api_keys: key_store.clone(),
            start_time: std::time::Instant::now(),
            data_dir: data_dir.display().to_string(),
            grpc_addr: listen_addr.to_string(),
            replica_state: replica_state_for_ui.clone(),
        });
        let ui_addr = SocketAddr::from(([0, 0, 0, 0], ui_port));
        let ui_token = token.clone();
        let tls = tls_pem.clone();
        let jh = tokio::spawn(async move {
            let app = ui_api::build_ui_router(ui_state);
            if let Some((cert, key)) = tls {
                serve_axum_tls(app, ui_addr, cert, key, ui_token)
                    .await
                    .expect("UI HTTPS server error");
            } else {
                axum::Server::bind(&ui_addr)
                    .serve(app.into_make_service())
                    .with_graceful_shutdown(async move { ui_token.cancelled().await })
                    .await
                    .expect("UI HTTP server error");
            }
        });
        info!(port = ui_port, "management UI listening at http://0.0.0.0:{}", ui_port);
        Some(jh)
    } else {
        None
    };

    let svc = PolarGraphServiceServer::new(pg_server);

    // ── gRPC serve loop ───────────────────────────────────────────────────────
    info!(addr = %listen_addr, "listening");
    let grpc_token = token.clone();

    let mut builder = Server::builder()
        .layer(rate_layer)
        .layer(auth_layer)
        .layer(telemetry::TelemetryLayer);

    if let Some((cert_pem, key_pem)) = &tls_pem {
        let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
        let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
        builder
            .tls_config(tls_config)
            .context("invalid TLS configuration")?
            .add_service(health_service)
            .add_service(svc)
            .serve_with_shutdown(listen_addr, async move { grpc_token.cancelled().await })
            .await
            .context("gRPC server error")?;
    } else {
        builder
            .add_service(health_service)
            .add_service(svc)
            .serve_with_shutdown(listen_addr, async move { grpc_token.cancelled().await })
            .await
            .context("gRPC server error")?;
    }

    info!("gRPC server stopped");

    if replica_address.is_some() {
        info!("WAL replication stopped");
    }

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

    info!("RocksDB closed");
    info!("shutdown complete");

    Ok(())
}

// ── HTTPS helper for axum ─────────────────────────────────────────────────────

/// Serve an axum router over TLS using tokio-rustls.
///
/// Accepts connections on `addr`, wraps each TCP stream with TLS, and serves
/// using hyper's low-level `accept::from_stream` bridge.  Shuts down cleanly
/// when `token` is cancelled.
async fn serve_axum_tls(
    app: Router,
    addr: SocketAddr,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    token: CancellationToken,
) -> anyhow::Result<()> {
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::TlsAcceptor;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &*cert_pem)
        .map(|r| r.map(|c| c.into_owned()))
        .collect::<std::result::Result<_, _>>()
        .context("failed to parse TLS certificate")?;

    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut &*key_pem)
        .context("failed to read TLS private key")?
        .ok_or_else(|| anyhow::anyhow!("no private key found in PEM file"))?
        .clone_key();

    let tls_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("failed to build TLS server config")?;

    let acceptor = TlsAcceptor::from(Arc::new(tls_config));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind HTTPS on {addr}"))?;

    let tls_stream = TcpListenerStream::new(listener).then(move |res| {
        let acceptor = acceptor.clone();
        async move {
            let tcp = res.map_err(std::io::Error::other)?;
            acceptor
                .accept(tcp)
                .await
                .map_err(std::io::Error::other)
        }
    });

    axum::Server::builder(accept::from_stream(tls_stream))
        .serve(app.into_make_service())
        .with_graceful_shutdown(async move { token.cancelled().await })
        .await
        .context("HTTPS server error")
}

// ── Signal handling ───────────────────────────────────────────────────────────

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
