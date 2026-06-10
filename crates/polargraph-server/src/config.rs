//! TOML configuration file support.
//!
//! Priority order (highest to lowest):
//!   CLI flag  >  environment variable  >  config file  >  built-in default
//!
//! The config file is located via `--config PATH` / `POLARGRAPH_CONFIG`, or by
//! searching `./polargraph.toml` then `~/.config/polargraph/config.toml`.  If
//! no file is found the returned `Config` is all-`None` and built-in defaults
//! apply unchanged.

use serde::Deserialize;
use std::path::{Path, PathBuf};
use thiserror::Error;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
}

// ── Config structs ────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub query: QueryConfig,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

#[derive(Debug, Default, Deserialize)]
pub struct ServerConfig {
    /// RocksDB data directory.  Equivalent to `--data-dir` / `POLARGRAPH_DATA_DIR`.
    pub data_dir: Option<String>,
    /// gRPC listen port (the address is always `0.0.0.0`).  Equivalent to the
    /// port portion of `--listen` / `POLARGRAPH_LISTEN_ADDR`.
    pub grpc_port: Option<u16>,
    /// Management UI HTTP port.  Equivalent to `--ui-port` / `POLARGRAPH_UI_PORT`.
    pub ui_port: Option<u16>,
    /// Prometheus metrics HTTP port.  Equivalent to `--metrics-port` / `POLARGRAPH_METRICS_PORT`.
    pub metrics_port: Option<u16>,
    /// Milliseconds to wait for in-flight RPCs to drain on shutdown.
    /// Equivalent to `--shutdown-timeout-ms` / `POLARGRAPH_SHUTDOWN_TIMEOUT_MS`.
    pub shutdown_timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct StorageConfig {
    /// Directory for incremental RocksDB backups.
    /// Equivalent to `--backup-dir` / `POLARGRAPH_BACKUP_DIR`.
    pub backup_dir: Option<String>,
    /// Delete triples whose transaction time is older than this many seconds
    /// (run at startup).  Equivalent to `--retention-tx-age-secs` /
    /// `POLARGRAPH_RETENTION_TX_AGE_SECS`.
    pub retention_tx_age_secs: Option<u64>,
    /// Companion retention window for valid-time end.  Equivalent to
    /// `--retention-vt-lookback-secs` / `POLARGRAPH_RETENTION_VT_LOOKBACK_SECS`.
    pub retention_vt_lookback_secs: Option<u64>,
    /// Scheduled (periodic) retention configuration.
    #[serde(default)]
    pub retention_schedule: RetentionScheduleConfig,
}

/// Configuration for the periodic scheduled retention task.
///
/// When `enabled` is true, a background task fires every `interval_secs`
/// and runs a full retention pass using the same policy as startup retention.
#[derive(Debug, Default, Deserialize, Clone)]
pub struct RetentionScheduleConfig {
    /// Enable the scheduled retention task.
    /// Equivalent to `--retention-schedule` / `POLARGRAPH_RETENTION_SCHEDULE`.
    pub enabled: Option<bool>,
    /// How often (in seconds) to run the retention pass.  Defaults to 3600.
    /// Equivalent to `--retention-interval-secs` / `POLARGRAPH_RETENTION_INTERVAL_SECS`.
    pub interval_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ReplicationConfig {
    /// gRPC address of the primary to replicate from (enables replica mode).
    /// Equivalent to `--replica-of` / `POLARGRAPH_REPLICA_OF`.
    pub replica_of: Option<String>,
    /// CA certificate path for verifying the primary's TLS certificate.
    /// Equivalent to `--replica-tls-ca` / `POLARGRAPH_REPLICA_TLS_CA`.
    pub tls_ca: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct TlsConfig {
    /// Path to PEM certificate file.  Enables TLS when combined with `key`.
    /// Equivalent to `--tls-cert` / `POLARGRAPH_TLS_CERT`.
    pub cert: Option<String>,
    /// Path to PEM private key file.  Must be paired with `cert`.
    /// Equivalent to `--tls-key` / `POLARGRAPH_TLS_KEY`.
    pub key: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct AuthConfig {
    /// API keys accepted on all gRPC and HTTP management requests.
    /// Equivalent to `--api-key` / `POLARGRAPH_API_KEY`.
    pub api_keys: Option<Vec<String>>,
    /// Suppress the "no API key configured" startup warning.
    /// Equivalent to `--no-auth`.
    pub no_auth: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ObservabilityConfig {
    /// Log level / filter directive (`info`, `debug`, `warn`, etc.).
    /// Equivalent to `--log-level` / `RUST_LOG`.
    pub log_level: Option<String>,
    /// Log output format: `"pretty"` or `"json"`.
    /// Equivalent to `--log-format` / `LOG_FORMAT`.
    pub log_format: Option<String>,
    /// Disable the Prometheus `/metrics` HTTP endpoint.
    /// Equivalent to `--no-metrics`.
    pub no_metrics: Option<bool>,
    /// Disable the web management UI.
    /// Equivalent to `--no-ui`.
    pub no_ui: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryConfig {
    /// Maximum query execution time in milliseconds (0 = disabled).
    /// Equivalent to `--query-timeout-ms` / `POLARGRAPH_QUERY_TIMEOUT_MS`.
    pub timeout_ms: Option<u64>,
    /// Log a warning when any query RPC exceeds this many ms (0 = disabled).
    /// Equivalent to `--slow-query-ms` / `POLARGRAPH_SLOW_QUERY_MS`.
    pub slow_query_ms: Option<u64>,
    /// Default ef for HNSW vector searches (higher = better recall, slower).
    /// Equivalent to `--default-vector-ef` / `POLARGRAPH_DEFAULT_VECTOR_EF`.
    pub default_vector_ef: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum requests per second allowed per client IP address (0 = disabled).
    /// Equivalent to `--rate-limit-rps` / `POLARGRAPH_RATE_LIMIT_RPS`.
    pub max_rps: Option<u32>,
}

// ── Loading ───────────────────────────────────────────────────────────────────

/// Load the server configuration from a TOML file.
///
/// - If `path` is `Some`, that file is read; an error is returned if it does
///   not exist or cannot be parsed.
/// - If `path` is `None`, the following locations are tried in order:
///   1. `./polargraph.toml`
///   2. `~/.config/polargraph/config.toml`
///   If neither exists, `Config::default()` is returned silently.
pub fn load_config(path: Option<&Path>) -> Result<Config, ConfigError> {
    if let Some(p) = path {
        return read_and_parse(p);
    }

    for candidate in default_config_paths() {
        if candidate.exists() {
            return read_and_parse(&candidate);
        }
    }

    Ok(Config::default())
}

fn read_and_parse(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Read {
        path: path.display().to_string(),
        source: e,
    })?;
    toml::from_str(&text).map_err(|e| ConfigError::Parse {
        path: path.display().to_string(),
        source: e,
    })
}

fn default_config_paths() -> Vec<PathBuf> {
    let mut paths = vec![PathBuf::from("polargraph.toml")];
    let home_var = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
    if let Ok(home) = std::env::var(home_var) {
        paths.push(PathBuf::from(home).join(".config/polargraph/config.toml"));
    }
    paths
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_toml_values() {
        let mut f = NamedTempFile::new().unwrap();
        write!(
            f,
            r#"
[server]
data_dir = "/var/lib/polargraph"
grpc_port = 50052
ui_port = 8081
metrics_port = 9091
shutdown_timeout_ms = 15000

[storage]
backup_dir = "/var/lib/polargraph/backups"
retention_tx_age_secs = 604800
retention_vt_lookback_secs = 86400

[replication]
replica_of = "http://primary:50051"
tls_ca = "/etc/pg/ca.crt"

[tls]
cert = "/etc/pg/server.crt"
key  = "/etc/pg/server.key"

[auth]
api_keys = ["key-alpha", "key-beta"]
no_auth  = false

[observability]
log_level  = "debug"
log_format = "json"
no_metrics = false
no_ui      = false

[query]
timeout_ms       = 5000
slow_query_ms    = 250
default_vector_ef = 100
"#
        )
        .unwrap();

        let cfg = load_config(Some(f.path())).unwrap();

        assert_eq!(cfg.server.data_dir.as_deref(), Some("/var/lib/polargraph"));
        assert_eq!(cfg.server.grpc_port, Some(50052));
        assert_eq!(cfg.server.ui_port, Some(8081));
        assert_eq!(cfg.server.metrics_port, Some(9091));
        assert_eq!(cfg.server.shutdown_timeout_ms, Some(15_000));

        assert_eq!(
            cfg.storage.backup_dir.as_deref(),
            Some("/var/lib/polargraph/backups")
        );
        assert_eq!(cfg.storage.retention_tx_age_secs, Some(604_800));
        assert_eq!(cfg.storage.retention_vt_lookback_secs, Some(86_400));

        assert_eq!(
            cfg.replication.replica_of.as_deref(),
            Some("http://primary:50051")
        );
        assert_eq!(cfg.replication.tls_ca.as_deref(), Some("/etc/pg/ca.crt"));

        assert_eq!(cfg.tls.cert.as_deref(), Some("/etc/pg/server.crt"));
        assert_eq!(cfg.tls.key.as_deref(), Some("/etc/pg/server.key"));

        assert_eq!(
            cfg.auth.api_keys.as_deref(),
            Some(
                ["key-alpha".to_string(), "key-beta".to_string()].as_slice()
            )
        );
        assert_eq!(cfg.auth.no_auth, Some(false));

        assert_eq!(cfg.observability.log_level.as_deref(), Some("debug"));
        assert_eq!(cfg.observability.log_format.as_deref(), Some("json"));

        assert_eq!(cfg.query.timeout_ms, Some(5_000));
        assert_eq!(cfg.query.slow_query_ms, Some(250));
        assert_eq!(cfg.query.default_vector_ef, Some(100));
    }

    #[test]
    fn test_cli_overrides_config() {
        // Simulate the merge: cli_value.or(config_value).unwrap_or(default)
        // CLI value present → CLI wins.
        let cli_val: Option<String> = Some("from-cli".to_string());
        let config_val: Option<String> = Some("from-config".to_string());
        let result = cli_val.or(config_val).unwrap_or_else(|| "default".to_string());
        assert_eq!(result, "from-cli");

        // CLI absent, config present → config wins.
        let cli_val: Option<String> = None;
        let config_val: Option<String> = Some("from-config".to_string());
        let result = cli_val.or(config_val).unwrap_or_else(|| "default".to_string());
        assert_eq!(result, "from-config");

        // Both absent → default.
        let cli_val: Option<String> = None;
        let config_val: Option<String> = None;
        let result = cli_val.or(config_val).unwrap_or_else(|| "default".to_string());
        assert_eq!(result, "default");
    }

    #[test]
    fn test_explicit_path_not_found_is_error() {
        let result = load_config(Some(Path::new("/nonexistent/polargraph.toml")));
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("/nonexistent/polargraph.toml"));
    }

    #[test]
    fn test_default_config_is_all_none() {
        let cfg = Config::default();
        assert!(cfg.server.data_dir.is_none());
        assert!(cfg.server.grpc_port.is_none());
        assert!(cfg.storage.backup_dir.is_none());
        assert!(cfg.auth.api_keys.is_none());
        assert!(cfg.query.timeout_ms.is_none());
    }

    #[test]
    fn test_partial_toml_leaves_missing_keys_as_none() {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "[server]\ndata_dir = \"/data\"\n").unwrap();

        let cfg = load_config(Some(f.path())).unwrap();
        assert_eq!(cfg.server.data_dir.as_deref(), Some("/data"));
        // All other fields should be None / default.
        assert!(cfg.server.grpc_port.is_none());
        assert!(cfg.storage.backup_dir.is_none());
        assert!(cfg.auth.api_keys.is_none());
    }
}
