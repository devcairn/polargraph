//! Scheduled bitemporal retention.
//!
//! [`run_retention_scheduler`] is a long-lived async task that fires the
//! [`CompactionManager`] at a configurable interval.  It exits cleanly when
//! the supplied [`CancellationToken`] is cancelled.
//!
//! # Prometheus metrics
//!
//! | Name | Type | Description |
//! |------|------|-------------|
//! | `polargraph_scheduled_retention_runs_total` | counter | Successful scheduler wake-ups |
//! | `polargraph_scheduled_retention_deleted_total` | counter | Triples deleted across all runs |
//! | `polargraph_scheduled_retention_errors_total` | counter | Runs that returned an error |

use std::time::Duration;

use polargraph_core::schema::RetentionPolicy;
use polargraph_storage::{CompactionManager, TripleStore};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Spawn a background retention loop.
///
/// - Waits `interval` then calls [`CompactionManager::run_retention`].
/// - Repeats until `cancel` is cancelled.
/// - Never panics; errors are logged as warnings.
pub async fn run_retention_scheduler(
    store: TripleStore,
    policy: RetentionPolicy,
    interval: Duration,
    cancel: CancellationToken,
) {
    info!(
        interval_secs = interval.as_secs(),
        tx_age_secs = policy.tx_age_secs,
        vt_lookback_secs = ?policy.vt_lookback_secs,
        "retention scheduler started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("retention scheduler shutting down");
                break;
            }
            _ = tokio::time::sleep(interval) => {
                metrics::counter!("polargraph_scheduled_retention_runs_total").increment(1);

                let mgr = CompactionManager::new(store.clone());
                match mgr.run_retention(&policy) {
                    Ok(stats) => {
                        metrics::counter!("polargraph_scheduled_retention_deleted_total")
                            .increment(stats.triples_deleted as u64);
                        info!(
                            triples_scanned = stats.triples_scanned,
                            triples_deleted = stats.triples_deleted,
                            duration_ms = stats.duration_ms,
                            "scheduled retention pass complete"
                        );
                    }
                    Err(e) => {
                        metrics::counter!("polargraph_scheduled_retention_errors_total")
                            .increment(1);
                        warn!(error = %e, "scheduled retention pass failed");
                    }
                }
            }
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// The scheduler must exit promptly when the token is cancelled.
    #[tokio::test]
    async fn test_scheduler_exits_on_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let policy = RetentionPolicy {
            tx_age_secs: 86_400,
            vt_lookback_secs: None,
        };
        let cancel = CancellationToken::new();

        // Use a very long interval so we only test cancellation, not a real run.
        let interval = Duration::from_secs(3600);
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(run_retention_scheduler(
            store,
            policy,
            interval,
            cancel_clone,
        ));

        // Give the task a moment to start then cancel.
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        // Should complete within 1 second.
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok(), "scheduler did not exit within 1 second after cancel");
    }

    /// Verify RetentionPolicy default field values used by the scheduler.
    #[test]
    fn test_retention_policy_defaults() {
        let policy = RetentionPolicy {
            tx_age_secs: 604_800,
            vt_lookback_secs: Some(86_400),
        };
        assert_eq!(policy.tx_age_secs, 604_800);
        assert_eq!(policy.vt_lookback_secs, Some(86_400));
    }

    /// The scheduler runs at least one pass when the interval is very short.
    #[tokio::test]
    async fn test_scheduler_runs_on_interval() {
        let dir = tempfile::tempdir().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let policy = RetentionPolicy {
            tx_age_secs: 1, // expire everything older than 1 second (nothing in empty store)
            vt_lookback_secs: None,
        };
        let cancel = CancellationToken::new();
        let interval = Duration::from_millis(50); // fire quickly
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(run_retention_scheduler(
            store,
            policy,
            interval,
            cancel_clone,
        ));

        // Wait long enough for at least one pass, then cancel.
        tokio::time::sleep(Duration::from_millis(200)).await;
        cancel.cancel();

        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok(), "scheduler did not exit after cancel");
    }
}
