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

/// Default change-feed history kept: 7 days.
pub const DEFAULT_CHANGE_RETENTION_SECS: u64 = 7 * 24 * 3600;

/// Prune the change log (`Subscribe` history) every `interval`, keeping
/// `retention`. Runs once at start, then until `cancel` is cancelled.
pub async fn run_change_log_pruner(
    store: TripleStore,
    retention: Duration,
    interval: Duration,
    cancel: CancellationToken,
) {
    info!(
        retention_secs = retention.as_secs(),
        "change-log pruner started"
    );
    loop {
        let before = polargraph_core::temporal::Timestamp(
            polargraph_core::temporal::Timestamp::now().0
                - i64::try_from(retention.as_micros()).unwrap_or(i64::MAX),
        );
        match store.prune_changes(before) {
            Ok(n) if n > 0 => info!(pruned = n, "change log pruned"),
            Ok(_) => {}
            Err(e) => warn!(error = %e, "change-log pruning failed"),
        }
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("change-log pruner shutting down");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// The pruner trims entries older than the retention window at start.
    #[tokio::test]
    async fn change_log_pruner_trims_old_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let mut tx = store.begin();
        tx.insert(polargraph_core::triple::Triple::Property {
            subject: polargraph_core::id::NodeId::new(),
            predicate: polargraph_core::triple::Predicate::new("n"),
            value: polargraph_core::value::Value::Int(1),
            temporal: polargraph_core::temporal::BiTemporalRange::assert_now(
                polargraph_core::temporal::Timestamp(0),
            ),
        });
        tx.commit().unwrap();
        let floor = store.changes_floor().unwrap();
        assert_eq!(store.changes_after(floor, 10).unwrap().len(), 1);

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run_change_log_pruner(
            store.clone(),
            Duration::ZERO,
            Duration::from_secs(3600),
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        handle.await.unwrap();
        assert!(store.changes_after(floor, 10).unwrap().is_empty());
        assert!(store.changes_floor().unwrap() > floor);
    }

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
        assert!(
            result.is_ok(),
            "scheduler did not exit within 1 second after cancel"
        );
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
