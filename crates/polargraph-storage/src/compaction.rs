//! Bitemporal retention and RocksDB compaction.
//!
//! `CompactionManager` scans the six hexastore column families and deletes
//! triples that have aged out according to a [`RetentionPolicy`]. After
//! deleting, it triggers a full RocksDB compaction on every modified CF so
//! that tombstones are reclaimed immediately.
//!
//! # What is deleted
//!
//! Two independent age checks may trigger deletion (either is sufficient):
//!
//! - **Transaction-time age** (`tx_age_secs`): the triple's `tt` (the wall-clock
//!   time when it was written) is older than `tx_age_secs` seconds.
//! - **Valid-time lookback** (`vt_lookback_secs`): the triple's `vt_end` is
//!   earlier than `now − vt_lookback_secs` seconds, meaning even the fact's
//!   claimed validity window has fully expired.
//!
//! META and HNSW column families are never touched.

use std::time::{SystemTime, UNIX_EPOCH};

use polargraph_core::{schema::RetentionPolicy, temporal::Timestamp};

use crate::{cf, error::StorageError, store::TripleStore};

const HEXASTORE_CFS: &[&str] = &[cf::SPO, cf::SOP, cf::PSO, cf::POS, cf::OSP, cf::OPS];

/// Statistics returned by a completed retention run.
#[derive(Debug, Clone, Default)]
pub struct RetentionStats {
    pub triples_scanned: usize,
    pub triples_deleted: usize,
    pub duration_ms: u64,
}

/// Manages bitemporal retention and RocksDB compaction for a [`TripleStore`].
pub struct CompactionManager {
    store: TripleStore,
}

impl CompactionManager {
    pub fn new(store: TripleStore) -> Self {
        Self { store }
    }

    /// Scan all six hexastore CFs and delete triples that violate `policy`.
    ///
    /// After deletion, triggers a full compaction on every CF that had at
    /// least one deletion so that RocksDB reclaims disk space promptly.
    pub fn run_retention(&self, policy: &RetentionPolicy) -> Result<RetentionStats, StorageError> {
        let start = std::time::Instant::now();

        let now_us = now_micros();
        let tx_cutoff = Timestamp(now_us.saturating_sub(
            (policy.tx_age_secs as i64).saturating_mul(1_000_000),
        ));
        let vt_cutoff = policy.vt_lookback_secs.map(|secs| {
            Timestamp(now_us.saturating_sub((secs as i64).saturating_mul(1_000_000)))
        });

        let mut total_scanned = 0usize;
        let mut total_deleted = 0usize;

        for &cf_name in HEXASTORE_CFS {
            let mut cf_scanned = 0usize;
            let cf_deleted = self.store.scan_cf_raw(cf_name, |key, value| {
                cf_scanned += 1;
                is_expired(key, value, tx_cutoff, vt_cutoff)
            })?;

            total_scanned += cf_scanned;
            total_deleted += cf_deleted;

            if cf_deleted > 0 {
                self.store.compact_cf(cf_name)?;
            }
        }

        Ok(RetentionStats {
            triples_scanned: total_scanned,
            triples_deleted: total_deleted,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Returns `true` if the key/value pair should be deleted under the policy.
///
/// `key` is always 44 bytes for hexastore CFs; `tt` lives in the last 8 bytes.
/// `vt_end` is extracted from the value based on the discriminant byte.
fn is_expired(
    key: &[u8],
    value: &[u8],
    tx_cutoff: Timestamp,
    vt_cutoff: Option<Timestamp>,
) -> bool {
    if key.len() < 8 {
        return false;
    }
    let tt_bytes: [u8; 8] = match key[key.len() - 8..].try_into() {
        Ok(b) => b,
        Err(_) => return false,
    };
    let tt = Timestamp::from_be_bytes(tt_bytes);

    if tt.0 < tx_cutoff.0 {
        return true;
    }

    if let Some(vt_cut) = vt_cutoff {
        if let Some(vt_end) = extract_vt_end(value) {
            if vt_end.0 < vt_cut.0 {
                return true;
            }
        }
    }

    false
}

/// Extract `vt_end` from a raw value blob without a full decode.
///
/// Layout by discriminant:
/// - 0x01 (Relation): `[disc(1)][edge_id(16)][vt_start(8)][vt_end(8)]` → vt_end at [25..33]
/// - 0x02 (Property): `[disc(1)][vt_start(8)][vt_end(8)][json]`        → vt_end at [9..17]
/// - 0x03 (Vector):   `[disc(1)][vt_start(8)][vt_end(8)][...]`          → vt_end at [9..17]
fn extract_vt_end(value: &[u8]) -> Option<Timestamp> {
    if value.is_empty() {
        return None;
    }
    match value[0] {
        0x01 if value.len() >= 33 => {
            Some(Timestamp::from_be_bytes(value[25..33].try_into().unwrap()))
        }
        0x02 | 0x03 if value.len() >= 17 => {
            Some(Timestamp::from_be_bytes(value[9..17].try_into().unwrap()))
        }
        _ => None,
    }
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec;
    use polargraph_core::{temporal::BiTemporalRange, value::Value};

    fn temporal(vt_start: i64, vt_end: i64) -> BiTemporalRange {
        BiTemporalRange {
            vt_start: Timestamp(vt_start),
            vt_end: Timestamp(vt_end),
            tt: Timestamp(0),
        }
    }

    fn make_key(tt: i64) -> [u8; 44] {
        let mut key = [0u8; 44];
        key[36..44].copy_from_slice(&Timestamp(tt).to_be_bytes());
        key
    }

    #[test]
    fn extract_vt_end_from_relation_value() {
        use polargraph_core::id::EdgeId;
        let eid = EdgeId(uuid::Uuid::from_bytes([0xAB; 16]));
        let t = temporal(100, 999);
        let encoded = codec::encode_relation(&eid, &t);
        let vt_end = extract_vt_end(&encoded).unwrap();
        assert_eq!(vt_end, Timestamp(999));
    }

    #[test]
    fn extract_vt_end_from_property_value() {
        let t = temporal(200, 888);
        let encoded = codec::encode_property(&Value::Int(42), &t).unwrap();
        let vt_end = extract_vt_end(&encoded).unwrap();
        assert_eq!(vt_end, Timestamp(888));
    }

    #[test]
    fn extract_vt_end_from_vector_value() {
        let t = temporal(300, 777);
        let encoded = codec::encode_property(&Value::Vector(vec![1.0, 2.0]), &t).unwrap();
        let vt_end = extract_vt_end(&encoded).unwrap();
        assert_eq!(vt_end, Timestamp(777));
    }

    #[test]
    fn is_expired_by_tx_age() {
        let key = make_key(50);
        let value = codec::encode_property(&Value::Bool(true), &temporal(0, i64::MAX)).unwrap();
        // tx_cutoff=100 → tt(50) < 100 → expired
        assert!(is_expired(&key, &value, Timestamp(100), None));
    }

    #[test]
    fn is_not_expired_recent_triple() {
        let key = make_key(500);
        let value = codec::encode_property(&Value::Bool(true), &temporal(0, i64::MAX)).unwrap();
        // tx_cutoff=100 → tt(500) ≥ 100 → not expired
        assert!(!is_expired(&key, &value, Timestamp(100), None));
    }

    #[test]
    fn is_expired_by_vt_lookback() {
        let key = make_key(500); // recent tx, so tx check passes
        let value = codec::encode_property(&Value::Bool(true), &temporal(0, 10)).unwrap();
        // vt_end=10, vt_cutoff=100 → expired by vt
        assert!(is_expired(&key, &value, Timestamp(100), Some(Timestamp(100))));
    }

    #[test]
    fn is_not_expired_open_ended_vt() {
        let key = make_key(500);
        let value = codec::encode_property(&Value::Bool(true), &temporal(0, i64::MAX)).unwrap();
        // vt_end=MAX → never expired by vt
        assert!(!is_expired(&key, &value, Timestamp(100), Some(Timestamp(200))));
    }
}
