//! Bitemporal retention and RocksDB compaction.
//!
//! `CompactionManager` scans the six hexastore column families and deletes
//! triples that have aged out according to a [`RetentionPolicy`]. After
//! deleting, it triggers a full RocksDB compaction on every modified CF so
//! that tombstones are reclaimed immediately.
//!
//! # What is deleted
//!
//! Retention prunes *history*, never the current state of the graph. Every
//! stored version of one logical triple shares a key prefix (the S,P,O tuple),
//! so each CF is walked one version group at a time:
//!
//! - **Transaction-time age** (`tx_age_secs`): a version is deleted when a
//!   newer version with the **same `vt_start`** was committed before the
//!   cutoff. Such a version can no longer win any read — plain, `as_of_valid_time`
//!   or `as_of_tx_time` at or after the cutoff — so removing it changes no
//!   answer inside the retention window. This covers corrections and DELETE
//!   tombstones (which reuse the original `vt_start`). The newest version of a
//!   triple is never removed by this rule, however old it is, and versions
//!   that record valid-time history (a later `vt_start`) are kept.
//! - **Valid-time lookback** (`vt_lookback_secs`): a triple is removed
//!   entirely — every remaining version — once *all* of its versions have a
//!   `vt_end` earlier than `now − vt_lookback_secs`. Removing only some closed
//!   versions could let an older, still-open version win again and resurrect
//!   a deleted fact, so partial removal is never done.
//!
//! META and HNSW column families are never touched.

use std::time::{SystemTime, UNIX_EPOCH};

use polargraph_core::{schema::RetentionPolicy, temporal::Timestamp};

use crate::{cf, error::StorageError, keys, store::TripleStore};

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

    /// Scan all six hexastore CFs and delete versions that `policy` expires.
    ///
    /// After deletion, triggers a full compaction on every CF that had at
    /// least one deletion so that RocksDB reclaims disk space promptly.
    pub fn run_retention(&self, policy: &RetentionPolicy) -> Result<RetentionStats, StorageError> {
        let start = std::time::Instant::now();

        let now_us = now_micros();
        let tx_cutoff =
            Timestamp(now_us.saturating_sub((policy.tx_age_secs as i64).saturating_mul(1_000_000)));
        let vt_cutoff = policy
            .vt_lookback_secs
            .map(|secs| Timestamp(now_us.saturating_sub((secs as i64).saturating_mul(1_000_000))));

        let mut total_scanned = 0usize;
        let mut total_deleted = 0usize;

        for &cf_name in HEXASTORE_CFS {
            let (cf_scanned, cf_deleted) =
                self.store
                    .prune_cf_groups(cf_name, keys::HEXASTORE_TUPLE_LEN, |versions| {
                        select_expired(versions, tx_cutoff, vt_cutoff)
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

/// Pick the versions of one triple that `policy` expires.
///
/// `versions` are all stored versions of a single (S,P,O) in one CF, sorted by
/// `tt` ascending (the hexastore key order). Returns indices into `versions`.
/// See the module docs for the rules.
fn select_expired<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    versions: &[(K, V)],
    tx_cutoff: Timestamp,
    vt_cutoff: Option<Timestamp>,
) -> Vec<usize> {
    // Parse (tt, vt_start, vt_end); entries we can't parse are never touched.
    let parsed: Vec<Option<(Timestamp, Timestamp, Timestamp)>> = versions
        .iter()
        .map(|(k, v)| {
            let k = k.as_ref();
            if k.len() < 8 {
                return None;
            }
            let (vt_start, vt_end) = extract_vt(v.as_ref())?;
            Some((keys::hexastore_tt(k), vt_start, vt_end))
        })
        .collect();

    // Transaction-time rule: shadowed by a newer same-vt_start version
    // committed before the cutoff.
    let mut expired = vec![false; versions.len()];
    for (i, vi) in parsed.iter().enumerate() {
        let Some((tt_i, start_i, _)) = vi else {
            continue;
        };
        expired[i] = parsed.iter().any(|vj| {
            matches!(vj, Some((tt_j, start_j, _))
                if start_j == start_i && tt_j > tt_i && *tt_j < tx_cutoff)
        });
    }

    // Valid-time rule: drop the whole triple once every surviving version is
    // closed before the lookback cutoff.
    if let Some(vt_cut) = vt_cutoff {
        let survivors_all_closed = parsed
            .iter()
            .zip(&expired)
            .filter(|(_, e)| !**e)
            .all(|(p, _)| matches!(p, Some((_, _, vt_end)) if *vt_end < vt_cut));
        if survivors_all_closed {
            return (0..versions.len())
                .filter(|&i| parsed[i].is_some())
                .collect();
        }
    }

    (0..versions.len()).filter(|&i| expired[i]).collect()
}

/// Extract `(vt_start, vt_end)` from a raw value blob without a full decode.
///
/// Layout by discriminant:
/// - 0x01 (Relation): `[disc(1)][edge_id(16)][vt_start(8)][vt_end(8)]`
/// - 0x02 (Property): `[disc(1)][vt_start(8)][vt_end(8)][json]`
/// - 0x03 (Vector):   `[disc(1)][vt_start(8)][vt_end(8)][...]`
fn extract_vt(value: &[u8]) -> Option<(Timestamp, Timestamp)> {
    let at = |off: usize| Timestamp::from_be_bytes(value[off..off + 8].try_into().unwrap());
    match *value.first()? {
        0x01 if value.len() >= 33 => Some((at(17), at(25))),
        0x02 | 0x03 if value.len() >= 17 => Some((at(1), at(9))),
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

    const OPEN: i64 = i64::MAX;

    fn version(tt: i64, vt_start: i64, vt_end: i64) -> (Vec<u8>, Vec<u8>) {
        let mut key = vec![0u8; keys::HEXASTORE_KEY_LEN];
        key[keys::HEXASTORE_TUPLE_LEN..].copy_from_slice(&Timestamp(tt).to_be_bytes());
        let t = BiTemporalRange {
            vt_start: Timestamp(vt_start),
            vt_end: Timestamp(vt_end),
            tt: Timestamp(tt),
        };
        (key, codec::encode_property(&Value::Bool(true), &t).unwrap())
    }

    fn select(versions: &[(Vec<u8>, Vec<u8>)], tx_cut: i64, vt_cut: Option<i64>) -> Vec<usize> {
        select_expired(versions, Timestamp(tx_cut), vt_cut.map(Timestamp))
    }

    #[test]
    fn extract_vt_from_relation_value() {
        use polargraph_core::id::EdgeId;
        let eid = EdgeId(uuid::Uuid::from_bytes([0xAB; 16]));
        let t = BiTemporalRange {
            vt_start: Timestamp(100),
            vt_end: Timestamp(999),
            tt: Timestamp(0),
        };
        let encoded = codec::encode_relation(&eid, &t);
        assert_eq!(extract_vt(&encoded), Some((Timestamp(100), Timestamp(999))));
    }

    #[test]
    fn extract_vt_from_property_and_vector_values() {
        let t = BiTemporalRange {
            vt_start: Timestamp(200),
            vt_end: Timestamp(888),
            tt: Timestamp(0),
        };
        let p = codec::encode_property(&Value::Int(42), &t).unwrap();
        assert_eq!(extract_vt(&p), Some((Timestamp(200), Timestamp(888))));
        let v = codec::encode_property(&Value::Vector(vec![1.0, 2.0]), &t).unwrap();
        assert_eq!(extract_vt(&v), Some((Timestamp(200), Timestamp(888))));
    }

    #[test]
    fn sole_version_is_never_expired_by_tx_age() {
        // An old, still-current fact is live state, not history.
        assert!(select(&[version(50, 0, OPEN)], 100, None).is_empty());
    }

    #[test]
    fn correction_before_cutoff_expires_the_shadowed_version() {
        let vs = [version(10, 0, OPEN), version(20, 0, OPEN)];
        assert_eq!(select(&vs, 100, None), vec![0]);
    }

    #[test]
    fn correction_after_cutoff_keeps_the_shadowed_version() {
        // as_of_tx_time queries inside the window can still see version 0.
        let vs = [version(10, 0, OPEN), version(200, 0, OPEN)];
        assert!(select(&vs, 100, None).is_empty());
    }

    #[test]
    fn valid_time_history_is_kept() {
        // Sequential valid-time versions: v0 answers as_of_valid_time < 50.
        let vs = [version(10, 0, OPEN), version(20, 50, OPEN)];
        assert!(select(&vs, 100, None).is_empty());
    }

    #[test]
    fn delete_tombstone_expires_original_but_is_kept_itself() {
        // DELETE re-inserts with the original vt_start and a closed vt_end.
        let vs = [version(10, 0, OPEN), version(20, 0, 20)];
        assert_eq!(select(&vs, 100, None), vec![0]);
    }

    #[test]
    fn vt_lookback_never_removes_a_tombstone_alone() {
        // Tombstone committed inside the tx window: the open original must not
        // resurface, so nothing is removed yet.
        let vs = [version(10, 0, OPEN), version(200, 0, 20)];
        assert!(select(&vs, 100, Some(1_000)).is_empty());
    }

    #[test]
    fn vt_lookback_removes_fully_closed_triple() {
        let vs = [version(10, 0, OPEN), version(20, 0, 20)];
        assert_eq!(select(&vs, 100, Some(1_000)), vec![0, 1]);
    }

    #[test]
    fn vt_lookback_keeps_open_ended_triple() {
        assert!(select(&[version(500, 0, OPEN)], 100, Some(200)).is_empty());
    }
}
