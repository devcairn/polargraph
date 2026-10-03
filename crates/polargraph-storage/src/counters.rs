//! Counters (step 9d, `docs/design/step9-inference-vectors-stats.md`): a
//! small generic primitive — `i64` per `(namespace, node)` in the `sts` CF,
//! incremented atomically with a RocksDB merge operator (no read, no MVCC
//! versions, not in the change log, not covered by retention). What the
//! counters mean is up to the caller (e.g. ContxtBroker usage feedback).

use polargraph_core::id::NodeId;
use rocksdb::{MergeOperands, WriteBatch};

use crate::{cf, error::StorageError, store::TripleStore};

/// Most increments or lookups per call.
pub const MAX_COUNTERS_PER_CALL: usize = 10_000;

/// The add merge operator: the sum of the existing value and every operand
/// (little-endian `i64`, saturating).
pub(crate) fn add_i64(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let decode = |b: &[u8]| <[u8; 8]>::try_from(b).map(i64::from_le_bytes).unwrap_or(0);
    let mut total = existing.map(decode).unwrap_or(0);
    for op in operands {
        total = total.saturating_add(decode(op));
    }
    Some(total.to_le_bytes().to_vec())
}

fn key(namespace: &str, node: &NodeId) -> Vec<u8> {
    let mut k = Vec::with_capacity(namespace.len() + 17);
    k.extend_from_slice(namespace.as_bytes());
    k.push(0);
    k.extend_from_slice(node.as_bytes());
    k
}

fn check(namespace: &str, n: usize) -> Result<(), StorageError> {
    if namespace.is_empty() || namespace.contains('\0') {
        return Err(StorageError::Validation(
            "counter namespace must be non-empty and contain no NUL".into(),
        ));
    }
    if n > MAX_COUNTERS_PER_CALL {
        return Err(StorageError::Validation(format!(
            "at most {MAX_COUNTERS_PER_CALL} counters per call"
        )));
    }
    Ok(())
}

impl TripleStore {
    /// Add each `delta` to its node's counter in `namespace`, atomically.
    pub fn increment_counters(
        &self,
        namespace: &str,
        increments: &[(NodeId, i64)],
    ) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(StorageError::ReadOnly(
                "counters are written on the primary".into(),
            ));
        }
        check(namespace, increments.len())?;
        let sts = self.cf_handle(cf::STS)?;
        let mut batch = WriteBatch::default();
        for (node, delta) in increments {
            batch.merge_cf(&sts, key(namespace, node), delta.to_le_bytes());
        }
        self.db_ref().write(batch)?;
        Ok(())
    }

    /// The counters of `nodes` in `namespace` (0 for a node never counted),
    /// in request order.
    pub fn get_counters(
        &self,
        namespace: &str,
        nodes: &[NodeId],
    ) -> Result<Vec<i64>, StorageError> {
        check(namespace, nodes.len())?;
        let sts = self.cf_handle(cf::STS)?;
        let keys: Vec<Vec<u8>> = nodes.iter().map(|n| key(namespace, n)).collect();
        self.db_ref()
            .batched_multi_get_cf(&sts, keys.iter(), false)
            .into_iter()
            .map(|r| {
                Ok(r?
                    .and_then(|v| <[u8; 8]>::try_from(&v[..]).ok())
                    .map(i64::from_le_bytes)
                    .unwrap_or(0))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn counters_add_up_and_survive_reopen() {
        let dir = TempDir::new().unwrap();
        let (a, b, c) = (NodeId::new(), NodeId::new(), NodeId::new());
        {
            let store = TripleStore::open(dir.path()).unwrap();
            store.increment_counters("used", &[(a, 1), (b, 5)]).unwrap();
            store
                .increment_counters("used", &[(a, 2), (b, -1)])
                .unwrap();
            store.increment_counters("other", &[(a, 100)]).unwrap();
            assert_eq!(
                store.get_counters("used", &[a, b, c]).unwrap(),
                vec![3, 4, 0]
            );
        }
        let store = TripleStore::open(dir.path()).unwrap();
        assert_eq!(store.get_counters("used", &[a, b]).unwrap(), vec![3, 4]);
        assert_eq!(store.get_counters("other", &[a]).unwrap(), vec![100]);
        assert!(store.increment_counters("", &[(a, 1)]).is_err());
    }
}
