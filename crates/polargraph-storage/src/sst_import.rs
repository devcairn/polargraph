//! Bulk import via RocksDB SST file ingestion.
//!
//! `SstImporter` collects triples in memory, encodes them into SST files (one
//! per quad-index column family), and ingests all eight files via
//! `DB::ingest_external_file_cf`. This bypasses gRPC and the write-path WAL,
//! making it 10–100× faster than individual `insert()` calls for large initial
//! loads.
//!
//! # Why offline-only
//!
//! SST ingestion requires exclusive write access to the column families being
//! ingested. Running the import tool while `polargraphd` is serving requests
//! will conflict and may corrupt the database. Always stop the server before
//! running `polargraph-import`.
//!
//! # Atomicity
//!
//! Each `finish()` call is a single logical commit: all eight CFs are ingested
//! within a single `begin_commit()` guard so the commit timestamp is
//! monotonically increasing. If ingestion of any CF fails, the remaining CFs
//! are skipped and the returned error should be treated as fatal (the DB may
//! be in a partially-imported state).

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Instant,
};

use polargraph_core::{
    id::GraphId,
    temporal::{BiTemporalRange, Timestamp},
    triple::Triple,
};
use rocksdb::{Options, SstFileWriter, WriteBatch};

use crate::{
    cf, codec,
    error::StorageError,
    keys::{self, Order, QuadKey},
    mvcc::META_ORACLE_CTR,
    store::TripleStore,
};

/// Key/value pairs destined for one SST file.
pub(crate) type SortedEntries = Vec<(Vec<u8>, Vec<u8>)>;

/// Sort `entries` by key (dropping duplicate keys), write them to one SST
/// file in `dir`, and ingest it into `cf_name`. No-op for an empty list.
pub(crate) fn ingest_sorted(
    store: &TripleStore,
    dir: &Path,
    cf_name: &str,
    mut entries: SortedEntries,
) -> Result<(), StorageError> {
    if entries.is_empty() {
        return Ok(());
    }
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    entries.dedup_by(|a, b| a.0 == b.0);
    std::fs::create_dir_all(dir)?;
    let sst_path = dir.join(format!("{cf_name}.sst"));
    {
        let opts = Options::default();
        let mut writer = SstFileWriter::create(&opts);
        writer.open(&sst_path)?;
        for (key, value) in &entries {
            writer.put(key, value)?;
        }
        writer.finish()?;
    }
    let cf = store.cf_handle(cf_name)?;
    store
        .db_ref()
        .ingest_external_file_cf(&cf, vec![sst_path])?;
    Ok(())
}

/// Statistics returned by a successful `SstImporter::finish()` call.
#[derive(Debug, Clone)]
pub struct ImportStats {
    pub triples_imported: usize,
    pub duration_ms: u64,
}

/// Buffers triples and bulk-imports them via RocksDB SST file ingestion.
///
/// # Usage
///
/// ```ignore
/// let mut importer = SstImporter::new(&temp_dir)?;
/// for triple in my_triples {
///     importer.add_triple(&triple);
/// }
/// let stats = importer.finish(&store)?;
/// ```
pub struct SstImporter {
    output_dir: PathBuf,
    triples: Vec<(Triple, GraphId)>,
    iris: Vec<String>,
}

impl SstImporter {
    /// Create a new importer that will write SST files into `output_dir`.
    ///
    /// `output_dir` is created if it does not exist.
    pub fn new(output_dir: &Path) -> Result<Self, StorageError> {
        std::fs::create_dir_all(output_dir)?;
        Ok(Self {
            output_dir: output_dir.to_path_buf(),
            triples: Vec::new(),
            iris: Vec::new(),
        })
    }

    /// Record `iri` in the IRI dictionary as part of this import (see
    /// `Transaction::bind_iri`). Duplicates are fine.
    pub fn add_iri(&mut self, iri: impl Into<String>) {
        self.iris.push(iri.into());
    }

    /// Buffer a triple for import. The triple's `tt` field is ignored;
    /// the actual commit timestamp is assigned during `finish()`.
    pub fn add_triple(&mut self, triple: &Triple) {
        self.add_triple_in(triple, GraphId::DEFAULT);
    }

    /// Buffer a triple for import into graph `g` (intern graph IRIs with
    /// `TripleStore::intern_graph` first).
    pub fn add_triple_in(&mut self, triple: &Triple, g: GraphId) {
        self.triples.push((triple.clone(), g));
    }

    /// Encode all buffered triples into SST files, ingest them, and advance
    /// the MVCC oracle.
    ///
    /// Steps:
    /// 1. Intern all unique predicates (writes to META CF).
    /// 2. Acquire a commit timestamp via `begin_commit()`.
    /// 3. Encode keys for all 6 hexastore CFs; sort per CF.
    /// 4. Write one SST file per CF and call `ingest_external_file_cf`.
    /// 5. Persist the updated oracle counter to META CF.
    pub fn finish(self, store: &TripleStore) -> Result<ImportStats, StorageError> {
        let start = Instant::now();
        let n = self.triples.len();

        if n == 0 && self.iris.is_empty() {
            return Ok(ImportStats {
                triples_imported: 0,
                duration_ms: 0,
            });
        }

        // ── 1. Intern predicates (outside commit lock to avoid contention) ──────
        let mut pred_ids: HashMap<String, u32> = HashMap::new();
        for (triple, _) in &self.triples {
            let pred_str = triple.predicate().0.clone();
            if let std::collections::hash_map::Entry::Vacant(e) = pred_ids.entry(pred_str) {
                let id = store.intern_predicate(e.key())?;
                e.insert(id);
            }
        }

        // ── 2. Acquire commit timestamp ───────────────────────────────────────
        let (commit_ts, _guard) = store.oracle().begin_commit();

        let temporal = BiTemporalRange {
            vt_start: Timestamp::now(),
            vt_end: Timestamp::END_OF_TIME,
            tt: commit_ts,
        };

        // ── 3. Build per-order key-value buffers ─────────────────────────────
        // Out-of-line values, trigrams and annotations go through `batch`.
        // Bulk import only adds: existing values are never replaced.
        let mut batch = WriteBatch::default();
        let mut per_order: HashMap<Order, SortedEntries> = HashMap::new();
        for (triple, g) in &self.triples {
            let g = *g;
            let p = *pred_ids.get(triple.predicate().0.as_str()).unwrap();
            let (q, value_bytes) = match triple {
                Triple::Relation {
                    subject,
                    object,
                    edge_id,
                    ..
                } => (
                    QuadKey {
                        s: *subject,
                        p,
                        o: *object,
                        g,
                        tt: commit_ts,
                    },
                    codec::encode_relation(edge_id, &temporal),
                ),
                Triple::Property { subject, value, .. } => {
                    if let Some(text) = value.as_text() {
                        store.batch_text_trigrams(&mut batch, subject, p, g, text)?;
                    }
                    (
                        QuadKey {
                            s: *subject,
                            p,
                            o: keys::value_object(value),
                            g,
                            tt: commit_ts,
                        },
                        store.encode_property_entry(&mut batch, value, &temporal)?,
                    )
                }
                Triple::EdgeProperty {
                    edge,
                    value,
                    temporal,
                    ..
                } => {
                    let stamped = BiTemporalRange {
                        tt: commit_ts,
                        ..*temporal
                    };
                    let value_bytes = codec::encode_property(value, &stamped)?;
                    store.batch_epa(&mut batch, *edge, p, g, commit_ts, &value_bytes)?;
                    continue;
                }
                Triple::EdgeRelation {
                    edge,
                    object,
                    temporal,
                    ..
                } => {
                    let stamped = BiTemporalRange {
                        tt: commit_ts,
                        ..*temporal
                    };
                    let epo_val = crate::store::encode_epo_value(&stamped);
                    store.batch_epo(&mut batch, *edge, p, *object, g, commit_ts, &epo_val)?;
                    continue;
                }
            };
            for order in Order::ALL {
                per_order
                    .entry(order)
                    .or_default()
                    .push((order.encode(&q).to_vec(), value_bytes.clone()));
            }
        }

        // ── 4. Write SST files and ingest per CF ──────────────────────────────
        for (order, entries) in per_order {
            ingest_sorted(store, &self.output_dir, order.cf(), entries)?;
        }

        // ── 5. Blob, trigram and annotation entries go in via `batch` ────────
        let mut tri_batch = batch;

        // ── 6. IRI dictionary entries ─────────────────────────────────────────
        let mut pending_iris = std::collections::HashMap::new();
        for iri in &self.iris {
            store.batch_iri(&mut tri_batch, iri, &mut pending_iris)?;
        }

        // ── 7. Persist oracle counter ─────────────────────────────────────────
        let meta_cf = store.cf_handle(cf::META)?;
        tri_batch.put_cf(&meta_cf, META_ORACLE_CTR, commit_ts.0.to_be_bytes());
        store.db_write(tri_batch)?;

        Ok(ImportStats {
            triples_imported: n,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}
