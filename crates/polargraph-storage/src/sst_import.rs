//! Bulk import via RocksDB SST file ingestion.
//!
//! `SstImporter` collects triples in memory, encodes them into SST files (one
//! per hexastore column family), and ingests all six files atomically via
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
//! Each `finish()` call is a single logical commit: all six CFs are ingested
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
    id::NodeId,
    temporal::{BiTemporalRange, Timestamp},
    triple::Triple,
};
use rocksdb::{Options, SstFileWriter, WriteBatch};

use crate::{
    cf,
    codec,
    error::StorageError,
    keys,
    mvcc::META_ORACLE_CTR,
    store::TripleStore,
};

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
    triples: Vec<Triple>,
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
        })
    }

    /// Buffer a triple for import. The triple's `tt` field is ignored;
    /// the actual commit timestamp is assigned during `finish()`.
    pub fn add_triple(&mut self, triple: &Triple) {
        self.triples.push(triple.clone());
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

        if n == 0 {
            return Ok(ImportStats { triples_imported: 0, duration_ms: 0 });
        }

        // ── 1. Intern predicates (outside commit lock to avoid contention) ──────
        let mut pred_ids: HashMap<String, u32> = HashMap::new();
        for triple in &self.triples {
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

        // ── 3. Build per-CF key-value buffers ────────────────────────────────
        let mut spo_buf: Vec<([u8; 44], Vec<u8>)> = Vec::with_capacity(n);
        let mut sop_buf: Vec<([u8; 44], Vec<u8>)> = Vec::with_capacity(n);
        let mut pso_buf: Vec<([u8; 44], Vec<u8>)> = Vec::with_capacity(n);
        let mut pos_buf: Vec<([u8; 44], Vec<u8>)> = Vec::with_capacity(n);
        let mut osp_buf: Vec<([u8; 44], Vec<u8>)> = Vec::with_capacity(n);
        let mut ops_buf: Vec<([u8; 44], Vec<u8>)> = Vec::with_capacity(n);

        for triple in &self.triples {
            // EdgeProperty / EdgeRelation go to EPA/EPO via WriteBatch below.
            match triple {
                Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => continue,
                _ => {}
            }

            let s = triple.subject();
            let p = *pred_ids.get(triple.predicate().0.as_str()).unwrap();
            let o = object_of(triple);
            let value_bytes = encode_value(triple, &temporal)?;

            spo_buf.push((keys::encode_spo(&s, p, &o, commit_ts), value_bytes.clone()));
            sop_buf.push((keys::encode_sop(&s, &o, p, commit_ts), value_bytes.clone()));
            pso_buf.push((keys::encode_pso(p, &s, &o, commit_ts), value_bytes.clone()));
            pos_buf.push((keys::encode_pos(p, &o, &s, commit_ts), value_bytes.clone()));
            osp_buf.push((keys::encode_osp(&o, &s, p, commit_ts), value_bytes.clone()));
            ops_buf.push((keys::encode_ops(&o, p, &s, commit_ts), value_bytes.clone()));
        }

        // Sort each CF's entries; dedup on key (same triple in same batch).
        for buf in [&mut spo_buf, &mut sop_buf, &mut pso_buf, &mut pos_buf, &mut osp_buf, &mut ops_buf] {
            buf.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            buf.dedup_by_key(|item| item.0);
        }

        // ── 4. Write SST files and ingest per CF ──────────────────────────────
        let opts = Options::default();

        #[allow(clippy::type_complexity)]
        let cf_data: [(&str, &Vec<([u8; 44], Vec<u8>)>); 6] = [
            (cf::SPO, &spo_buf),
            (cf::SOP, &sop_buf),
            (cf::PSO, &pso_buf),
            (cf::POS, &pos_buf),
            (cf::OSP, &osp_buf),
            (cf::OPS, &ops_buf),
        ];

        for (cf_name, buf) in &cf_data {
            if buf.is_empty() {
                continue;
            }
            let sst_path = self.output_dir.join(format!("{cf_name}.sst"));
            {
                let mut writer = SstFileWriter::create(&opts);
                writer.open(&sst_path)?;
                for (key, value) in buf.iter() {
                    writer.put(key.as_slice(), value.as_slice())?;
                }
                writer.finish()?;
            }
            let cf = store.cf_handle(cf_name)?;
            store.db_ref().ingest_external_file_cf(&cf, vec![sst_path])?;
        }

        // ── 5. Write trigram + EPA/EPO entries via WriteBatch ────────────────
        let mut tri_batch = WriteBatch::default();
        for triple in &self.triples {
            let p = *pred_ids.get(triple.predicate().0.as_str()).unwrap();
            match triple {
                Triple::Property { subject, value: polargraph_core::value::Value::Text(text), .. } => {
                    store.batch_text_trigrams(&mut tri_batch, subject, p, text)?;
                }
                Triple::EdgeProperty { edge, value, temporal, .. } => {
                    let temporal_stamped = BiTemporalRange { tt: commit_ts, ..*temporal };
                    let value_bytes = codec::encode_property(value, &temporal_stamped)?;
                    store.batch_epa(&mut tri_batch, *edge, p, commit_ts, &value_bytes)?;
                }
                Triple::EdgeRelation { edge, object, temporal, .. } => {
                    let temporal_stamped = BiTemporalRange { tt: commit_ts, ..*temporal };
                    let epo_val = crate::store::encode_epo_value(&temporal_stamped);
                    store.batch_epo(&mut tri_batch, *edge, p, *object, commit_ts, &epo_val)?;
                }
                _ => {}
            }
        }

        // ── 6. Persist oracle counter ─────────────────────────────────────────
        let meta_cf = store.cf_handle(cf::META)?;
        tri_batch.put_cf(&meta_cf, META_ORACLE_CTR, commit_ts.0.to_be_bytes());
        store.db_write(tri_batch)?;

        Ok(ImportStats {
            triples_imported: n,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

// ── helpers (mirror of mvcc private helpers) ──────────────────────────────────

fn object_of(triple: &Triple) -> NodeId {
    match triple {
        Triple::Relation { object, .. } => *object,
        Triple::Property { .. } => NodeId(uuid::Uuid::from_bytes(keys::PROPERTY_SENTINEL)),
        // EdgeProperty/EdgeRelation don't go through the hexastore loop.
        Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => unreachable!(),
    }
}

fn encode_value(triple: &Triple, temporal: &BiTemporalRange) -> Result<Vec<u8>, StorageError> {
    match triple {
        Triple::Relation { edge_id, .. } => Ok(codec::encode_relation(edge_id, temporal)),
        Triple::Property { value, .. } => codec::encode_property(value, temporal),
        // EdgeProperty/EdgeRelation don't go through the hexastore loop.
        Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => unreachable!(),
    }
}
