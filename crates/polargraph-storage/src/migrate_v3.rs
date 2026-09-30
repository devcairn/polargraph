//! Offline migration of a store from storage format v2 to v3
//! (`docs/design/v3-key-layout.md` §7).
//!
//! v2 kept each triple in six 44-byte-key CFs with a `0xFF×16` object sentinel
//! for properties and no graph dimension. v3 writes every quad to eight
//! 48-byte-key orders with `g = 0` (default graph), replaces the sentinel with
//! the value's content hash, moves large values to the `blob` CF, and rebuilds
//! the trigram index.
//!
//! The migration reads only the v2 `spo`, `drv`, `epa` and `epo` CFs — every
//! other v2 CF is derivable from them — and writes sorted SST files into the
//! new CFs in bounded chunks. It then verifies entry counts across all eight
//! orders and an order-independent checksum of every decoded version. The
//! **commit point** is recording format 3 in META; only after that are the
//! v2 CFs dropped. An interrupted run leaves v2 intact and is simply rerun
//! (partial v3 data is cleared first).

use std::{path::Path, time::Instant};

use polargraph_core::{id::GraphId, temporal::Timestamp};
use rocksdb::{IteratorMode, Options, WriteBatch};
use tracing::info;

use crate::{
    cf,
    codec::{self, DecodedValue},
    error::StorageError,
    keys::{self, Order, QuadKey},
    sst_import::{ingest_sorted, SortedEntries},
    store::{StoreMode, TripleStore, V2Policy, META_STORAGE_FORMAT, STORAGE_FORMAT},
};

/// Index entries buffered per CF before a chunk is written as an SST file.
const CHUNK_ENTRIES: usize = 500_000;

/// Outcome of [`migrate`].
#[derive(Debug, Clone, Default)]
pub struct MigrationReport {
    /// `true` if the store was already format 3 (nothing done).
    pub already_migrated: bool,
    /// Quad versions written (relations + properties, including synthesized
    /// closing versions).
    pub quad_versions: u64,
    /// Property versions read from v2.
    pub property_versions: u64,
    /// Closing versions added so that a v2 correction (a new value under the
    /// same sentinel key) still supersedes the value it replaced — see
    /// `QuadOut::group`.
    pub closing_versions_synthesized: u64,
    pub derived_versions: u64,
    pub edge_property_annotations: u64,
    pub edge_relation_annotations: u64,
    /// Property values moved out of line.
    pub values_out_of_line: u64,
    pub duration_ms: u64,
}

/// Open a store for migration (or for taking a pre-migration backup) —
/// unlike [`TripleStore::open`], a v2 store is accepted.
pub fn open_for_migration(path: &Path) -> Result<TripleStore, StorageError> {
    TripleStore::open_db(path, StoreMode::Primary, V2Policy::AllowForMigration)
}

/// Migrate the store at `path` (server stopped) to storage format 3.
pub fn migrate(path: &Path) -> Result<MigrationReport, StorageError> {
    let store = open_for_migration(path)?;
    migrate_store(&store, path)
}

/// Migrate an already-opened store (see [`open_for_migration`]).
pub fn migrate_store(store: &TripleStore, path: &Path) -> Result<MigrationReport, StorageError> {
    let start = Instant::now();
    if store.with_db(TripleStore::stored_format)? == Some(STORAGE_FORMAT) {
        return Ok(MigrationReport {
            already_migrated: true,
            ..Default::default()
        });
    }

    let sst_dir = path.join("migrate_v3_sst");
    clear_v3_data(store)?;
    let mut report = MigrationReport::default();

    let spo_checksum = migrate_quads(store, &sst_dir, &mut report)?;
    migrate_derived(store, &sst_dir, &mut report)?;
    migrate_annotations(store, &mut report)?;

    verify(store, &report, spo_checksum)?;

    // ── commit point ──────────────────────────────────────────────────────────
    store.with_db(|db| -> Result<(), StorageError> {
        let meta = db
            .cf_handle(cf::META)
            .ok_or_else(|| StorageError::MissingCf(cf::META.into()))?;
        db.put_cf(&meta, META_STORAGE_FORMAT, STORAGE_FORMAT.to_be_bytes())?;
        db.flush_cf(&meta)?;
        TripleStore::drop_v2_cfs(db)
    })?;
    let _ = std::fs::remove_dir_all(&sst_dir);

    report.duration_ms = start.elapsed().as_millis() as u64;
    info!(?report, "storage migrated to format 3");
    Ok(report)
}

/// Remove anything a previous, interrupted attempt wrote to v3 CFs.
fn clear_v3_data(store: &TripleStore) -> Result<(), StorageError> {
    store.with_db(|db| -> Result<(), StorageError> {
        for name in cf::ALL {
            if matches!(*name, cf::META | cf::HNSW | cf::IRI) {
                continue; // shared with v2, never rewritten
            }
            let non_empty = db
                .cf_handle(name)
                .and_then(|h| db.iterator_cf(&h, IteratorMode::Start).next())
                .is_some();
            if non_empty {
                db.drop_cf(name)?;
                db.create_cf(name, &Options::default())?;
            }
        }
        Ok(())
    })
}

/// Per-order buffers flushed as SST chunks.
struct ChunkedWriter<'a> {
    store: &'a TripleStore,
    dir: &'a Path,
    bufs: Vec<(&'static str, SortedEntries)>,
    chunk: usize,
}

impl<'a> ChunkedWriter<'a> {
    fn new(store: &'a TripleStore, dir: &'a Path, cfs: &[&'static str]) -> Self {
        Self {
            store,
            dir,
            bufs: cfs.iter().map(|c| (*c, Vec::new())).collect(),
            chunk: 0,
        }
    }

    fn push(&mut self, i: usize, key: Vec<u8>, value: Vec<u8>) -> Result<(), StorageError> {
        self.bufs[i].1.push((key, value));
        if self.bufs[i].1.len() >= CHUNK_ENTRIES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.chunk += 1;
        let dir = self.dir.join(format!("chunk_{}", self.chunk));
        for (cf_name, buf) in &mut self.bufs {
            ingest_sorted(self.store, &dir, cf_name, std::mem::take(buf))?;
        }
        Ok(())
    }
}

/// An order-independent checksum over decoded triple versions: count and
/// wrapping sum of xxh3 digests of `(s, p, o-or-value-hash, tt, vt_start, vt_end)`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Checksum {
    count: u64,
    sum: u128,
}

impl Checksum {
    fn add(&mut self, q: &QuadKey, vt: (Timestamp, Timestamp)) {
        let mut buf = Vec::with_capacity(56);
        buf.extend_from_slice(q.s.as_bytes());
        buf.extend_from_slice(&q.p.to_be_bytes());
        buf.extend_from_slice(q.o.as_bytes());
        buf.extend_from_slice(&q.tt.to_be_bytes());
        buf.extend_from_slice(&vt.0.to_be_bytes());
        buf.extend_from_slice(&vt.1.to_be_bytes());
        self.count += 1;
        self.sum = self.sum.wrapping_add(xxhash_rust::xxh3::xxh3_128(&buf));
    }
}

fn migrate_quads(
    store: &TripleStore,
    sst_dir: &Path,
    report: &mut MigrationReport,
) -> Result<Checksum, StorageError> {
    let cfs: Vec<&'static str> = Order::ALL.iter().map(|o| o.cf()).collect();
    let mut out = QuadOut {
        store,
        writer: ChunkedWriter::new(store, sst_dir, &cfs),
        side: WriteBatch::default(), // blobs + trigrams
        checksum: Checksum::default(),
    };

    store.with_db(|db| -> Result<(), StorageError> {
        let Some(spo) = db.cf_handle(cf::v2::SPO) else {
            return Ok(());
        };
        // v2 keys sort by (s, p, o) then tt, so every version of one v2
        // property — all under the sentinel object — arrives consecutively.
        let mut group: Vec<(keys::v2::Spo, Vec<u8>)> = Vec::new();
        for item in db.iterator_cf(&spo, IteratorMode::Start) {
            let (key, value) = item?;
            let k = keys::v2::decode_spo(&key)?;
            if group
                .first()
                .is_some_and(|(g, _)| (g.s, g.p, g.o) != (k.s, k.p, k.o))
            {
                out.group(std::mem::take(&mut group), report)?;
            }
            group.push((k, value.to_vec()));
        }
        out.group(group, report)
    })?;

    out.writer.flush()?;
    store.db_write(out.side)?;
    Ok(out.checksum)
}

struct QuadOut<'a> {
    store: &'a TripleStore,
    writer: ChunkedWriter<'a>,
    side: WriteBatch,
    checksum: Checksum,
}

impl QuadOut<'_> {
    fn emit(
        &mut self,
        q: &QuadKey,
        bytes: &[u8],
        report: &mut MigrationReport,
    ) -> Result<(), StorageError> {
        let vt = codec::valid_time(bytes)
            .ok_or_else(|| StorageError::KeyDecode("value without valid time".into()))?;
        self.checksum.add(q, vt);
        report.quad_versions += 1;
        for (i, order) in Order::ALL.iter().enumerate() {
            self.writer
                .push(i, order.encode(q).to_vec(), bytes.to_vec())?;
        }
        if self.side.len() >= CHUNK_ENTRIES {
            self.store.db_write(std::mem::take(&mut self.side))?;
        }
        Ok(())
    }

    /// Rewrite every version of one v2 `(s, p, o)`.
    ///
    /// A relation's versions map one-to-one. A v2 property's versions all
    /// shared the sentinel key, so a new value implicitly replaced the old
    /// one; in v3 each value has its own key, so the versions are replayed as
    /// `WriteMode::Auto` writes: an open-ended version closes the other open
    /// values at its `vt_start` (a synthesized closing version at the same
    /// `tt`), exactly as `stage_writes` does for live writes.
    fn group(
        &mut self,
        versions: Vec<(keys::v2::Spo, Vec<u8>)>,
        report: &mut MigrationReport,
    ) -> Result<(), StorageError> {
        let g = GraphId::DEFAULT;
        // Latest bytes per value hash, for the replay.
        let mut open: Vec<(polargraph_core::id::NodeId, Vec<u8>)> = Vec::new();
        for (k, value) in versions {
            match codec::decode_value(&value)? {
                DecodedValue::Relation { .. } => {
                    let q = QuadKey {
                        s: k.s,
                        p: k.p,
                        o: k.o,
                        g,
                        tt: k.tt,
                    };
                    self.emit(&q, &value, report)?;
                }
                DecodedValue::Property { value: v, temporal } => {
                    report.property_versions += 1;
                    if let Some(text) = v.as_text() {
                        self.store
                            .batch_text_trigrams(&mut self.side, &k.s, k.p, g, text)?;
                    }
                    let bytes = self
                        .store
                        .encode_property_entry(&mut self.side, &v, &temporal)?;
                    if bytes.first() == Some(&codec::DISC_PROPERTY_REF) {
                        report.values_out_of_line += 1;
                    }
                    let o = keys::value_object(&v);
                    if temporal.vt_end == Timestamp::END_OF_TIME {
                        let at = temporal.vt_start;
                        for (other, other_bytes) in open.iter_mut() {
                            let still_open =
                                codec::valid_time(other_bytes).is_some_and(|(_, end)| end > at);
                            if *other == o || !still_open {
                                continue;
                            }
                            let closed = codec::with_vt_end(other_bytes, at)?;
                            let q = QuadKey {
                                s: k.s,
                                p: k.p,
                                o: *other,
                                g,
                                tt: k.tt,
                            };
                            self.emit(&q, &closed, report)?;
                            report.closing_versions_synthesized += 1;
                            *other_bytes = closed;
                        }
                    }
                    let q = QuadKey {
                        s: k.s,
                        p: k.p,
                        o,
                        g,
                        tt: k.tt,
                    };
                    self.emit(&q, &bytes, report)?;
                    match open.iter_mut().find(|(h, _)| *h == o) {
                        Some((_, b)) => *b = bytes,
                        None => open.push((o, bytes)),
                    }
                }
                DecodedValue::PropertyRef { .. } => {
                    return Err(StorageError::KeyDecode(
                        "v2 store contains a v3 property reference".into(),
                    ))
                }
            }
        }
        Ok(())
    }
}

fn migrate_derived(
    store: &TripleStore,
    sst_dir: &Path,
    report: &mut MigrationReport,
) -> Result<(), StorageError> {
    let drv_dir = sst_dir.join("drv");
    let mut writer = ChunkedWriter::new(store, &drv_dir, &[cf::DRV]);
    store.with_db(|db| -> Result<(), StorageError> {
        let Some(drv) = db.cf_handle(cf::v2::DRV) else {
            return Ok(());
        };
        for item in db.iterator_cf(&drv, IteratorMode::Start) {
            let (key, value) = item?;
            let k = keys::v2::decode_spo(&key)?;
            let q = QuadKey {
                s: k.s,
                p: k.p,
                o: k.o,
                g: GraphId::DEFAULT,
                tt: k.tt,
            };
            writer.push(0, Order::Spog.encode(&q).to_vec(), value.to_vec())?;
            report.derived_versions += 1;
        }
        Ok(())
    })?;
    writer.flush()
}

fn migrate_annotations(
    store: &TripleStore,
    report: &mut MigrationReport,
) -> Result<(), StorageError> {
    let g = GraphId::DEFAULT;
    let mut batch = WriteBatch::default();
    store.with_db(|db| -> Result<(), StorageError> {
        if let Some(epa) = db.cf_handle(cf::v2::EPA) {
            for item in db.iterator_cf(&epa, IteratorMode::Start) {
                let (key, value) = item?;
                let (edge, p, tt) = keys::v2::decode_epa(&key)?;
                store.batch_epa(&mut batch, edge, p, g, tt, &value)?;
                report.edge_property_annotations += 1;
                if batch.len() >= CHUNK_ENTRIES {
                    store.db_write(std::mem::take(&mut batch))?;
                }
            }
        }
        if let Some(epo) = db.cf_handle(cf::v2::EPO) {
            for item in db.iterator_cf(&epo, IteratorMode::Start) {
                let (key, value) = item?;
                let (edge, p, o, tt) = keys::v2::decode_epo(&key)?;
                store.batch_epo(&mut batch, edge, p, o, g, tt, &value)?;
                report.edge_relation_annotations += 1;
                if batch.len() >= CHUNK_ENTRIES {
                    store.db_write(std::mem::take(&mut batch))?;
                }
            }
        }
        Ok(())
    })?;
    store.db_write(batch)
}

fn count_cf(store: &TripleStore, name: &str) -> Result<u64, StorageError> {
    store.with_db(|db| -> Result<u64, StorageError> {
        let Some(h) = db.cf_handle(name) else {
            return Ok(0);
        };
        let mut n = 0;
        for item in db.iterator_cf(&h, IteratorMode::Start) {
            item?;
            n += 1;
        }
        Ok(n)
    })
}

/// Check the rewritten data before the commit point.
fn verify(
    store: &TripleStore,
    report: &MigrationReport,
    expected: Checksum,
) -> Result<(), StorageError> {
    let fail =
        |what: String| StorageError::Validation(format!("migration verification failed: {what}"));

    for order in Order::ALL {
        let n = count_cf(store, order.cf())?;
        if n != report.quad_versions {
            return Err(fail(format!(
                "{} has {n} entries, expected {}",
                order.cf(),
                report.quad_versions
            )));
        }
    }

    let mut actual = Checksum::default();
    store.with_db(|db| -> Result<(), StorageError> {
        let h = db
            .cf_handle(Order::Spog.cf())
            .ok_or_else(|| StorageError::MissingCf(Order::Spog.cf().into()))?;
        for item in db.iterator_cf(&h, IteratorMode::Start) {
            let (key, value) = item?;
            let q = Order::Spog.decode(&key)?;
            let vt = codec::valid_time(&value)
                .ok_or_else(|| StorageError::KeyDecode("value without valid time".into()))?;
            actual.add(&q, vt);
        }
        Ok(())
    })?;
    if actual != expected {
        return Err(fail(format!(
            "spog checksum {actual:?} != v2 spo checksum {expected:?}"
        )));
    }

    let checks = [
        (cf::DRV, report.derived_versions),
        (cf::EPA, report.edge_property_annotations),
        (cf::PEA, report.edge_property_annotations),
        (cf::EPO, report.edge_relation_annotations),
    ];
    for (name, expected) in checks {
        let n = count_cf(store, name)?;
        if n != expected {
            return Err(fail(format!("{name} has {n} entries, expected {expected}")));
        }
    }
    Ok(())
}
