//! Offline migration from storage format v2 to v3.
//!
//! The fixture is written byte-for-byte in the v2 layout (44-byte keys,
//! property sentinel, no graph slot) straight into RocksDB, the way a v2
//! build left it.

use std::path::Path;

use polargraph_core::{
    id::{EdgeId, GraphId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::Triple,
    value::Value,
};
use polargraph_storage::{
    cf, codec,
    keys::v2,
    migrate_v3::{self, open_for_migration},
    StorageError, TripleStore,
};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tempfile::TempDir;
use uuid::Uuid;

const KNOWS: u32 = 1;
const NAME: u32 = 2;
const BIO: u32 = 3;
const CONFIDENCE: u32 = 4;
const SOURCE: u32 = 5;
const INFERRED: u32 = 6;

struct Fixture {
    alice: NodeId,
    bob: NodeId,
    edge: EdgeId,
    doc: NodeId,
    bio: String,
}

fn t(vt_start: i64, vt_end: i64) -> BiTemporalRange {
    BiTemporalRange {
        vt_start: Timestamp(vt_start),
        vt_end: Timestamp(vt_end),
        tt: Timestamp(0),
    }
}

/// Write a v2 store at `path`.
fn write_v2_store(path: &Path) -> Fixture {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let names = ["meta", "hnsw", "iri"]
        .into_iter()
        .chain(cf::v2::ALL.iter().copied());
    let cfs: Vec<_> = names
        .map(|n| ColumnFamilyDescriptor::new(n, Options::default()))
        .collect();
    let db = DB::open_cf_descriptors(&opts, path, cfs).unwrap();

    let meta = db.cf_handle("meta").unwrap();
    for (name, id) in [
        ("knows", KNOWS),
        ("name", NAME),
        ("bio", BIO),
        ("confidence", CONFIDENCE),
        ("source", SOURCE),
        ("inferred", INFERRED),
    ] {
        db.put_cf(
            &meta,
            [b"p/".as_slice(), name.as_bytes()].concat(),
            id.to_be_bytes(),
        )
        .unwrap();
        db.put_cf(
            &meta,
            [b"pid/".as_slice(), &id.to_be_bytes()].concat(),
            name,
        )
        .unwrap();
    }
    db.put_cf(&meta, b"__pred_ctr", 7u32.to_be_bytes()).unwrap();
    db.put_cf(&meta, b"__oracle_ctr", 100i64.to_be_bytes())
        .unwrap();

    let f = Fixture {
        alice: NodeId(Uuid::from_bytes([1; 16])),
        bob: NodeId(Uuid::from_bytes([2; 16])),
        edge: EdgeId(Uuid::from_bytes([9; 16])),
        doc: NodeId(Uuid::from_bytes([3; 16])),
        bio: "Alice writes long bios. ".repeat(40),
    };
    let sentinel = NodeId(Uuid::from_bytes(v2::PROPERTY_SENTINEL));
    let spo = db.cf_handle("spo").unwrap();
    let put_spo = |s: &NodeId, p: u32, o: &NodeId, tt: i64, v: Vec<u8>| {
        db.put_cf(&spo, v2::encode_spo(s, p, o, Timestamp(tt)), v)
            .unwrap();
    };
    // Relation.
    put_spo(
        &f.alice,
        KNOWS,
        &f.bob,
        10,
        codec::encode_relation(&f.edge, &t(0, i64::MAX)),
    );
    // A property corrected once: v2 kept both versions under one key.
    put_spo(
        &f.alice,
        NAME,
        &sentinel,
        10,
        codec::encode_property(&Value::Text("Alicia".into()), &t(0, i64::MAX)).unwrap(),
    );
    put_spo(
        &f.alice,
        NAME,
        &sentinel,
        20,
        codec::encode_property(&Value::Text("Alice".into()), &t(0, i64::MAX)).unwrap(),
    );
    // A large value (goes out of line in v3) and a deleted fact.
    put_spo(
        &f.alice,
        BIO,
        &sentinel,
        10,
        codec::encode_property(&Value::Text(f.bio.clone()), &t(0, i64::MAX)).unwrap(),
    );
    put_spo(
        &f.bob,
        NAME,
        &sentinel,
        10,
        codec::encode_property(&Value::Text("Bob".into()), &t(0, i64::MAX)).unwrap(),
    );
    put_spo(
        &f.bob,
        NAME,
        &sentinel,
        30,
        codec::encode_property(&Value::Text("Bob".into()), &t(0, 30)).unwrap(),
    );
    // A vector and a language-tagged label.
    put_spo(
        &f.doc,
        NAME,
        &sentinel,
        10,
        codec::encode_property(&Value::Vector(vec![0.25; 128]), &t(0, i64::MAX)).unwrap(),
    );
    put_spo(
        &f.doc,
        SOURCE,
        &sentinel,
        10,
        codec::encode_property(
            &Value::LangText {
                text: "Rapport".into(),
                lang: "fr".into(),
            },
            &t(0, i64::MAX),
        )
        .unwrap(),
    );

    // Derived fact and RDF-star annotations.
    let drv = db.cf_handle("drv").unwrap();
    db.put_cf(
        &drv,
        v2::encode_spo(&f.bob, INFERRED, &f.alice, Timestamp(40)),
        codec::encode_relation(&EdgeId(Uuid::nil()), &t(0, i64::MAX)),
    )
    .unwrap();
    let epa = db.cf_handle("epa").unwrap();
    db.put_cf(
        &epa,
        v2::encode_epa(&f.edge, CONFIDENCE, Timestamp(10)),
        codec::encode_property(&Value::Float(0.8), &t(0, i64::MAX)).unwrap(),
    )
    .unwrap();
    let epo = db.cf_handle("epo").unwrap();
    db.put_cf(
        &epo,
        v2::encode_epo(&f.edge, SOURCE, &f.doc, Timestamp(10)),
        [0i64.to_be_bytes(), i64::MAX.to_be_bytes()].concat(),
    )
    .unwrap();
    f
}

fn prop_values(store: &TripleStore, s: &NodeId, p: &str) -> Vec<Value> {
    store
        .scan_by_subject_predicate(s, p)
        .unwrap()
        .into_iter()
        .filter_map(|t| match t {
            Triple::Property { value, .. } => Some(value),
            _ => None,
        })
        .collect()
}

#[test]
fn v2_store_is_refused_until_migrated_then_reads_the_same() {
    let dir = TempDir::new().unwrap();
    let f = write_v2_store(dir.path());

    assert!(matches!(
        TripleStore::open(dir.path()),
        Err(StorageError::NeedsMigration)
    ));

    let report = migrate_v3::migrate(dir.path()).unwrap();
    assert!(!report.already_migrated);
    // 8 v2 versions + 1 synthesized: "Alice" (tt 20) closes "Alicia".
    assert_eq!(report.quad_versions, 9);
    assert_eq!(report.closing_versions_synthesized, 1);
    assert_eq!(report.property_versions, 7);
    assert_eq!(report.values_out_of_line, 2, "the bio and the vector");
    assert_eq!(report.derived_versions, 1);
    assert_eq!(report.edge_property_annotations, 1);
    assert_eq!(report.edge_relation_annotations, 1);

    let store = TripleStore::open(dir.path()).unwrap();

    // Relations and current property values.
    let knows = store.scan_by_subject_predicate(&f.alice, "knows").unwrap();
    assert!(
        matches!(&knows[..], [Triple::Relation { object, edge_id, .. }]
        if *object == f.bob && *edge_id == f.edge)
    );
    assert_eq!(
        prop_values(&store, &f.alice, "name"),
        vec![Value::Text("Alice".into())]
    );
    assert_eq!(
        prop_values(&store, &f.alice, "bio"),
        vec![Value::Text(f.bio.clone())]
    );
    assert!(
        prop_values(&store, &f.bob, "name").is_empty(),
        "deleted stays deleted"
    );
    assert_eq!(
        prop_values(&store, &f.doc, "name"),
        vec![Value::Vector(vec![0.25; 128])]
    );

    // As of tx time 15 the old name still holds.
    let before: Vec<Value> = store
        .snapshot(Timestamp(15))
        .scan_by_subject_predicate(&f.alice, "name")
        .unwrap()
        .into_iter()
        .filter_map(|t| match t {
            Triple::Property { value, .. } => Some(value),
            _ => None,
        })
        .collect();
    assert_eq!(before, vec![Value::Text("Alicia".into())]);

    // History keeps both versions of the corrected name.
    let history = store.scan_property_history(f.alice, "name", 10).unwrap();
    assert_eq!(
        history,
        vec![
            (Value::Text("Alice".into()), 20),
            (Value::Text("Alicia".into()), 10)
        ]
    );

    // Trigrams were rebuilt; annotations and derived facts carried over.
    let ts = Timestamp(store.oracle_ts());
    assert_eq!(
        store.text_search("source", "Rapp", ts, None).unwrap(),
        vec![f.doc]
    );
    assert_eq!(store.scan_edge_annotations(f.edge, ts).unwrap().len(), 2);
    assert_eq!(store.scan_derived().unwrap().len(), 1);
    // Live now: knows, Alice's name and bio, the doc's vector and source.
    assert_eq!(
        store
            .snapshot(ts)
            .scan_graph(GraphId::DEFAULT)
            .unwrap()
            .len(),
        5
    );

    // v2 CFs are gone; migrating again is a no-op.
    let cfs = DB::list_cf(&Options::default(), dir.path()).unwrap();
    assert!(!cfs.contains(&"spo".to_string()));
    drop(store);
    assert!(migrate_v3::migrate(dir.path()).unwrap().already_migrated);
}

#[test]
fn interrupted_migration_is_rerun_from_scratch() {
    let dir = TempDir::new().unwrap();
    let f = write_v2_store(dir.path());

    // Simulate a crash mid-migration: junk in a v3 CF, no format recorded.
    {
        let store = open_for_migration(dir.path()).unwrap();
        drop(store);
        let mut opts = Options::default();
        opts.create_missing_column_families(true);
        let names = DB::list_cf(&opts, dir.path()).unwrap();
        let db = DB::open_cf(&opts, dir.path(), &names).unwrap();
        let spog = db.cf_handle("spog").unwrap();
        db.put_cf(&spog, [0xAAu8; 48], [1u8, 2, 3]).unwrap();
    }
    assert!(matches!(
        TripleStore::open(dir.path()),
        Err(StorageError::NeedsMigration)
    ));

    let report = migrate_v3::migrate(dir.path()).unwrap();
    assert_eq!(report.quad_versions, 9, "junk was cleared, not counted");
    let store = TripleStore::open(dir.path()).unwrap();
    assert_eq!(
        prop_values(&store, &f.alice, "name"),
        vec![Value::Text("Alice".into())]
    );
}

#[test]
fn empty_v2_cfs_are_treated_as_a_new_store() {
    let dir = TempDir::new().unwrap();
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs: Vec<_> = ["meta"]
            .into_iter()
            .chain(cf::v2::ALL.iter().copied())
            .map(|n| ColumnFamilyDescriptor::new(n, Options::default()))
            .collect();
        DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap();
    }
    TripleStore::open(dir.path()).unwrap();
    let cfs = DB::list_cf(&Options::default(), dir.path()).unwrap();
    assert!(!cfs.contains(&"spo".to_string()));
}
