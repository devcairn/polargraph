use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_storage::{SstImporter, TripleStore};
use tempfile::TempDir;

fn node(seed: u128) -> NodeId {
    NodeId(uuid::Uuid::from_bytes(seed.to_le_bytes()))
}

fn edge(seed: u128) -> EdgeId {
    EdgeId(uuid::Uuid::from_bytes(seed.to_le_bytes()))
}

fn make_relation(i: u64) -> Triple {
    Triple::Relation {
        subject: node(i as u128),
        predicate: Predicate::new("knows"),
        object: node((i as u128) | (1 << 64)),
        edge_id: edge((i as u128) | (2 << 64)),
        temporal: BiTemporalRange {
            vt_start: Timestamp(0),
            vt_end: Timestamp::END_OF_TIME,
            tt: Timestamp(0), // overwritten by finish()
        },
    }
}

fn make_property(i: u64) -> Triple {
    Triple::Property {
        subject: node(i as u128),
        predicate: Predicate::new("label"),
        value: Value::Text(format!("node-{i}")),
        temporal: BiTemporalRange {
            vt_start: Timestamp(0),
            vt_end: Timestamp::END_OF_TIME,
            tt: Timestamp(0),
        },
    }
}

#[test]
fn import_10k_relation_triples_queryable() {
    let data_dir = TempDir::new().unwrap();
    let sst_dir = TempDir::new().unwrap();

    let store = TripleStore::open(data_dir.path()).unwrap();
    let mut importer = SstImporter::new(sst_dir.path()).unwrap();

    for i in 0..10_000u64 {
        importer.add_triple(&make_relation(i));
    }

    let stats = importer.finish(&store).unwrap();
    assert_eq!(stats.triples_imported, 10_000);

    let all = store.scan_all().unwrap();
    assert_eq!(
        all.len(),
        10_000,
        "all 10k imported triples should be visible"
    );
}

#[test]
fn import_zero_triples_returns_zero_stats() {
    let data_dir = TempDir::new().unwrap();
    let sst_dir = TempDir::new().unwrap();

    let store = TripleStore::open(data_dir.path()).unwrap();
    let importer = SstImporter::new(sst_dir.path()).unwrap();

    let stats = importer.finish(&store).unwrap();
    assert_eq!(stats.triples_imported, 0);
    assert_eq!(stats.duration_ms, 0);
}

#[test]
fn import_property_triples_queryable() {
    let data_dir = TempDir::new().unwrap();
    let sst_dir = TempDir::new().unwrap();

    let store = TripleStore::open(data_dir.path()).unwrap();
    let mut importer = SstImporter::new(sst_dir.path()).unwrap();

    for i in 0..100u64 {
        importer.add_triple(&make_property(i));
    }

    let stats = importer.finish(&store).unwrap();
    assert_eq!(stats.triples_imported, 100);

    let all = store.scan_all().unwrap();
    assert_eq!(all.len(), 100);
}

#[test]
fn import_mixed_triples_queryable_by_subject() {
    let data_dir = TempDir::new().unwrap();
    let sst_dir = TempDir::new().unwrap();

    let store = TripleStore::open(data_dir.path()).unwrap();
    let mut importer = SstImporter::new(sst_dir.path()).unwrap();

    let subject = node(0xDEAD_BEEF);
    let rel = Triple::Relation {
        subject,
        predicate: Predicate::new("follows"),
        object: node(0xCAFE_BABE),
        edge_id: edge(0x1234_5678),
        temporal: BiTemporalRange {
            vt_start: Timestamp(0),
            vt_end: Timestamp::END_OF_TIME,
            tt: Timestamp(0),
        },
    };
    let prop = Triple::Property {
        subject,
        predicate: Predicate::new("name"),
        value: Value::Text("Alice".into()),
        temporal: BiTemporalRange {
            vt_start: Timestamp(0),
            vt_end: Timestamp::END_OF_TIME,
            tt: Timestamp(0),
        },
    };

    importer.add_triple(&rel);
    importer.add_triple(&prop);
    importer.finish(&store).unwrap();

    let triples = store.scan_by_subject(&subject).unwrap();
    assert_eq!(triples.len(), 2);
}

#[test]
fn import_two_batches_both_visible() {
    let data_dir = TempDir::new().unwrap();
    let store = TripleStore::open(data_dir.path()).unwrap();

    // Batch 1
    {
        let sst_dir = TempDir::new().unwrap();
        let mut importer = SstImporter::new(sst_dir.path()).unwrap();
        for i in 0..500u64 {
            importer.add_triple(&make_relation(i));
        }
        importer.finish(&store).unwrap();
    }

    // Batch 2
    {
        let sst_dir = TempDir::new().unwrap();
        let mut importer = SstImporter::new(sst_dir.path()).unwrap();
        for i in 500..1000u64 {
            importer.add_triple(&make_relation(i));
        }
        importer.finish(&store).unwrap();
    }

    let all = store.scan_all().unwrap();
    assert_eq!(
        all.len(),
        1_000,
        "both batches should be visible after sequential imports"
    );
}

#[test]
fn import_then_normal_insert_both_visible() {
    let data_dir = TempDir::new().unwrap();
    let sst_dir = TempDir::new().unwrap();

    let store = TripleStore::open(data_dir.path()).unwrap();

    // Bulk-import 100 triples
    let mut importer = SstImporter::new(sst_dir.path()).unwrap();
    for i in 0..100u64 {
        importer.add_triple(&make_relation(i));
    }
    importer.finish(&store).unwrap();

    // Add one more triple via the normal MVCC path
    let extra = Triple::Relation {
        subject: node(9999),
        predicate: Predicate::new("knows"),
        object: node(8888),
        edge_id: edge(7777),
        temporal: BiTemporalRange {
            vt_start: Timestamp(0),
            vt_end: Timestamp::END_OF_TIME,
            tt: Timestamp(0),
        },
    };
    store.insert(&extra).unwrap();

    let all = store.scan_all().unwrap();
    assert_eq!(all.len(), 101);
}
