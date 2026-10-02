//! Runtime vocabulary: base IRI and prefixes, no restart.

use polargraph_storage::{TripleStore, DEFAULT_VOCAB_BASE};
use tempfile::TempDir;

#[test]
fn vocabulary_changes_apply_immediately_and_persist() {
    let dir = TempDir::new().unwrap();
    {
        let store = TripleStore::open(dir.path()).unwrap();
        assert_eq!(store.vocabulary().base, DEFAULT_VOCAB_BASE);

        store.put_prefix("ex", "http://ex/").unwrap();
        assert_eq!(store.vocabulary().expand("ex:Widget"), "http://ex/Widget");
        store
            .set_vocabulary_base("https://kb.example/vocab/")
            .unwrap();
        assert_eq!(
            store.vocabulary().expand("Widget"),
            "https://kb.example/vocab/Widget"
        );

        store.put_prefix("ex", "http://example.org/").unwrap(); // re-point
        assert_eq!(store.vocabulary().expand("ex:W"), "http://example.org/W");
        store.put_prefix("tmp", "urn:tmp:").unwrap();
        assert!(store.remove_prefix("tmp").unwrap());
        assert!(!store.remove_prefix("tmp").unwrap());

        assert!(store.put_prefix("1bad", "http://x/").is_err());
        assert!(store.put_prefix("ok", "not an iri").is_err());
        assert!(store.set_vocabulary_base("relative").is_err());
    }
    // Survives a reopen (stored in the system graph).
    let store = TripleStore::open(dir.path()).unwrap();
    let v = store.vocabulary();
    assert_eq!(v.base, "https://kb.example/vocab/");
    assert_eq!(
        v.prefixes.get("ex").map(String::as_str),
        Some("http://example.org/")
    );
    assert!(!v.prefixes.contains_key("tmp"));
}

mod legacy {
    use polargraph_core::{
        id::{EdgeId, NodeId},
        temporal::{BiTemporalRange, Timestamp},
        term::iri_to_node_id,
        triple::{Predicate, Triple},
        value::Value,
    };
    use polargraph_storage::{legacy::RDF_TYPE, TripleStore};
    use tempfile::TempDir;

    fn prop(s: NodeId, p: &str, v: &str) -> Triple {
        Triple::Property {
            subject: s,
            predicate: Predicate::new(p),
            value: Value::Text(v.into()),
            temporal: BiTemporalRange::assert_now(Timestamp(0)),
        }
    }

    fn rel(s: NodeId, p: &str, o: NodeId) -> Triple {
        Triple::Relation {
            subject: s,
            predicate: Predicate::new(p),
            object: o,
            edge_id: EdgeId::new(),
            temporal: BiTemporalRange::assert_now(Timestamp(0)),
        }
    }

    #[test]
    fn conversion_is_explicit_idempotent_and_complete() {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let (alice, bob) = (NodeId::new(), NodeId::new());

        // A store written before the vocabulary: bare names and __type labels.
        store.set_canonical_predicate_names(false);
        let mut tx = store.begin();
        tx.insert(prop(alice, "__type", "Person"));
        tx.insert(prop(alice, "name", "Alice"));
        tx.insert(rel(alice, "KNOWS", bob));
        tx.insert(prop(bob, "__type", "ex:Robot"));
        tx.commit().unwrap();
        store.set_canonical_predicate_names(true);

        let status = store.legacy_status().unwrap();
        assert!(status.pending());
        assert_eq!(
            status.bare_predicates,
            vec!["KNOWS".to_string(), "name".to_string()]
        );
        assert_eq!(status.type_labels, 2);

        // The window: by bare name, legacy data isn't reachable.
        let snap = store.snapshot(Timestamp(store.oracle_ts()));
        assert!(snap
            .scan_by_subject_predicate(&alice, "name")
            .unwrap()
            .is_empty());

        store.set_vocabulary_base("https://kb.example/v/").unwrap();
        store.put_prefix("ex", "http://ex/").unwrap();

        let dry = store.convert_legacy(true).unwrap();
        assert!(dry.dry_run);
        assert_eq!(dry.renamed.len(), 2);
        assert_eq!(dry.labels_converted, 2);
        assert!(
            store.legacy_status().unwrap().pending(),
            "dry run changes nothing"
        );

        let report = store.convert_legacy(false).unwrap();
        assert_eq!(report.labels_converted, 2);
        assert!(report
            .renamed
            .contains(&("name".into(), "https://kb.example/v/name".into())));
        assert!(!store.legacy_status().unwrap().pending());

        let snap = store.snapshot(Timestamp(store.oracle_ts()));
        // Bare names now resolve under the base and reach the renamed data.
        assert_eq!(
            snap.scan_by_subject_predicate(&alice, "name")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            snap.scan_by_subject_predicate(&alice, "KNOWS")
                .unwrap()
                .len(),
            1
        );
        // Labels are rdf:type relations to class IRIs.
        let types = |n| {
            snap.scan_by_subject_predicate(&n, RDF_TYPE)
                .unwrap()
                .into_iter()
                .filter_map(|t| match t {
                    Triple::Relation { object, .. } => Some(object),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            types(alice),
            vec![iri_to_node_id("https://kb.example/v/Person")]
        );
        assert_eq!(types(bob), vec![iri_to_node_id("http://ex/Robot")]);
        assert_eq!(
            store
                .iri_of(&iri_to_node_id("http://ex/Robot"))
                .unwrap()
                .as_deref(),
            Some("http://ex/Robot")
        );
        assert!(snap.scan_by_predicate("__type").unwrap().is_empty());

        // Idempotent.
        let again = store.convert_legacy(false).unwrap();
        assert!(again.renamed.is_empty() && again.merged.is_empty() && again.labels_converted == 0);
    }

    #[test]
    fn a_name_whose_iri_already_exists_is_merged() {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let (a, b) = (NodeId::new(), NodeId::new());
        store.set_canonical_predicate_names(false);
        let mut tx = store.begin();
        tx.insert(prop(a, "name", "old"));
        tx.commit().unwrap();
        store.set_canonical_predicate_names(true);
        // New-style write of the same name interns the IRI separately.
        let mut tx = store.begin();
        tx.insert(prop(b, "name", "new"));
        tx.commit().unwrap();

        let report = store.convert_legacy(false).unwrap();
        assert_eq!(
            report.merged,
            vec![("name".into(), "urn:pg:vocab:name".into(), 1)]
        );
        assert!(!store.legacy_status().unwrap().pending());
        let snap = store.snapshot(Timestamp(store.oracle_ts()));
        assert_eq!(
            snap.scan_by_predicate("name").unwrap().len(),
            2,
            "both under the IRI"
        );
        assert_eq!(
            snap.scan_by_predicate("__legacy__/name").unwrap().len(),
            0,
            "moved, history kept under __legacy__/name"
        );
    }
}
