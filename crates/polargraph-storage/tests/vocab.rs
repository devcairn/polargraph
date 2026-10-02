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
