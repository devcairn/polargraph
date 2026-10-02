//! The store's vocabulary: a base IRI for bare names and a prefix map
//! (`docs/design/cypher-rdf.md` §2.1, §2.5).
//!
//! Both live as properties of `urn:pg:vocab` in the system graph and change
//! at runtime (no restart): [`TripleStore::set_vocabulary_base`],
//! [`TripleStore::put_prefix`], [`TripleStore::remove_prefix`]. The
//! in-memory copy is swapped atomically after each change.
//!
//! Predicates written with a **bare name** (no `:`), e.g. `name`, are stored
//! under the base IRI (`urn:pg:vocab:name`) — so exports and SPARQL see real
//! IRIs. Internal predicates (`__…` and the built-in access-control names)
//! stay as they are. Prefixed names (`ex:Person`) are expanded only by
//! callers that ask for it ([`Vocabulary::expand`]), never implicitly at the
//! storage layer: a string like `cb:status` is a valid IRI in its own right.

use std::{collections::BTreeMap, sync::Arc};

use polargraph_core::{
    schema::{
        BUILTIN_GRAPH_ACCESS_LEVEL_PRED, BUILTIN_HAS_ACCESS_PRED, BUILTIN_HAS_ACCESS_TYPE_PRED,
        BUILTIN_HAS_GRAPH_ACCESS_PRED, BUILTIN_MEMBER_OF_PRED,
    },
    temporal::{BiTemporalRange, Timestamp},
    term::iri_to_node_id,
    triple::{Predicate, Triple},
    value::Value,
};

use crate::{error::StorageError, mvcc::WriteMode, store::TripleStore};

/// The vocabulary base a new store starts with.
pub const DEFAULT_VOCAB_BASE: &str = "urn:pg:vocab:";
/// The system-graph node holding the vocabulary.
pub const VOCAB_NODE: &str = "urn:pg:vocab";
/// Property of [`VOCAB_NODE`]: the base IRI.
pub const VOCAB_BASE_PRED: &str = "urn:pg:vocabBase";
/// Property prefix of [`VOCAB_NODE`]: `urn:pg:prefix:<name>` → namespace IRI.
pub const VOCAB_PREFIX_PRED: &str = "urn:pg:prefix:";

/// Predicates that keep their exact name (internal bookkeeping).
const BUILTIN_PREDICATES: [&str; 5] = [
    BUILTIN_MEMBER_OF_PRED,
    BUILTIN_HAS_ACCESS_PRED,
    BUILTIN_HAS_ACCESS_TYPE_PRED,
    BUILTIN_HAS_GRAPH_ACCESS_PRED,
    BUILTIN_GRAPH_ACCESS_LEVEL_PRED,
];

/// Base IRI + prefix map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Vocabulary {
    pub base: String,
    /// Prefix name → namespace IRI.
    pub prefixes: BTreeMap<String, String>,
}

impl Default for Vocabulary {
    fn default() -> Self {
        Self {
            base: DEFAULT_VOCAB_BASE.to_string(),
            prefixes: BTreeMap::new(),
        }
    }
}

impl Vocabulary {
    /// Whether `name` is an internal predicate kept verbatim.
    pub fn is_internal(name: &str) -> bool {
        name.starts_with("__") || BUILTIN_PREDICATES.contains(&name)
    }

    /// Whether `name` is bare (no `:`), i.e. relative to the base.
    pub fn is_bare(name: &str) -> bool {
        !name.is_empty() && !name.contains(':') && !Self::is_internal(name)
    }

    /// The stored form of a predicate name: bare names under the base,
    /// everything else unchanged.
    pub fn canonical_predicate<'a>(&self, name: &'a str) -> std::borrow::Cow<'a, str> {
        if Self::is_bare(name) {
            std::borrow::Cow::Owned(format!("{}{name}", self.base))
        } else {
            std::borrow::Cow::Borrowed(name)
        }
    }

    /// A name as an IRI: bare → base, `prefix:local` with a declared prefix →
    /// namespace + local, anything else (a full IRI, an internal name)
    /// unchanged.
    pub fn expand(&self, name: &str) -> String {
        if Self::is_bare(name) {
            return format!("{}{name}", self.base);
        }
        if let Some((prefix, local)) = name.split_once(':') {
            if let Some(ns) = self.prefixes.get(prefix) {
                if !local.starts_with("//") {
                    return format!("{ns}{local}");
                }
            }
        }
        name.to_string()
    }

    /// An IRI in its shortest form: under the base → bare name, under a
    /// prefix → `prefix:local` (longest namespace wins), else unchanged.
    pub fn compact(&self, iri: &str) -> String {
        if let Some(local) = iri.strip_prefix(self.base.as_str()) {
            if !local.is_empty() && !local.contains(':') && !local.contains('/') {
                return local.to_string();
            }
        }
        let best = self
            .prefixes
            .iter()
            .filter(|(_, ns)| iri.starts_with(ns.as_str()) && iri.len() > ns.len())
            .max_by_key(|(_, ns)| ns.len());
        match best {
            Some((prefix, ns)) => format!("{prefix}:{}", &iri[ns.len()..]),
            None => iri.to_string(),
        }
    }
}

fn validate_iri(what: &str, iri: &str) -> Result<(), StorageError> {
    if iri.is_empty() || !iri.contains(':') || iri.contains(char::is_whitespace) {
        return Err(StorageError::Validation(format!(
            "{what} must be an absolute IRI, got {iri:?}"
        )));
    }
    Ok(())
}

fn validate_prefix(name: &str) -> Result<(), StorageError> {
    let mut chars = name.chars();
    let ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !ok {
        return Err(StorageError::Validation(format!(
            "prefix names start with a letter and use letters, digits, _ or -: {name:?}"
        )));
    }
    Ok(())
}

impl TripleStore {
    /// The current vocabulary.
    pub fn vocabulary(&self) -> Arc<Vocabulary> {
        Arc::clone(&self.vocab_cell().read().unwrap())
    }

    /// Set the base IRI for bare names. Affects names resolved from now on;
    /// predicates already stored keep their IRIs.
    pub fn set_vocabulary_base(&self, base: &str) -> Result<(), StorageError> {
        validate_iri("vocabulary base", base)?;
        self.write_vocab_property(VOCAB_BASE_PRED, Some(base))
    }

    /// Declare (or re-point) a prefix.
    pub fn put_prefix(&self, name: &str, namespace: &str) -> Result<(), StorageError> {
        validate_prefix(name)?;
        validate_iri("prefix namespace", namespace)?;
        self.write_vocab_property(&format!("{VOCAB_PREFIX_PRED}{name}"), Some(namespace))
    }

    /// Remove a prefix. Returns whether it existed.
    pub fn remove_prefix(&self, name: &str) -> Result<bool, StorageError> {
        if !self.vocabulary().prefixes.contains_key(name) {
            return Ok(false);
        }
        self.write_vocab_property(&format!("{VOCAB_PREFIX_PRED}{name}"), None)?;
        Ok(true)
    }

    /// Replace (`Some`) or close (`None`) a vocabulary property, then reload.
    fn write_vocab_property(
        &self,
        predicate: &str,
        value: Option<&str>,
    ) -> Result<(), StorageError> {
        if self.is_replica() {
            return Err(StorageError::ReadOnly(
                "vocabulary changes are made on the primary".into(),
            ));
        }
        let meta = self.system_graph()?;
        let node = iri_to_node_id(VOCAB_NODE);
        let now = Timestamp::now();
        let mut tx = self.begin();
        tx.bind_iri(VOCAB_NODE);
        match value {
            Some(v) => tx.insert_in(
                Triple::Property {
                    subject: node,
                    predicate: Predicate::new(predicate),
                    value: Value::Text(v.to_string()),
                    temporal: BiTemporalRange::assert_now(now),
                },
                meta,
                WriteMode::Replace,
            ),
            None => {
                let snap = self.snapshot(Timestamp(self.oracle_ts()));
                for t in snap.scan_by_subject_in_graph(meta, &node)? {
                    if t.predicate().0 == predicate {
                        tx.insert_in(crate::graphs::close_at(t, now), meta, WriteMode::Add);
                    }
                }
            }
        }
        tx.commit()?;
        self.reload_vocabulary()
    }

    /// Re-read the vocabulary from the system graph (after a local change,
    /// or a replicated batch).
    pub fn reload_vocabulary(&self) -> Result<(), StorageError> {
        let mut vocab = Vocabulary::default();
        if let Some(meta) = self.graph_id(crate::SYSTEM_GRAPH_IRI) {
            let snap = self.snapshot(Timestamp(self.oracle_ts()));
            for t in snap.scan_by_subject_in_graph(meta, &iri_to_node_id(VOCAB_NODE))? {
                let Triple::Property {
                    predicate,
                    value: Value::Text(v),
                    ..
                } = t
                else {
                    continue;
                };
                if predicate.0 == VOCAB_BASE_PRED {
                    vocab.base = v;
                } else if let Some(name) = predicate.0.strip_prefix(VOCAB_PREFIX_PRED) {
                    vocab.prefixes.insert(name.to_string(), v);
                }
            }
        }
        *self.vocab_cell().write().unwrap() = Arc::new(vocab);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_and_compact() {
        let mut v = Vocabulary::default();
        v.prefixes.insert("ex".into(), "http://ex/".into());
        v.prefixes.insert("exn".into(), "http://ex/ns/".into());
        assert_eq!(v.expand("Person"), "urn:pg:vocab:Person");
        assert_eq!(v.expand("ex:Person"), "http://ex/Person");
        assert_eq!(v.expand("http://other/X"), "http://other/X");
        assert_eq!(
            v.expand("cb:status"),
            "cb:status",
            "unknown prefix stays an IRI"
        );
        assert_eq!(v.expand("MEMBER_OF"), "MEMBER_OF", "internal");
        assert_eq!(v.expand("__type"), "__type");
        assert_eq!(v.compact("urn:pg:vocab:Person"), "Person");
        assert_eq!(v.compact("http://ex/Person"), "ex:Person");
        assert_eq!(
            v.compact("http://ex/ns/Thing"),
            "exn:Thing",
            "longest namespace"
        );
        assert_eq!(v.compact("http://other/X"), "http://other/X");
        assert_eq!(v.canonical_predicate("name"), "urn:pg:vocab:name");
        assert_eq!(
            v.canonical_predicate("ex:name"),
            "ex:name",
            "storage never expands prefixes"
        );
    }
}
