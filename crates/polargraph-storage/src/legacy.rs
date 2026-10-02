//! One-time conversion of pre-vocabulary data (`docs/design/cypher-rdf.md`
//! §2.4, decision G2): bare predicate names → IRIs under the vocabulary
//! base, and `__type` label properties → `rdf:type` relations.
//!
//! Operator-triggered ([`TripleStore::convert_legacy`]), online,
//! idempotent and resumable: every step works from what is still legacy, in
//! chunked commits, so re-running continues an interrupted conversion and is
//! a no-op once nothing is left.

use polargraph_core::{
    temporal::Timestamp,
    term,
    triple::{Predicate, Triple},
    value::Value,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::StorageError,
    graphs::close_at,
    mvcc::WriteMode,
    store::{GraphScope, TripleStore},
    vocab::Vocabulary,
};

/// The legacy label predicate.
pub const LEGACY_TYPE_PRED: &str = "__type";
/// `rdf:type`.
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
/// Prefix for a bare predicate whose IRI form already existed: its entry is
/// renamed to `__legacy__/<name>` and its live quads are moved to the IRI.
pub const LEGACY_MERGE_PREFIX: &str = "__legacy__/";

/// Quads per commit.
const CHUNK: usize = 10_000;

/// What is still legacy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyStatus {
    /// Interned bare predicate names (not yet under the vocabulary base).
    pub bare_predicates: Vec<String>,
    /// `__legacy__/…` predicates that still have live quads to move.
    pub pending_merges: Vec<String>,
    /// Live `__type` label properties.
    pub type_labels: u64,
}

impl LegacyStatus {
    pub fn pending(&self) -> bool {
        !self.bare_predicates.is_empty() || !self.pending_merges.is_empty() || self.type_labels > 0
    }
}

/// What a conversion did (or, with `dry_run`, would do).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversionReport {
    pub dry_run: bool,
    /// `(bare name, IRI)` renamed in place.
    pub renamed: Vec<(String, String)>,
    /// `(bare name, IRI, live quads moved)` where the IRI already existed.
    pub merged: Vec<(String, String, u64)>,
    /// `__type` labels converted to `rdf:type`.
    pub labels_converted: u64,
}

impl TripleStore {
    /// What legacy data remains. Counts every live `__type` property (a
    /// full predicate scan) — call it at startup / after a conversion, not
    /// per request.
    pub fn legacy_status(&self) -> Result<LegacyStatus, StorageError> {
        let names = self.interned_predicates();
        let snap = self.snapshot(Timestamp(self.oracle_ts()));
        let mut pending_merges = Vec::new();
        for name in names.iter().filter(|n| n.starts_with(LEGACY_MERGE_PREFIX)) {
            if !snap.scan_by_predicate(name)?.is_empty() {
                pending_merges.push(name.clone());
            }
        }
        Ok(LegacyStatus {
            bare_predicates: names
                .into_iter()
                .filter(|n| Vocabulary::is_bare(n))
                .collect(),
            pending_merges,
            type_labels: snap.scan_by_predicate(LEGACY_TYPE_PRED)?.len() as u64,
        })
    }

    /// Convert legacy data using the current vocabulary. With `dry_run`,
    /// report what would change and change nothing.
    pub fn convert_legacy(&self, dry_run: bool) -> Result<ConversionReport, StorageError> {
        if self.is_replica() {
            return Err(StorageError::ReadOnly(
                "legacy conversion runs on the primary".into(),
            ));
        }
        let vocab = self.vocabulary();
        let mut report = ConversionReport {
            dry_run,
            ..Default::default()
        };

        // ── 1. bare predicate names → IRIs ────────────────────────────────────
        let names = self.interned_predicates();
        for bare in names.iter().filter(|n| Vocabulary::is_bare(n)) {
            let iri = format!("{}{bare}", vocab.base);
            if names.contains(&iri) {
                report.merged.push((bare.clone(), iri, 0));
                if !dry_run {
                    self.rename_predicate(bare, &format!("{LEGACY_MERGE_PREFIX}{bare}"))?;
                }
            } else {
                report.renamed.push((bare.clone(), iri.clone()));
                if !dry_run {
                    self.rename_predicate(bare, &iri)?;
                }
            }
        }
        // Move live quads of merged predicates (including ones left by an
        // interrupted earlier run).
        let merge_names: Vec<String> = self
            .interned_predicates()
            .into_iter()
            .filter(|n| n.starts_with(LEGACY_MERGE_PREFIX))
            .collect();
        for legacy in merge_names {
            let bare = &legacy[LEGACY_MERGE_PREFIX.len()..];
            let iri = format!("{}{bare}", vocab.base);
            let moved = self.move_live_quads(&legacy, &iri, dry_run)?;
            match report.merged.iter_mut().find(|(b, _, _)| b == bare) {
                Some(entry) => entry.2 += moved,
                None if moved > 0 => report.merged.push((bare.to_string(), iri, moved)),
                None => {}
            }
        }

        // ── 2. __type labels → rdf:type ───────────────────────────────────────
        report.labels_converted = self.convert_type_labels(&vocab, dry_run)?;
        Ok(report)
    }

    /// Copy the live quads of predicate `from` to predicate `to` (an IRI)
    /// and close them under `from`. Returns how many were (or would be)
    /// moved.
    fn move_live_quads(&self, from: &str, to: &str, dry_run: bool) -> Result<u64, StorageError> {
        let snap = self.snapshot(Timestamp(self.oracle_ts()));
        let live = snap.scan_scoped(None, Some(from), None, &GraphScope::Union)?;
        if dry_run {
            return Ok(live.len() as u64);
        }
        let now = Timestamp::now();
        for chunk in live.chunks(CHUNK) {
            let mut tx = self.begin();
            tx.set_author("urn:pg:legacy-conversion");
            for (g, t) in chunk {
                tx.insert_in(with_predicate(t.clone(), to), *g, WriteMode::Add);
                tx.insert_in(close_at(t.clone(), now), *g, WriteMode::Add);
            }
            tx.commit()?;
        }
        Ok(live.len() as u64)
    }

    /// `s __type "X"` → `s rdf:type <expand(X)>` (same graph, same valid
    /// time), closing the property.
    fn convert_type_labels(&self, vocab: &Vocabulary, dry_run: bool) -> Result<u64, StorageError> {
        let snap = self.snapshot(Timestamp(self.oracle_ts()));
        let live = snap.scan_scoped(None, Some(LEGACY_TYPE_PRED), None, &GraphScope::Union)?;
        if dry_run {
            return Ok(live.len() as u64);
        }
        let now = Timestamp::now();
        let mut converted = 0;
        for chunk in live.chunks(CHUNK) {
            let mut tx = self.begin();
            tx.set_author("urn:pg:legacy-conversion");
            for (g, t) in chunk {
                let Triple::Property {
                    subject,
                    value,
                    temporal,
                    ..
                } = t
                else {
                    continue;
                };
                let label = match value {
                    Value::Text(s) => s.clone(),
                    other => match other.as_text() {
                        Some(s) => s.to_string(),
                        None => continue,
                    },
                };
                let class = vocab.expand(&label);
                tx.bind_iri(class.clone());
                tx.insert_in(
                    Triple::Relation {
                        subject: *subject,
                        predicate: Predicate::new(RDF_TYPE),
                        object: term::iri_to_node_id(&class),
                        edge_id: term::edge_id_for(&subject.to_string(), RDF_TYPE, &class),
                        temporal: *temporal,
                    },
                    *g,
                    WriteMode::Add,
                );
                tx.insert_in(close_at(t.clone(), now), *g, WriteMode::Add);
                converted += 1;
            }
            tx.commit()?;
        }
        Ok(converted)
    }
}

/// `t` with its predicate replaced.
fn with_predicate(t: Triple, to: &str) -> Triple {
    match t {
        Triple::Relation {
            subject,
            object,
            edge_id,
            temporal,
            ..
        } => Triple::Relation {
            subject,
            predicate: Predicate::new(to),
            object,
            edge_id,
            temporal,
        },
        Triple::Property {
            subject,
            value,
            temporal,
            ..
        } => Triple::Property {
            subject,
            predicate: Predicate::new(to),
            value,
            temporal,
        },
        other => other,
    }
}
