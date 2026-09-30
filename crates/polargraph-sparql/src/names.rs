//! Rendering NodeIds as IRIs on export.
//!
//! Hashed NodeIds can only be shown under their real IRI if the caller has
//! looked them up in the IRI dictionary (gRPC `ResolveIris`). [`IriNames`]
//! carries the result of that lookup to every serializer; nodes without an
//! entry render as `urn:uuid:<id>`.
//!
//! RDF export paths build [`RdfTriple`]s whose node terms are
//! `<urn:uuid:…>` strings; [`IriNames::rewrite_triples`] swaps those terms
//! (field by field, never by text search) for the resolved names.

use std::collections::{BTreeSet, HashMap};

use polargraph_core::{id::NodeId, skolem, term};

use crate::{
    response::{SparqlBindings, SparqlValue},
    serialize::{RdfStarSubject, RdfStarTriple, RdfTriple},
};

/// Resolved IRIs for the nodes in one response.
#[derive(Debug, Clone, Default)]
pub struct IriNames {
    names: HashMap<NodeId, String>,
    deskolemize: bool,
}

impl IriNames {
    /// `names` maps nodes to IRIs (typically from `ResolveIris`). With
    /// `deskolemize`, skolem IRIs render as blank nodes (`_:label`).
    pub fn new(names: HashMap<NodeId, String>, deskolemize: bool) -> Self {
        Self { names, deskolemize }
    }

    /// Names resolved from `(node, iri)` pairs.
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = (NodeId, String)>,
        deskolemize: bool,
    ) -> Self {
        Self::new(pairs.into_iter().collect(), deskolemize)
    }

    /// The node's IRI: its dictionary entry, or `urn:uuid:<id>`.
    pub fn iri(&self, id: &NodeId) -> String {
        self.names
            .get(id)
            .cloned()
            .unwrap_or_else(|| term::fallback_iri(id))
    }

    /// The blank-node label for `id` when de-skolemizing and `id` is a skolem IRI.
    pub fn bnode_label(&self, id: &NodeId) -> Option<String> {
        if !self.deskolemize {
            return None;
        }
        self.names
            .get(id)
            .and_then(|iri| skolem::deskolemized_label(iri))
    }

    /// The node as an N-Triples / Turtle term: `<iri>` or `_:label`.
    pub fn term(&self, id: &NodeId) -> String {
        match self.bnode_label(id) {
            Some(label) => format!("_:{label}"),
            None => format!("<{}>", self.iri(id)),
        }
    }

    /// Rewrite one rendered term: `<urn:uuid:X>` becomes [`Self::term`] of `X`;
    /// every other term (other IRIs, literals, blank nodes) is returned as is.
    pub fn rewrite_term(&self, rendered: &str) -> String {
        match uuid_term(rendered) {
            Some(id) => self.term(&id),
            None => rendered.to_string(),
        }
    }

    /// Rewrite the subject and object of every triple.
    pub fn rewrite_triples(&self, triples: &mut [RdfTriple]) {
        for t in triples {
            t.subject = self.rewrite_term(&t.subject);
            t.object = self.rewrite_term(&t.object);
        }
    }

    /// Rewrite node terms in RDF-star triples, including quoted-triple parts.
    pub fn rewrite_star_triples(&self, triples: &mut [RdfStarTriple]) {
        for t in triples {
            t.subject = match &t.subject {
                RdfStarSubject::Iri(s) => RdfStarSubject::Iri(self.rewrite_term(s)),
                RdfStarSubject::QuotedTriple { s, p, o } => RdfStarSubject::QuotedTriple {
                    s: self.rewrite_term(s),
                    p: p.clone(),
                    o: self.rewrite_term(o),
                },
            };
            t.object = self.rewrite_term(&t.object);
        }
    }
}

/// `Some(id)` if `rendered` is exactly `<urn:uuid:…>`.
fn uuid_term(rendered: &str) -> Option<NodeId> {
    let inner = rendered.strip_prefix('<')?.strip_suffix('>')?;
    term::uuid_iri(inner)
}

/// Every NodeId referenced as a `<urn:uuid:…>` subject or object term.
pub fn node_ids_in_triples(triples: &[RdfTriple]) -> Vec<NodeId> {
    let ids: BTreeSet<NodeId> = triples
        .iter()
        .flat_map(|t| [uuid_term(&t.subject), uuid_term(&t.object)])
        .flatten()
        .collect();
    ids.into_iter().collect()
}

/// Every NodeId referenced by RDF-star triples (quoted-triple parts included).
pub fn node_ids_in_star_triples(triples: &[RdfStarTriple]) -> Vec<NodeId> {
    let mut ids = BTreeSet::new();
    for t in triples {
        match &t.subject {
            RdfStarSubject::Iri(s) => ids.extend(uuid_term(s)),
            RdfStarSubject::QuotedTriple { s, o, .. } => {
                ids.extend(uuid_term(s));
                ids.extend(uuid_term(o));
            }
        }
        ids.extend(uuid_term(&t.object));
    }
    ids.into_iter().collect()
}

/// Every NodeId bound to a variable in `bindings`.
pub fn node_ids_in_bindings(bindings: &[SparqlBindings]) -> Vec<NodeId> {
    let ids: BTreeSet<NodeId> = bindings
        .iter()
        .flat_map(|b| b.values())
        .filter_map(|v| match v {
            SparqlValue::Uri(id) => Some(*id),
            _ => None,
        })
        .collect();
    ids.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::skolem::ImportScope;

    #[test]
    fn resolved_nodes_render_under_their_iri_others_fall_back() {
        let alice = term::iri_to_node_id("http://ex/Alice");
        let native = NodeId::new();
        let names = IriNames::from_pairs([(alice, "http://ex/Alice".to_string())], false);

        assert_eq!(names.term(&alice), "<http://ex/Alice>");
        assert_eq!(
            names.term(&native),
            format!("<{}>", term::fallback_iri(&native))
        );
    }

    #[test]
    fn rewrite_touches_only_uuid_node_terms() {
        let alice = term::iri_to_node_id("http://ex/Alice");
        let names = IriNames::from_pairs([(alice, "http://ex/Alice".to_string())], false);
        let mut triples = vec![RdfTriple {
            subject: format!("<{}>", term::fallback_iri(&alice)),
            predicate: "<http://ex/name>".into(),
            object: "\"<urn:uuid:not-a-term>\"".into(),
        }];
        assert_eq!(node_ids_in_triples(&triples), vec![alice]);
        names.rewrite_triples(&mut triples);
        assert_eq!(triples[0].subject, "<http://ex/Alice>");
        assert_eq!(
            triples[0].object, "\"<urn:uuid:not-a-term>\"",
            "literals untouched"
        );
    }

    #[test]
    fn deskolemize_renders_skolem_iris_as_blank_nodes() {
        let scope = ImportScope::new("https://kb.example.com", "i1");
        let iri = scope.skolem_iri("b0");
        let id = scope.bnode_node_id("b0");
        let plain = IriNames::from_pairs([(id, iri.clone())], false);
        let desk = IriNames::from_pairs([(id, iri.clone())], true);

        assert_eq!(plain.term(&id), format!("<{iri}>"));
        assert_eq!(desk.term(&id), "_:i1_b0");
    }
}
