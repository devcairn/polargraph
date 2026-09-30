//! Term identity: the one mapping between RDF terms and PolarGraph IDs.
//!
//! Every import, query-translation and export path maps IRIs through this
//! module, so the same IRI names the same node however it arrives:
//!
//! - `urn:uuid:<uuid>` ↔ `NodeId(<uuid>)` — native IDs round-trip unchanged;
//! - any other IRI → [`NodeId::from_iri`] (xxHash3-128 of the IRI);
//! - blank nodes are first skolemized to IRIs by
//!   [`ImportScope`](crate::skolem::ImportScope), then mapped as above.
//!
//! Hashing is one-way; the IRI dictionary in `polargraph-storage` records the
//! IRI for each hashed `NodeId` so export can render it again.

use uuid::Uuid;

use crate::id::{EdgeId, NodeId};

/// Prefix of IRIs that carry a `NodeId` directly.
pub const UUID_IRI_PREFIX: &str = "urn:uuid:";

/// The `NodeId` an IRI names.
pub fn iri_to_node_id(iri: &str) -> NodeId {
    uuid_iri(iri).unwrap_or_else(|| NodeId::from_iri(iri))
}

/// `Some(id)` if `iri` is a well-formed `urn:uuid:` IRI.
pub fn uuid_iri(iri: &str) -> Option<NodeId> {
    let rest = iri.strip_prefix(UUID_IRI_PREFIX)?;
    Uuid::parse_str(rest).ok().map(NodeId)
}

/// True when `iri` needs a dictionary entry to be recovered from its `NodeId`
/// (i.e. it is hashed rather than a `urn:uuid:` IRI).
pub fn needs_dictionary(iri: &str) -> bool {
    uuid_iri(iri).is_none()
}

/// The IRI to show for a `NodeId` with no dictionary entry: `urn:uuid:<id>`.
pub fn fallback_iri(id: &NodeId) -> String {
    format!("{UUID_IRI_PREFIX}{}", id.0)
}

/// Deterministic `EdgeId` for a relation given the IRIs of its three terms
/// (skolem IRIs for blank nodes). Used by every RDF import path.
pub fn edge_id_for(subject: &str, predicate: &str, object: &str) -> EdgeId {
    let mut buf = Vec::with_capacity(subject.len() + predicate.len() + object.len() + 2);
    buf.extend_from_slice(subject.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(predicate.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(object.as_bytes());
    let hash: u128 = xxhash_rust::xxh3::xxh3_128(&buf);
    EdgeId(Uuid::from_bytes(hash.to_le_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_iris_round_trip() {
        let id = NodeId::new();
        let iri = fallback_iri(&id);
        assert_eq!(iri_to_node_id(&iri), id);
        assert!(!needs_dictionary(&iri));
    }

    #[test]
    fn other_iris_hash() {
        let iri = "http://example.org/Alice";
        assert_eq!(iri_to_node_id(iri), NodeId::from_iri(iri));
        assert_eq!(iri_to_node_id(iri), iri_to_node_id(iri));
        assert_ne!(
            iri_to_node_id(iri),
            iri_to_node_id("http://example.org/Bob")
        );
        assert!(needs_dictionary(iri));
    }

    #[test]
    fn malformed_uuid_iri_is_hashed() {
        let iri = "urn:uuid:not-a-uuid";
        assert_eq!(iri_to_node_id(iri), NodeId::from_iri(iri));
    }

    #[test]
    fn edge_ids_depend_on_all_three_terms() {
        let e = edge_id_for("s", "p", "o");
        assert_eq!(e, edge_id_for("s", "p", "o"));
        assert_ne!(e, edge_id_for("s", "p", "o2"));
        // The separator keeps ("ab","c") and ("a","bc") apart.
        assert_ne!(edge_id_for("ab", "c", "o"), edge_id_for("a", "bc", "o"));
    }
}
