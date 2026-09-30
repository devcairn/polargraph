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

use crate::{
    id::{EdgeId, NodeId},
    value::Value,
};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

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

/// The [`Value`] for an RDF literal — the one mapping every import and query
/// path uses.
///
/// - a language tag → [`Value::LangText`];
/// - no datatype or `xsd:string` → [`Value::Text`];
/// - `xsd:integer`/`long`/`int`/`short`/`byte`/`nonNegativeInteger`/
///   `positiveInteger` → [`Value::Int`]; `xsd:double`/`float`/`decimal` →
///   [`Value::Float`]; `xsd:boolean` → [`Value::Bool`];
/// - anything else, or a lexical form that doesn't parse as its datatype →
///   [`Value::Typed`], keeping the lexical form and datatype IRI.
pub fn literal_to_value(lexical: &str, datatype: Option<&str>, lang: Option<&str>) -> Value {
    if let Some(lang) = lang.filter(|l| !l.is_empty()) {
        return Value::LangText {
            text: lexical.to_string(),
            lang: lang.to_string(),
        };
    }
    let Some(datatype) = datatype else {
        return Value::Text(lexical.to_string());
    };
    let typed = || Value::Typed {
        lexical: lexical.to_string(),
        datatype: datatype.to_string(),
    };
    match datatype.strip_prefix(XSD) {
        Some("string") => Value::Text(lexical.to_string()),
        Some(
            "integer" | "long" | "int" | "short" | "byte" | "nonNegativeInteger"
            | "positiveInteger",
        ) => lexical.parse().map(Value::Int).unwrap_or_else(|_| typed()),
        Some("double" | "float" | "decimal") => lexical
            .parse()
            .map(Value::Float)
            .unwrap_or_else(|_| typed()),
        Some("boolean") => match lexical {
            "true" | "1" => Value::Bool(true),
            "false" | "0" => Value::Bool(false),
            _ => typed(),
        },
        _ => typed(),
    }
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
    fn literals_keep_language_and_datatype() {
        let xsd = |t: &str| format!("{XSD}{t}");
        assert_eq!(
            literal_to_value("Acme", None, Some("en")),
            Value::LangText {
                text: "Acme".into(),
                lang: "en".into()
            }
        );
        assert_eq!(
            literal_to_value("Acme", None, None),
            Value::Text("Acme".into())
        );
        assert_eq!(
            literal_to_value("Acme", Some(&xsd("string")), None),
            Value::Text("Acme".into())
        );
        assert_eq!(
            literal_to_value("30", Some(&xsd("integer")), None),
            Value::Int(30)
        );
        assert_eq!(
            literal_to_value("0", Some(&xsd("boolean")), None),
            Value::Bool(false)
        );
        assert_eq!(
            literal_to_value("2026-09-29", Some(&xsd("date")), None),
            Value::Typed {
                lexical: "2026-09-29".into(),
                datatype: xsd("date")
            }
        );
        // A lexical form that doesn't parse keeps its datatype instead of
        // silently becoming a string.
        assert!(matches!(
            literal_to_value("thirty", Some(&xsd("integer")), None),
            Value::Typed { .. }
        ));
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
