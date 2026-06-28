//! RDF import — parse N-Triples, Turtle, and JSON-LD into [`ImportedTriple`] structs.
//!
//! Used by the REST gateway's `POST /import/rdf` and `POST /import/subgraph`
//! handlers to accept multiple RDF serialization formats and load them into
//! PolarGraph via the gRPC Insert RPC.

use polargraph_core::{
    id::{EdgeId, NodeId},
    value::Value,
};
use rio_api::{
    model::{Literal, Subject, Term, Triple},
    parser::TriplesParser,
};
use rio_turtle::{NTriplesParser, TurtleParser};

// ── NodeId / EdgeId helpers ───────────────────────────────────────────────────

/// Map a URI string to a deterministic, stable [`NodeId`] using xxHash3-128.
///
/// The same URI always produces the same NodeId across processes and restarts.
pub fn uri_to_node_id(uri: &str) -> NodeId {
    let hash: u128 = xxhash_rust::xxh3::xxh3_128(uri.as_bytes());
    NodeId(uuid::Uuid::from_bytes(hash.to_le_bytes()))
}

/// Map a blank node identifier to a deterministic [`NodeId`].
///
/// The identifier is scoped with a `_:bnode_` prefix so it cannot collide with
/// real URIs.
pub fn bnode_to_node_id(bnode_id: &str) -> NodeId {
    let scoped = format!("_:bnode_{}", bnode_id);
    uri_to_node_id(&scoped)
}

/// Derive a deterministic [`EdgeId`] from the three IRI/blank-node strings of
/// a Relation triple. The same (S, P, O) combination always yields the same EdgeId.
pub fn edge_id_for(subject: &str, predicate: &str, object: &str) -> EdgeId {
    let mut buf = Vec::with_capacity(subject.len() + predicate.len() + object.len() + 2);
    buf.extend_from_slice(subject.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(predicate.as_bytes());
    buf.push(b'\x00');
    buf.extend_from_slice(object.as_bytes());
    let hash: u128 = xxhash_rust::xxh3::xxh3_128(&buf);
    EdgeId(uuid::Uuid::from_bytes(hash.to_le_bytes()))
}

// ── ImportedTriple ────────────────────────────────────────────────────────────

/// The object component of an imported RDF triple.
#[derive(Debug, Clone)]
pub enum ImportedObject {
    /// An IRI (e.g. `http://example.org/Bob`).
    Iri(String),
    /// A blank node identifier (without `_:` prefix).
    BlankNode(String),
    /// An RDF literal with a resolved PolarGraph [`Value`] and the original XSD
    /// datatype IRI (or `"lang:<tag>"` for language-tagged strings).
    Literal { value: Value, datatype: String },
}

/// A parsed RDF triple ready for insertion into PolarGraph.
#[derive(Debug, Clone)]
pub struct ImportedTriple {
    /// Subject IRI or blank node identifier.
    pub subject: String,
    /// `true` when `subject` is a blank node identifier (not a URI).
    pub subject_is_bnode: bool,
    /// Predicate IRI (always a URI in RDF).
    pub predicate: String,
    /// Object value.
    pub object: ImportedObject,
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn xsd_literal_to_value(value: &str, datatype_iri: &str) -> Value {
    match datatype_iri {
        "http://www.w3.org/2001/XMLSchema#integer"
        | "http://www.w3.org/2001/XMLSchema#long"
        | "http://www.w3.org/2001/XMLSchema#int"
        | "http://www.w3.org/2001/XMLSchema#short"
        | "http://www.w3.org/2001/XMLSchema#byte"
        | "http://www.w3.org/2001/XMLSchema#nonNegativeInteger"
        | "http://www.w3.org/2001/XMLSchema#positiveInteger" => value
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or_else(|_| Value::Text(value.to_string())),
        "http://www.w3.org/2001/XMLSchema#double"
        | "http://www.w3.org/2001/XMLSchema#float"
        | "http://www.w3.org/2001/XMLSchema#decimal" => value
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or_else(|_| Value::Text(value.to_string())),
        "http://www.w3.org/2001/XMLSchema#boolean" => Value::Bool(matches!(value, "true" | "1")),
        _ => Value::Text(value.to_string()),
    }
}

fn rio_literal_to_imported(lit: &Literal<'_>) -> (Value, String) {
    match lit {
        Literal::Simple { value } => (
            Value::Text(value.to_string()),
            "http://www.w3.org/2001/XMLSchema#string".to_string(),
        ),
        Literal::LanguageTaggedString { value, language } => {
            (Value::Text(value.to_string()), format!("lang:{}", language))
        }
        Literal::Typed { value, datatype } => {
            let dt = datatype.iri.to_string();
            (xsd_literal_to_value(value, &dt), dt)
        }
    }
}

fn rio_subject_to_parts(s: &Subject<'_>) -> (String, bool) {
    match s {
        Subject::NamedNode(n) => (n.iri.to_string(), false),
        Subject::BlankNode(b) => (b.id.to_string(), true),
        Subject::Triple(t) => {
            // RDF-star: quoted triple as subject — render as N-Triples-star string.
            (
                format!("<< {} {} {} >>", t.subject, t.predicate, t.object),
                false,
            )
        }
    }
}

fn rio_object_to_imported(o: &Term<'_>) -> ImportedObject {
    match o {
        Term::NamedNode(n) => ImportedObject::Iri(n.iri.to_string()),
        Term::BlankNode(b) => ImportedObject::BlankNode(b.id.to_string()),
        Term::Literal(lit) => {
            let (value, datatype) = rio_literal_to_imported(lit);
            ImportedObject::Literal { value, datatype }
        }
        Term::Triple(t) => {
            // RDF-star: quoted triple as object — store the N-Triples-star rendering as text.
            ImportedObject::Iri(format!("<< {} {} {} >>", t.subject, t.predicate, t.object))
        }
    }
}

fn collect_triple(t: Triple<'_>) -> ImportedTriple {
    let (subject, subject_is_bnode) = rio_subject_to_parts(&t.subject);
    ImportedTriple {
        subject,
        subject_is_bnode,
        predicate: t.predicate.iri.to_string(),
        object: rio_object_to_imported(&t.object),
    }
}

// ── Public parsers ────────────────────────────────────────────────────────────

/// Parse an [N-Triples](https://www.w3.org/TR/n-triples/) document.
pub fn parse_ntriples(input: &[u8]) -> Result<Vec<ImportedTriple>, String> {
    let cursor = std::io::Cursor::new(input);
    let mut parser = NTriplesParser::new(cursor);
    let mut triples = Vec::new();
    parser
        .parse_all(
            &mut |t: Triple<'_>| -> Result<(), rio_turtle::TurtleError> {
                triples.push(collect_triple(t));
                Ok(())
            },
        )
        .map_err(|e| format!("N-Triples parse error: {}", e))?;
    Ok(triples)
}

/// Parse a [Turtle](https://www.w3.org/TR/turtle/) document.
///
/// Relative IRIs are rejected (no base IRI is supplied).
pub fn parse_turtle(input: &[u8]) -> Result<Vec<ImportedTriple>, String> {
    let cursor = std::io::Cursor::new(input);
    let mut parser = TurtleParser::new(cursor, None);
    let mut triples = Vec::new();
    parser
        .parse_all(
            &mut |t: Triple<'_>| -> Result<(), rio_turtle::TurtleError> {
                triples.push(collect_triple(t));
                Ok(())
            },
        )
        .map_err(|e| format!("Turtle parse error: {}", e))?;
    Ok(triples)
}

/// Parse a JSON-LD document (flat `@graph` format as produced by
/// [`crate::serialize_jsonld`]).
///
/// Supports:
/// - `{ "@id": "<iri>", "<pred>": { "@id": "<iri>" } }` → Relation triple
/// - `{ "@id": "<iri>", "<pred>": { "@value": ..., "@type": "xsd:..." } }` → Property triple
/// - Array-valued predicates expand into multiple triples.
pub fn parse_jsonld(input: &str) -> Result<Vec<ImportedTriple>, String> {
    let doc: serde_json::Value =
        serde_json::from_str(input).map_err(|e| format!("JSON parse error: {}", e))?;

    let graph = doc
        .get("@graph")
        .ok_or_else(|| "JSON-LD document missing @graph".to_string())?
        .as_array()
        .ok_or_else(|| "@graph must be an array".to_string())?;

    let mut triples = Vec::new();

    for node in graph {
        let obj = match node.as_object() {
            Some(o) => o,
            None => continue,
        };
        let subject = match obj.get("@id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        for (key, val) in obj {
            if key.starts_with('@') {
                continue;
            }
            let predicate = key.clone();

            // Expand both singleton and array-valued predicates.
            let items: Vec<&serde_json::Value> = if val.is_array() {
                val.as_array().unwrap().iter().collect()
            } else {
                vec![val]
            };

            for item in items {
                let imported_object = if let Some(id) = item.get("@id").and_then(|v| v.as_str()) {
                    ImportedObject::Iri(id.to_string())
                } else if let Some(raw_val) = item.get("@value") {
                    let type_str = item
                        .get("@type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("xsd:string");
                    let full_dt = expand_xsd_prefix(type_str);
                    let value = xsd_literal_to_value(
                        raw_val.as_str().unwrap_or(&raw_val.to_string()),
                        &full_dt,
                    );
                    ImportedObject::Literal {
                        value,
                        datatype: full_dt,
                    }
                } else {
                    match item {
                        serde_json::Value::String(s) => ImportedObject::Literal {
                            value: Value::Text(s.clone()),
                            datatype: "http://www.w3.org/2001/XMLSchema#string".to_string(),
                        },
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                ImportedObject::Literal {
                                    value: Value::Int(i),
                                    datatype: "http://www.w3.org/2001/XMLSchema#integer"
                                        .to_string(),
                                }
                            } else {
                                ImportedObject::Literal {
                                    value: Value::Float(n.as_f64().unwrap_or(0.0)),
                                    datatype: "http://www.w3.org/2001/XMLSchema#double".to_string(),
                                }
                            }
                        }
                        serde_json::Value::Bool(b) => ImportedObject::Literal {
                            value: Value::Bool(*b),
                            datatype: "http://www.w3.org/2001/XMLSchema#boolean".to_string(),
                        },
                        _ => continue,
                    }
                };

                triples.push(ImportedTriple {
                    subject: subject.clone(),
                    subject_is_bnode: false,
                    predicate: predicate.clone(),
                    object: imported_object,
                });
            }
        }
    }

    Ok(triples)
}

/// Expand `xsd:` and `rdfs:` shorthand prefixes in datatype strings.
pub fn expand_xsd_prefix(dt: &str) -> String {
    if let Some(local) = dt.strip_prefix("xsd:") {
        format!("http://www.w3.org/2001/XMLSchema#{}", local)
    } else if let Some(local) = dt.strip_prefix("rdfs:") {
        format!("http://www.w3.org/2000/01/rdf-schema#{}", local)
    } else {
        dt.to_string()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_to_node_id_deterministic() {
        let a = uri_to_node_id("http://example.org/Alice");
        let b = uri_to_node_id("http://example.org/Alice");
        let c = uri_to_node_id("http://example.org/Bob");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn parse_ntriples_relation() {
        let nt =
            b"<http://example.org/Alice> <http://schema.org/knows> <http://example.org/Bob> .\n";
        let triples = parse_ntriples(nt).unwrap();
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].subject, "http://example.org/Alice");
        assert_eq!(triples[0].predicate, "http://schema.org/knows");
        assert!(
            matches!(&triples[0].object, ImportedObject::Iri(s) if s == "http://example.org/Bob")
        );
    }

    #[test]
    fn parse_ntriples_literal() {
        let nt = b"<http://example.org/Alice> <http://schema.org/name> \"Alice\" .\n";
        let triples = parse_ntriples(nt).unwrap();
        assert_eq!(triples.len(), 1);
        assert!(matches!(
            &triples[0].object,
            ImportedObject::Literal { value: Value::Text(s), .. } if s == "Alice"
        ));
    }

    #[test]
    fn parse_ntriples_typed_integer() {
        let nt = b"<http://example.org/x> <http://example.org/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n";
        let triples = parse_ntriples(nt).unwrap();
        assert!(matches!(
            &triples[0].object,
            ImportedObject::Literal {
                value: Value::Int(30),
                ..
            }
        ));
    }

    #[test]
    fn parse_turtle_basic() {
        let ttl = b"@prefix ex: <http://example.org/> .\nex:Alice ex:knows ex:Bob .\n";
        let triples = parse_turtle(ttl).unwrap();
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].subject, "http://example.org/Alice");
        assert_eq!(triples[0].predicate, "http://example.org/knows");
    }

    #[test]
    fn parse_jsonld_relation_and_literal() {
        let jsonld = r#"{
            "@context": { "xsd": "http://www.w3.org/2001/XMLSchema#" },
            "@graph": [
                {
                    "@id": "urn:uuid:aaa",
                    "http://schema.org/knows": { "@id": "urn:uuid:bbb" },
                    "http://schema.org/name": { "@value": "Alice", "@type": "xsd:string" }
                }
            ]
        }"#;
        let triples = parse_jsonld(jsonld).unwrap();
        assert_eq!(triples.len(), 2);
        let rel = triples
            .iter()
            .find(|t| t.predicate == "http://schema.org/knows")
            .unwrap();
        assert!(matches!(&rel.object, ImportedObject::Iri(s) if s == "urn:uuid:bbb"));
        let prop = triples
            .iter()
            .find(|t| t.predicate == "http://schema.org/name")
            .unwrap();
        assert!(matches!(
            &prop.object,
            ImportedObject::Literal { value: Value::Text(s), .. } if s == "Alice"
        ));
    }
}
