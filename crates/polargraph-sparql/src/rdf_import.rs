//! RDF import — parse N-Triples, Turtle, and JSON-LD into [`ImportedTriple`] structs.
//!
//! Used by the REST gateway's `POST /import/rdf` and `POST /import/subgraph`
//! handlers to accept multiple RDF serialization formats and load them into
//! PolarGraph via the gRPC Insert RPC.

use polargraph_core::{
    id::{EdgeId, NodeId},
    skolem::ImportScope,
    term,
    value::Value,
};
use rio_api::{
    model::{GraphName, Literal, Quad, Subject, Term, Triple},
    parser::{QuadsParser, TriplesParser},
};
use rio_turtle::{NQuadsParser, NTriplesParser, TriGParser, TurtleParser};

// ── NodeId / EdgeId helpers ───────────────────────────────────────────────────

/// Map an IRI to its [`NodeId`] — see [`polargraph_core::term::iri_to_node_id`]
/// (`urn:uuid:` IRIs map to their UUID, everything else is hashed).
pub fn uri_to_node_id(uri: &str) -> NodeId {
    term::iri_to_node_id(uri)
}

/// Deterministic [`EdgeId`] for a relation — see [`polargraph_core::term::edge_id_for`].
pub fn edge_id_for(subject: &str, predicate: &str, object: &str) -> EdgeId {
    term::edge_id_for(subject, predicate, object)
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
    /// Named graph from a quad format (TriG, N-Quads): an IRI, or a blank
    /// node label as `_:label`. `None` = the default graph.
    pub graph: Option<String>,
}

impl ImportedTriple {
    /// The graph IRI (blank-node graph names skolemized within `scope`);
    /// `None` for the default graph.
    pub fn graph_iri(&self, scope: &ImportScope) -> Option<String> {
        self.graph.as_ref().map(|g| match g.strip_prefix("_:") {
            Some(label) => scope.skolem_iri(label),
            None => g.clone(),
        })
    }

    /// The subject's `NodeId`; blank nodes are skolemized within `scope`.
    pub fn subject_node_id(&self, scope: &ImportScope) -> NodeId {
        if self.subject_is_bnode {
            scope.bnode_node_id(&self.subject)
        } else {
            uri_to_node_id(&self.subject)
        }
    }
}

impl ImportedTriple {
    /// The IRIs this triple names nodes by — subject and IRI/blank-node object,
    /// with blank nodes as their skolem IRIs — for the IRI dictionary.
    pub fn node_iris(&self, scope: &ImportScope) -> impl Iterator<Item = String> {
        let subject = if self.subject_is_bnode {
            scope.skolem_iri(&self.subject)
        } else {
            self.subject.clone()
        };
        let object = match &self.object {
            ImportedObject::Iri(iri) => Some(iri.clone()),
            ImportedObject::BlankNode(label) => Some(scope.skolem_iri(label)),
            ImportedObject::Literal { .. } => None,
        };
        std::iter::once(subject).chain(object)
    }
}

impl ImportedObject {
    /// The object's `NodeId` for IRIs and blank nodes (skolemized within
    /// `scope`); `None` for literals.
    pub fn node_id(&self, scope: &ImportScope) -> Option<NodeId> {
        match self {
            ImportedObject::Iri(iri) => Some(uri_to_node_id(iri)),
            ImportedObject::BlankNode(label) => Some(scope.bnode_node_id(label)),
            ImportedObject::Literal { .. } => None,
        }
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Map a typed literal to a [`Value`] — see [`term::literal_to_value`].
pub(crate) fn xsd_literal_to_value(value: &str, datatype_iri: &str) -> Value {
    term::literal_to_value(value, Some(datatype_iri), None)
}

fn rio_literal_to_imported(lit: &Literal<'_>) -> (Value, String) {
    match lit {
        Literal::Simple { value } => (
            Value::Text(value.to_string()),
            "http://www.w3.org/2001/XMLSchema#string".to_string(),
        ),
        Literal::LanguageTaggedString { value, language } => (
            term::literal_to_value(value, None, Some(language)),
            format!("lang:{}", language),
        ),
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
        graph: None,
    }
}

fn collect_quad(q: Quad<'_>) -> ImportedTriple {
    let mut t = collect_triple(Triple {
        subject: q.subject,
        predicate: q.predicate,
        object: q.object,
    });
    t.graph = q.graph_name.map(|g| match g {
        GraphName::NamedNode(n) => n.iri.to_string(),
        GraphName::BlankNode(b) => format!("_:{}", b.id),
    });
    t
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

/// Parse an [N-Quads](https://www.w3.org/TR/n-quads/) document.
pub fn parse_nquads(input: &[u8]) -> Result<Vec<ImportedTriple>, String> {
    let mut parser = NQuadsParser::new(std::io::Cursor::new(input));
    let mut quads = Vec::new();
    parser
        .parse_all(&mut |q: Quad<'_>| -> Result<(), rio_turtle::TurtleError> {
            quads.push(collect_quad(q));
            Ok(())
        })
        .map_err(|e| format!("N-Quads parse error: {}", e))?;
    Ok(quads)
}

/// Parse a [TriG](https://www.w3.org/TR/trig/) document.
pub fn parse_trig(input: &[u8]) -> Result<Vec<ImportedTriple>, String> {
    let mut parser = TriGParser::new(std::io::Cursor::new(input), None);
    let mut quads = Vec::new();
    parser
        .parse_all(&mut |q: Quad<'_>| -> Result<(), rio_turtle::TurtleError> {
            quads.push(collect_quad(q));
            Ok(())
        })
        .map_err(|e| format!("TriG parse error: {}", e))?;
    Ok(quads)
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
/// - `"@id": "_:label"` (subject or object) is a blank node.
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
        let (subject, subject_is_bnode) = match obj.get("@id").and_then(|v| v.as_str()) {
            Some(id) => match id.strip_prefix("_:") {
                Some(label) => (label.to_string(), true),
                None => (id.to_string(), false),
            },
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
                    match id.strip_prefix("_:") {
                        Some(label) => ImportedObject::BlankNode(label.to_string()),
                        None => ImportedObject::Iri(id.to_string()),
                    }
                } else if let Some(raw_val) = item.get("@value") {
                    let type_str = item
                        .get("@type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("xsd:string");
                    let full_dt = expand_xsd_prefix(type_str);
                    let lang = item.get("@language").and_then(|l| l.as_str());
                    let value = term::literal_to_value(
                        raw_val.as_str().unwrap_or(&raw_val.to_string()),
                        Some(&full_dt),
                        lang,
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
                    subject_is_bnode,
                    predicate: predicate.clone(),
                    object: imported_object,
                    graph: None,
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
    fn bnodes_are_scoped_per_import() {
        let nt = b"_:b0 <http://schema.org/knows> _:b1 .\n";
        let t = &parse_ntriples(nt).unwrap()[0];
        let first = ImportScope::new(crate::DEFAULT_SKOLEM_BASE, "import-1");
        let second = ImportScope::new(crate::DEFAULT_SKOLEM_BASE, "import-2");

        assert_ne!(t.subject_node_id(&first), t.subject_node_id(&second));
        assert_ne!(t.object.node_id(&first), t.object.node_id(&second));
        // Same import_id → same nodes (idempotent re-import).
        let again = ImportScope::new(crate::DEFAULT_SKOLEM_BASE, "import-1");
        assert_eq!(t.subject_node_id(&first), t.subject_node_id(&again));
        // IRIs are unaffected by scope.
        let iri = ImportedObject::Iri("http://example.org/Bob".into());
        assert_eq!(iri.node_id(&first), iri.node_id(&second));
    }

    #[test]
    fn parse_jsonld_blank_node_ids() {
        let doc = r#"{"@graph": [
            {"@id": "_:b0", "http://schema.org/knows": {"@id": "_:b1"}}
        ]}"#;
        let t = &parse_jsonld(doc).unwrap()[0];
        assert!(t.subject_is_bnode);
        assert_eq!(t.subject, "b0");
        assert!(matches!(&t.object, ImportedObject::BlankNode(l) if l == "b1"));
    }

    #[test]
    fn parse_quad_formats_keep_graph_names() {
        let nq = concat!(
            "<http://ex/a> <http://ex/p> <http://ex/b> <http://ex/g1> .\n",
            "<http://ex/a> <http://ex/p> \"x\" .\n",
            "<http://ex/a> <http://ex/p> <http://ex/c> _:g .\n",
        );
        let q = parse_nquads(nq.as_bytes()).unwrap();
        assert_eq!(q[0].graph.as_deref(), Some("http://ex/g1"));
        assert_eq!(q[1].graph, None);
        let scope = ImportScope::new("https://kb.example.com", "i1");
        assert_eq!(
            q[2].graph_iri(&scope).as_deref(),
            Some("https://kb.example.com/.well-known/genid/i1/g")
        );

        let trig = b"@prefix ex: <http://ex/> .\nex:g2 { ex:a ex:p ex:b . }\nex:a ex:p ex:c .\n";
        let t = parse_trig(trig).unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(t[0].graph.as_deref(), Some("http://ex/g2"));
        assert_eq!(t[1].graph, None);
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
    fn parse_keeps_language_tags_and_unknown_datatypes() {
        let nt = concat!(
            "<http://ex/a> <http://ex/label> \"Acme\"@en .\n",
            "<http://ex/a> <http://ex/founded> \"1999-01-01\"^^<http://www.w3.org/2001/XMLSchema#date> .\n",
        );
        let t = parse_ntriples(nt.as_bytes()).unwrap();
        assert!(matches!(
            &t[0].object,
            ImportedObject::Literal { value: Value::LangText { text, lang }, .. }
                if text == "Acme" && lang == "en"
        ));
        assert!(matches!(
            &t[1].object,
            ImportedObject::Literal { value: Value::Typed { lexical, datatype }, .. }
                if lexical == "1999-01-01" && datatype.ends_with("#date")
        ));

        let doc = r#"{"@graph": [{"@id": "http://ex/a",
            "http://ex/label": {"@value": "Acmé", "@language": "fr"}}]}"#;
        assert!(matches!(
            &parse_jsonld(doc).unwrap()[0].object,
            ImportedObject::Literal { value: Value::LangText { lang, .. }, .. } if lang == "fr"
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
