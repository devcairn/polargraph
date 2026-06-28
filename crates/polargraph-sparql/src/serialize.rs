//! RDF serializers: N-Triples and Turtle output formats.

use polargraph_core::id::NodeId;
use polargraph_core::value::Value;

// ── Value → literal ───────────────────────────────────────────────────────────

/// Convert a PolarGraph [`Value`] to an RDF literal in N-Triples syntax.
pub fn value_to_nt_literal(val: &Value) -> String {
    match val {
        Value::Text(s) => format!(
            "\"{}\"^^<http://www.w3.org/2001/XMLSchema#string>",
            nt_escape(s)
        ),
        Value::Int(n) => format!(
            "\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            n
        ),
        Value::Float(f) => format!(
            "\"{}\"^^<http://www.w3.org/2001/XMLSchema#double>",
            f
        ),
        Value::Bool(b) => format!(
            "\"{}\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
            b
        ),
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!(
                "\"{}\"^^<http://www.w3.org/2001/XMLSchema#hexBinary>",
                hex
            )
        }
        Value::Vector(v) => {
            // Represent as a JSON-array string — not an RDF standard but best-effort.
            let s: Vec<String> = v.iter().map(|f| f.to_string()).collect();
            format!(
                "\"[{}]\"^^<http://www.w3.org/2001/XMLSchema#string>",
                s.join(",")
            )
        }
        Value::Null => {
            "\"\"^^<http://www.w3.org/2001/XMLSchema#string>".to_string()
        }
    }
}

/// Format a [`NodeId`] as a `<urn:uuid:…>` IRI string (including angle brackets).
pub fn node_id_to_iri(id: &NodeId) -> String {
    format!("<urn:uuid:{}>", id.0)
}

fn nt_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

// ── RDF triple ready for serialization ───────────────────────────────────────

/// A fully-formed RDF triple ready for textual serialization.
///
/// All three components are already rendered as strings:
/// - `subject`   — an IRI in angle brackets, e.g. `<urn:uuid:…>`
/// - `predicate` — an IRI in angle brackets, e.g. `<http://example.org/knows>`
/// - `object`    — an IRI in angle brackets OR an N-Triples literal,
///   e.g. `"hello"^^<xsd:string>`
pub struct RdfTriple {
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

// ── RDF-star: quoted-triple subject ──────────────────────────────────────────

/// The subject component of an RDF-star triple, which may itself be a quoted triple.
#[derive(Clone)]
pub enum RdfStarSubject {
    /// An ordinary IRI or blank node already rendered with angle brackets, e.g. `<urn:uuid:…>`.
    Iri(String),
    /// A quoted triple `<< s p o >>` used as the subject of an outer statement.
    QuotedTriple {
        s: String,
        p: String,
        o: String,
    },
}

impl RdfStarSubject {
    /// Render to the N-Triples-star text form: either `<iri>` or `<< s p o >>`.
    pub fn to_ntriples(&self) -> String {
        match self {
            RdfStarSubject::Iri(iri) => iri.clone(),
            RdfStarSubject::QuotedTriple { s, p, o } => format!("<< {s} {p} {o} >>"),
        }
    }
}

/// An RDF-star triple whose subject may be a quoted triple.
pub struct RdfStarTriple {
    pub subject: RdfStarSubject,
    pub predicate: String,
    pub object: String,
}

// ── N-Triples-star serializer ─────────────────────────────────────────────────

/// Serialize a slice of RDF-star triples to [N-Triples-star](https://w3c.github.io/rdf-star/cg-spec/) format.
///
/// Subjects that are quoted triples emit as `<< s p o >> predicate object .`
pub fn serialize_ntriples_star(triples: &[RdfStarTriple]) -> String {
    let mut out = String::new();
    for t in triples {
        out.push_str(&t.subject.to_ntriples());
        out.push(' ');
        out.push_str(&t.predicate);
        out.push(' ');
        out.push_str(&t.object);
        out.push_str(" .\n");
    }
    out
}

// ── Turtle-star serializer ─────────────────────────────────────────────────────

/// Serialize a slice of RDF-star triples to [Turtle-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html) format.
///
/// Quoted-triple subjects are emitted as `<< s p o >>` inline. Triples with plain IRI subjects
/// are grouped by subject the same as in regular Turtle.
pub fn serialize_turtle_star(triples: &[RdfStarTriple]) -> String {
    use std::collections::BTreeMap;

    let prefixes = [
        ("xsd:", "http://www.w3.org/2001/XMLSchema#"),
        ("rdf:", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
        ("rdfs:", "http://www.w3.org/2000/01/rdf-schema#"),
        ("owl:", "http://www.w3.org/2002/07/owl#"),
    ];

    let mut out = String::new();
    for (prefix, iri) in &prefixes {
        out.push_str(&format!("@prefix {} <{}> .\n", prefix, iri));
    }
    if !triples.is_empty() {
        out.push('\n');
    }

    // Separate quoted-subject triples (emitted inline) from plain-IRI-subject triples (grouped).
    let mut plain: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    let mut quoted_lines: Vec<String> = Vec::new();

    for t in triples {
        match &t.subject {
            RdfStarSubject::Iri(iri) => {
                plain
                    .entry(iri.as_str())
                    .or_default()
                    .push((t.predicate.as_str(), t.object.as_str()));
            }
            RdfStarSubject::QuotedTriple { s, p, o } => {
                quoted_lines.push(format!(
                    "<< {s} {p} {o} >> {} {} .\n",
                    t.predicate, t.object,
                    s = s, p = p, o = o,
                ));
            }
        }
    }

    for (subj, preds) in &plain {
        out.push_str(subj);
        out.push('\n');
        for (i, (pred, obj)) in preds.iter().enumerate() {
            let terminator = if i == preds.len() - 1 { " ." } else { " ;" };
            out.push_str(&format!("    {} {}{}\n", pred, obj, terminator));
        }
        out.push('\n');
    }

    for line in &quoted_lines {
        out.push_str(line);
    }

    out
}

// ── N-Triples serializer ──────────────────────────────────────────────────────

/// Serialize a slice of RDF triples to [N-Triples](https://www.w3.org/TR/n-triples/) format.
///
/// Produces one line per triple: `<subject> <predicate> <object> .`
pub fn serialize_ntriples(triples: &[RdfTriple]) -> String {
    let mut out = String::new();
    for t in triples {
        out.push_str(&t.subject);
        out.push(' ');
        out.push_str(&t.predicate);
        out.push(' ');
        out.push_str(&t.object);
        out.push_str(" .\n");
    }
    out
}

// ── Turtle serializer ─────────────────────────────────────────────────────────

/// Serialize a slice of RDF triples to [Turtle](https://www.w3.org/TR/turtle/) format.
///
/// Groups triples by subject and emits common prefix declarations.
pub fn serialize_turtle(triples: &[RdfTriple]) -> String {
    use std::collections::BTreeMap;

    let prefixes = [
        ("xsd:", "http://www.w3.org/2001/XMLSchema#"),
        ("rdf:", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
        ("rdfs:", "http://www.w3.org/2000/01/rdf-schema#"),
        ("owl:", "http://www.w3.org/2002/07/owl#"),
    ];

    let mut out = String::new();
    for (prefix, iri) in &prefixes {
        out.push_str(&format!("@prefix {} <{}> .\n", prefix, iri));
    }
    if !triples.is_empty() {
        out.push('\n');
    }

    // Group by subject (BTreeMap keeps output deterministic).
    let mut by_subject: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    for t in triples {
        by_subject
            .entry(&t.subject)
            .or_default()
            .push((&t.predicate, &t.object));
    }

    for (subj, preds) in &by_subject {
        out.push_str(subj);
        out.push('\n');
        for (i, (pred, obj)) in preds.iter().enumerate() {
            let terminator = if i == preds.len() - 1 { " ." } else { " ;" };
            out.push_str(&format!("    {} {}{}\n", pred, obj, terminator));
        }
        out.push('\n');
    }
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::value::Value;
    use uuid::Uuid;

    fn test_id() -> NodeId {
        NodeId(Uuid::parse_str("018e8c1e-1234-7000-8000-000000000001").unwrap())
    }

    #[test]
    fn node_id_to_iri_format() {
        let id = test_id();
        let iri = node_id_to_iri(&id);
        assert!(iri.starts_with("<urn:uuid:"));
        assert!(iri.ends_with('>'));
    }

    #[test]
    fn value_text_literal() {
        let lit = value_to_nt_literal(&Value::Text("hello world".to_string()));
        assert!(lit.contains("hello world"));
        assert!(lit.contains("XMLSchema#string"));
    }

    #[test]
    fn value_int_literal() {
        let lit = value_to_nt_literal(&Value::Int(42));
        assert!(lit.contains("42"));
        assert!(lit.contains("XMLSchema#integer"));
    }

    #[test]
    fn value_text_escaping() {
        let lit = value_to_nt_literal(&Value::Text("say \"hi\"\nnewline".to_string()));
        assert!(lit.contains("\\\"hi\\\""));
        assert!(lit.contains("\\n"));
    }

    #[test]
    fn serialize_ntriples_basic() {
        let triples = vec![RdfTriple {
            subject: "<urn:uuid:aaa>".to_string(),
            predicate: "<http://example.org/knows>".to_string(),
            object: "<urn:uuid:bbb>".to_string(),
        }];
        let out = serialize_ntriples(&triples);
        assert_eq!(out, "<urn:uuid:aaa> <http://example.org/knows> <urn:uuid:bbb> .\n");
    }

    #[test]
    fn serialize_ntriples_empty() {
        let out = serialize_ntriples(&[]);
        assert!(out.is_empty());
    }

    #[test]
    fn serialize_turtle_basic() {
        let triples = vec![
            RdfTriple {
                subject: "<urn:uuid:aaa>".to_string(),
                predicate: "<http://example.org/knows>".to_string(),
                object: "<urn:uuid:bbb>".to_string(),
            },
            RdfTriple {
                subject: "<urn:uuid:aaa>".to_string(),
                predicate: "<http://example.org/age>".to_string(),
                object: "\"30\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_string(),
            },
        ];
        let out = serialize_turtle(&triples);
        // Turtle groups by subject — both predicates appear under one subject block.
        assert!(out.contains("<urn:uuid:aaa>"));
        assert!(out.contains("<http://example.org/knows>"));
        assert!(out.contains("<http://example.org/age>"));
        // Subject appears exactly once (grouping).
        assert_eq!(out.matches("<urn:uuid:aaa>").count(), 1);
        // Prefix declarations present.
        assert!(out.contains("@prefix xsd:"));
    }

    #[test]
    fn serialize_turtle_multiple_subjects() {
        let triples = vec![
            RdfTriple {
                subject: "<urn:uuid:aaa>".to_string(),
                predicate: "<http://example.org/knows>".to_string(),
                object: "<urn:uuid:bbb>".to_string(),
            },
            RdfTriple {
                subject: "<urn:uuid:bbb>".to_string(),
                predicate: "<http://example.org/knows>".to_string(),
                object: "<urn:uuid:ccc>".to_string(),
            },
        ];
        let out = serialize_turtle(&triples);
        // Two separate subject blocks.
        assert!(out.contains("<urn:uuid:aaa>"));
        assert!(out.contains("<urn:uuid:bbb>"));
    }
}
