//! RDF interoperability tests.
//!
//! These are pure library tests (no running server required) that verify the
//! parse ↔ serialize round-trips and structural correctness of all supported
//! RDF formats.

use polargraph_core::value::Value;
use polargraph_sparql::{
    parse_jsonld, parse_ntriples, parse_schema_rdf, parse_turtle, serialize_jsonld,
    serialize_ntriples, serialize_schema_rdf, serialize_turtle, ImportedObject, RdfTriple,
    SchemaEdgeType, SchemaField, SchemaNodeType,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn alice_knows_bob_nt() -> &'static [u8] {
    b"<http://example.org/Alice> <http://schema.org/knows> <http://example.org/Bob> .\n\
      <http://example.org/Alice> <http://schema.org/name> \"Alice\"^^<http://www.w3.org/2001/XMLSchema#string> .\n\
      <http://example.org/Bob> <http://schema.org/name> \"Bob\"^^<http://www.w3.org/2001/XMLSchema#string> .\n\
      <http://example.org/Alice> <http://schema.org/age> \"30\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n\
      <http://example.org/Bob> <http://schema.org/age> \"25\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n"
}

// ── Test 1: N-Triples round-trip ──────────────────────────────────────────────

#[test]
fn roundtrip_ntriples() {
    let input = alice_knows_bob_nt();
    let parsed = parse_ntriples(input).expect("parse_ntriples failed");

    assert_eq!(parsed.len(), 5, "expected 5 triples");

    // Reconstruct RdfTriple objects for serialization.
    let rdf_triples: Vec<RdfTriple> = parsed
        .iter()
        .filter_map(|t| {
            let subject = format!("<{}>", t.subject);
            let predicate = format!("<{}>", t.predicate);
            let object = match &t.object {
                ImportedObject::Iri(iri) => format!("<{}>", iri),
                ImportedObject::Literal { value, datatype } => {
                    let val_str = match value {
                        Value::Text(s) => format!("\"{}\"", s),
                        Value::Int(n) => format!("\"{}\"", n),
                        Value::Float(f) => format!("\"{}\"", f),
                        Value::Bool(b) => format!("\"{}\"", b),
                        _ => return None,
                    };
                    format!("{}^^<{}>", val_str, datatype)
                }
                ImportedObject::BlankNode(id) => format!("_:{}", id),
            };
            Some(RdfTriple {
                subject,
                predicate,
                object,
            })
        })
        .collect();

    let serialized = serialize_ntriples(&rdf_triples);

    // Re-parse the serialized output.
    let re_parsed = parse_ntriples(serialized.as_bytes()).expect("re-parse failed");
    assert_eq!(
        re_parsed.len(),
        parsed.len(),
        "triple count mismatch after round-trip"
    );

    // Verify all original subjects appear in re-parsed output.
    let orig_subjects: std::collections::HashSet<&str> =
        parsed.iter().map(|t| t.subject.as_str()).collect();
    let re_subjects: std::collections::HashSet<&str> =
        re_parsed.iter().map(|t| t.subject.as_str()).collect();
    assert_eq!(
        orig_subjects, re_subjects,
        "subjects changed after round-trip"
    );
}

// ── Test 2: Turtle round-trip ─────────────────────────────────────────────────

#[test]
fn roundtrip_turtle() {
    let ttl = b"@prefix ex: <http://example.org/> .\n\
                @prefix schema: <http://schema.org/> .\n\
                ex:Alice schema:knows ex:Bob .\n\
                ex:Alice schema:name \"Alice\" .\n\
                ex:Bob   schema:name \"Bob\" .\n";

    let parsed = parse_turtle(ttl).expect("parse_turtle failed");
    assert_eq!(parsed.len(), 3);

    // Serialize as Turtle via RdfTriple.
    let rdf_triples: Vec<RdfTriple> = parsed
        .iter()
        .map(|t| {
            let subject = format!("<{}>", t.subject);
            let predicate = format!("<{}>", t.predicate);
            let object = match &t.object {
                ImportedObject::Iri(iri) => format!("<{}>", iri),
                ImportedObject::Literal {
                    value: Value::Text(s),
                    ..
                } => {
                    format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#string>", s)
                }
                ImportedObject::Literal { value, .. } => format!("{:?}", value),
                ImportedObject::BlankNode(id) => format!("_:{}", id),
            };
            RdfTriple {
                subject,
                predicate,
                object,
            }
        })
        .collect();

    let turtle_out = serialize_turtle(&rdf_triples);
    assert!(
        turtle_out.contains("@prefix xsd:"),
        "should have prefix declarations"
    );
    assert!(
        turtle_out.contains("http://example.org/Alice"),
        "should contain Alice"
    );
    assert!(
        turtle_out.contains("http://schema.org/knows"),
        "should contain knows predicate"
    );

    // Re-parse from Turtle.
    let re_parsed = parse_turtle(turtle_out.as_bytes()).expect("re-parse Turtle failed");
    assert_eq!(re_parsed.len(), parsed.len(), "triple count mismatch");

    // All subjects should survive.
    let orig_predicates: std::collections::HashSet<&str> =
        parsed.iter().map(|t| t.predicate.as_str()).collect();
    let re_predicates: std::collections::HashSet<&str> =
        re_parsed.iter().map(|t| t.predicate.as_str()).collect();
    assert_eq!(orig_predicates, re_predicates);
}

// ── Test 3: JSON-LD export structure ─────────────────────────────────────────

#[test]
fn jsonld_export_structure() {
    let triples = vec![
        RdfTriple {
            subject: "<urn:uuid:aaa>".to_string(),
            predicate: "<http://schema.org/knows>".to_string(),
            object: "<urn:uuid:bbb>".to_string(),
        },
        RdfTriple {
            subject: "<urn:uuid:aaa>".to_string(),
            predicate: "<http://schema.org/name>".to_string(),
            object: "\"Alice\"^^<http://www.w3.org/2001/XMLSchema#string>".to_string(),
        },
        RdfTriple {
            subject: "<urn:uuid:bbb>".to_string(),
            predicate: "<http://schema.org/name>".to_string(),
            object: "\"Bob\"^^<http://www.w3.org/2001/XMLSchema#string>".to_string(),
        },
    ];

    let jsonld = serialize_jsonld(&triples);
    let doc: serde_json::Value = serde_json::from_str(&jsonld).expect("invalid JSON");

    // Must have @context and @graph.
    assert!(doc.get("@context").is_some(), "missing @context");
    let graph = doc
        .get("@graph")
        .expect("missing @graph")
        .as_array()
        .expect("@graph not array");
    assert_eq!(graph.len(), 2, "expected 2 subject groups");

    // Find Alice's node.
    let alice = graph
        .iter()
        .find(|n| n.get("@id").and_then(|v| v.as_str()) == Some("urn:uuid:aaa"))
        .expect("Alice node not found");

    // knows predicate → { "@id": "urn:uuid:bbb" }
    let knows = alice
        .get("http://schema.org/knows")
        .expect("knows predicate missing");
    assert_eq!(
        knows.get("@id").and_then(|v| v.as_str()),
        Some("urn:uuid:bbb")
    );

    // name predicate → { "@value": ..., "@type": "xsd:string" }
    let name = alice
        .get("http://schema.org/name")
        .expect("name predicate missing");
    assert!(name.get("@value").is_some(), "@value missing");
    let type_str = name.get("@type").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        type_str.contains("string"),
        "expected xsd:string type, got: {}",
        type_str
    );
}

// ── Test 4: JSON-LD import ────────────────────────────────────────────────────

#[test]
fn jsonld_import() {
    let jsonld = r#"{
        "@context": { "xsd": "http://www.w3.org/2001/XMLSchema#" },
        "@graph": [
            {
                "@id": "urn:uuid:alice",
                "http://schema.org/knows": { "@id": "urn:uuid:bob" },
                "http://schema.org/name": { "@value": "Alice", "@type": "xsd:string" },
                "http://schema.org/age":  { "@value": "30",    "@type": "xsd:integer" },
                "http://schema.org/score":{ "@value": "9.5",   "@type": "xsd:double" }
            }
        ]
    }"#;

    let triples = parse_jsonld(jsonld).expect("parse_jsonld failed");

    // 4 predicates on Alice.
    assert_eq!(triples.len(), 4);

    let knows = triples
        .iter()
        .find(|t| t.predicate.contains("knows"))
        .unwrap();
    assert!(matches!(&knows.object, ImportedObject::Iri(s) if s == "urn:uuid:bob"));

    let name_t = triples
        .iter()
        .find(|t| t.predicate.contains("name"))
        .unwrap();
    assert!(matches!(
        &name_t.object,
        ImportedObject::Literal { value: Value::Text(s), .. } if s == "Alice"
    ));

    let age_t = triples
        .iter()
        .find(|t| t.predicate.contains("age"))
        .unwrap();
    assert!(matches!(
        &age_t.object,
        ImportedObject::Literal {
            value: Value::Int(30),
            ..
        }
    ));

    let score_t = triples
        .iter()
        .find(|t| t.predicate.contains("score"))
        .unwrap();
    assert!(matches!(
        &score_t.object,
        ImportedObject::Literal {
            value: Value::Float(_),
            ..
        }
    ));
}

// ── Test 5: Subgraph export → import (N-Triples) ─────────────────────────────

#[test]
fn subgraph_export_import() {
    // Simulate what the subgraph export handler produces.
    let triples = vec![
        RdfTriple {
            subject: "<urn:uuid:alice>".to_string(),
            predicate: "<http://schema.org/knows>".to_string(),
            object: "<urn:uuid:bob>".to_string(),
        },
        RdfTriple {
            subject: "<urn:uuid:alice>".to_string(),
            predicate: "<http://schema.org/name>".to_string(),
            object: "\"Alice\"^^<http://www.w3.org/2001/XMLSchema#string>".to_string(),
        },
        // Edge annotation represented as a regular triple on the edge node.
        RdfTriple {
            subject: "<urn:uuid:edge-001>".to_string(),
            predicate: "<http://schema.org/since>".to_string(),
            object: "\"2024-01-01\"^^<http://www.w3.org/2001/XMLSchema#string>".to_string(),
        },
    ];

    // Export to N-Triples.
    let nt_export = serialize_ntriples(&triples);
    assert!(!nt_export.is_empty());

    // Import from the exported N-Triples.
    let imported = parse_ntriples(nt_export.as_bytes()).expect("import failed");
    assert_eq!(imported.len(), triples.len(), "triple count mismatch");

    // All subjects must be preserved.
    let export_subjects: std::collections::HashSet<String> = triples
        .iter()
        .map(|t| {
            // Strip angle brackets.
            t.subject.trim_matches(|c| c == '<' || c == '>').to_string()
        })
        .collect();
    let import_subjects: std::collections::HashSet<String> =
        imported.iter().map(|t| t.subject.clone()).collect();
    assert_eq!(export_subjects, import_subjects);

    // Verify the relation triple.
    let rel = imported
        .iter()
        .find(|t| t.predicate.contains("knows"))
        .unwrap();
    assert!(matches!(&rel.object, ImportedObject::Iri(s) if s == "urn:uuid:bob"));

    // Verify the property triple.
    let prop = imported
        .iter()
        .find(|t| t.predicate.contains("name"))
        .unwrap();
    assert!(matches!(
        &prop.object,
        ImportedObject::Literal { value: Value::Text(s), .. } if s == "Alice"
    ));

    // Verify the annotation triple.
    let ann = imported
        .iter()
        .find(|t| t.predicate.contains("since"))
        .unwrap();
    assert!(matches!(
        &ann.object,
        ImportedObject::Literal { value: Value::Text(s), .. } if s == "2024-01-01"
    ));
}

// ── Test 6: Schema OWL/RDFS round-trip ───────────────────────────────────────

#[test]
fn schema_rdf_roundtrip() {
    let node_types = vec![
        SchemaNodeType {
            type_name: "Person".to_string(),
            fields: vec![
                SchemaField {
                    name: "name".to_string(),
                    kind: "text".to_string(),
                    required: true,
                },
                SchemaField {
                    name: "age".to_string(),
                    kind: "int".to_string(),
                    required: false,
                },
            ],
            parent_types: vec![],
        },
        SchemaNodeType {
            type_name: "Company".to_string(),
            fields: vec![SchemaField {
                name: "name".to_string(),
                kind: "text".to_string(),
                required: true,
            }],
            parent_types: vec![],
        },
    ];

    let edge_types = vec![SchemaEdgeType {
        predicate: "works_at".to_string(),
        domain: "Person".to_string(),
        range: "Company".to_string(),
        fields: vec![SchemaField {
            name: "since".to_string(),
            kind: "text".to_string(),
            required: false,
        }],
    }];

    let turtle_out = serialize_schema_rdf(&node_types, &edge_types);

    // Structural checks on the Turtle output.
    assert!(turtle_out.contains("owl:Class"), "missing owl:Class");
    assert!(
        turtle_out.contains("urn:polargraph:type:Person"),
        "missing Person type IRI"
    );
    assert!(
        turtle_out.contains("urn:polargraph:type:Company"),
        "missing Company type IRI"
    );
    assert!(
        turtle_out.contains("owl:ObjectProperty"),
        "missing owl:ObjectProperty"
    );
    assert!(
        turtle_out.contains("urn:polargraph:rel:works_at"),
        "missing works_at IRI"
    );
    assert!(turtle_out.contains("rdfs:domain"), "missing rdfs:domain");
    assert!(turtle_out.contains("rdfs:range"), "missing rdfs:range");
    assert!(
        turtle_out.contains("owl:DatatypeProperty"),
        "missing owl:DatatypeProperty"
    );
    assert!(
        turtle_out.contains("urn:polargraph:prop:Person/name"),
        "missing Person/name prop"
    );

    // Parse the Turtle back and verify the schema structure.
    let (re_nodes, re_edges) =
        parse_schema_rdf(turtle_out.as_bytes()).expect("parse_schema_rdf failed");

    assert_eq!(
        re_nodes.len(),
        2,
        "expected 2 node types, got {}",
        re_nodes.len()
    );
    assert_eq!(
        re_edges.len(),
        1,
        "expected 1 edge type, got {}",
        re_edges.len()
    );

    let person = re_nodes
        .iter()
        .find(|n| n.type_name == "Person")
        .expect("Person not found");
    assert_eq!(person.fields.len(), 2, "Person should have 2 fields");
    assert!(
        person.fields.iter().any(|f| f.name == "name"),
        "name field missing"
    );
    assert!(
        person
            .fields
            .iter()
            .any(|f| f.name == "age" && f.kind == "int"),
        "age int field missing"
    );

    let edge = &re_edges[0];
    assert_eq!(edge.predicate, "works_at");
    assert_eq!(edge.domain, "Person");
    assert_eq!(edge.range, "Company");
}
