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
        Value::Int(n) => format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>", n),
        Value::Float(f) => format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#double>", f),
        Value::Bool(b) => format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#boolean>", b),
        Value::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#hexBinary>", hex)
        }
        Value::Vector(v) => {
            // Represent as a JSON-array string — not an RDF standard but best-effort.
            let s: Vec<String> = v.iter().map(|f| f.to_string()).collect();
            format!(
                "\"[{}]\"^^<http://www.w3.org/2001/XMLSchema#string>",
                s.join(",")
            )
        }
        Value::Null => "\"\"^^<http://www.w3.org/2001/XMLSchema#string>".to_string(),
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
    QuotedTriple { s: String, p: String, o: String },
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
                    t.predicate,
                    t.object,
                    s = s,
                    p = p,
                    o = o,
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

// ── JSON-LD serializer ────────────────────────────────────────────────────────

/// Serialize a slice of RDF triples to [JSON-LD](https://www.w3.org/TR/json-ld11/)
/// `@graph` format.
///
/// Triples are grouped by subject. Relation objects become `{ "@id": "…" }`;
/// property objects are parsed from N-Triples literal syntax into
/// `{ "@value": …, "@type": "xsd:…" }`.
pub fn serialize_jsonld(triples: &[RdfTriple]) -> String {
    use std::collections::BTreeMap;

    let context = serde_json::json!({
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "owl": "http://www.w3.org/2002/07/owl#"
    });

    // Group by subject (BTreeMap for deterministic output).
    let mut by_subject: BTreeMap<String, Vec<(&str, &str)>> = BTreeMap::new();
    for t in triples {
        by_subject
            .entry(t.subject.clone())
            .or_default()
            .push((&t.predicate, &t.object));
    }

    let mut graph: Vec<serde_json::Value> = Vec::new();

    for (subject, preds) in &by_subject {
        let mut node = serde_json::Map::new();
        // Strip angle brackets from IRI subjects.
        let id = strip_brackets(subject);
        node.insert("@id".to_string(), serde_json::Value::String(id.to_string()));

        for (pred, obj) in preds {
            let pred_iri = strip_brackets(pred).to_string();
            let obj_val = nt_object_to_jsonld(obj);
            // Collect multiple objects for the same predicate into an array.
            match node.get_mut(&pred_iri) {
                Some(existing) if existing.is_array() => {
                    existing.as_array_mut().unwrap().push(obj_val);
                }
                Some(existing) => {
                    let prev = existing.clone();
                    *existing = serde_json::Value::Array(vec![prev, obj_val]);
                }
                None => {
                    node.insert(pred_iri, obj_val);
                }
            }
        }

        graph.push(serde_json::Value::Object(node));
    }

    serde_json::json!({
        "@context": context,
        "@graph": graph,
    })
    .to_string()
}

/// Convert an N-Triples-syntax object string to a JSON-LD value.
///
/// - `<iri>` → `{ "@id": "iri" }`
/// - `"lit"^^<type>` → `{ "@value": lit, "@type": "xsd:…" }`
/// - `"lit"` → `{ "@value": lit, "@type": "xsd:string" }`
fn nt_object_to_jsonld(obj: &str) -> serde_json::Value {
    if obj.starts_with('<') {
        // IRI
        let iri = strip_brackets(obj);
        return serde_json::json!({ "@id": iri });
    }
    if let Some(rest) = obj.strip_prefix('"') {
        // Literal — find closing quote
        if let Some(close) = rest.find('"') {
            let value_str = &rest[..close];
            let after = &rest[close + 1..];
            // Datatype or language tag
            let (type_str, coerced) = if let Some(dt_part) = after.strip_prefix("^^<") {
                // Typed literal: ^^<http://...>
                let dt_iri = dt_part.trim_end_matches('>');
                let xsd_prefix = "http://www.w3.org/2001/XMLSchema#";
                let short = if let Some(local) = dt_iri.strip_prefix(xsd_prefix) {
                    format!("xsd:{}", local)
                } else {
                    dt_iri.to_string()
                };
                let json_val = coerce_xsd_value(value_str, dt_iri);
                (short, json_val)
            } else if let Some(lang_part) = after.strip_prefix('@') {
                let lang = lang_part.trim();
                (
                    format!("rdf:langString@{}", lang),
                    serde_json::Value::String(value_str.to_string()),
                )
            } else {
                (
                    "xsd:string".to_string(),
                    serde_json::Value::String(value_str.to_string()),
                )
            };
            return serde_json::json!({ "@value": coerced, "@type": type_str });
        }
    }
    // Fallback: return as plain string
    serde_json::Value::String(obj.to_string())
}

/// Attempt to coerce an XSD literal string into a native JSON type.
fn coerce_xsd_value(s: &str, dt_iri: &str) -> serde_json::Value {
    match dt_iri {
        "http://www.w3.org/2001/XMLSchema#integer"
        | "http://www.w3.org/2001/XMLSchema#long"
        | "http://www.w3.org/2001/XMLSchema#int"
        | "http://www.w3.org/2001/XMLSchema#short"
        | "http://www.w3.org/2001/XMLSchema#byte" => {
            if let Ok(n) = s.parse::<i64>() {
                return serde_json::Value::Number(serde_json::Number::from(n));
            }
        }
        "http://www.w3.org/2001/XMLSchema#double"
        | "http://www.w3.org/2001/XMLSchema#float"
        | "http://www.w3.org/2001/XMLSchema#decimal" => {
            if let Ok(f) = s.parse::<f64>() {
                if let Some(n) = serde_json::Number::from_f64(f) {
                    return serde_json::Value::Number(n);
                }
            }
        }
        "http://www.w3.org/2001/XMLSchema#boolean" => match s {
            "true" | "1" => return serde_json::Value::Bool(true),
            "false" | "0" => return serde_json::Value::Bool(false),
            _ => {}
        },
        _ => {}
    }
    serde_json::Value::String(s.to_string())
}

/// Strip surrounding angle brackets from an IRI string: `<iri>` → `iri`.
pub fn strip_brackets(s: &str) -> &str {
    s.strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(s)
}

// ── Schema RDF serializer / parser ────────────────────────────────────────────

/// A simplified view of a node type for schema RDF serialization.
#[derive(Debug, Clone)]
pub struct SchemaNodeType {
    pub type_name: String,
    pub fields: Vec<SchemaField>,
    pub parent_types: Vec<String>,
}

/// A simplified view of an edge type for schema RDF serialization.
#[derive(Debug, Clone)]
pub struct SchemaEdgeType {
    pub predicate: String,
    pub domain: String,
    pub range: String,
    pub fields: Vec<SchemaField>,
}

/// A field within a node or edge type.
#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: String,
    /// One of: "bool", "int", "float", "text", "blob", "vector".
    pub kind: String,
    pub required: bool,
}

const TYPE_BASE: &str = "urn:polargraph:type:";
const PROP_BASE: &str = "urn:polargraph:prop:";
const REL_BASE: &str = "urn:polargraph:rel:";

fn field_kind_to_xsd(kind: &str) -> &str {
    match kind {
        "int" | "integer" => "http://www.w3.org/2001/XMLSchema#integer",
        "float" | "double" => "http://www.w3.org/2001/XMLSchema#double",
        "bool" | "boolean" => "http://www.w3.org/2001/XMLSchema#boolean",
        "blob" => "http://www.w3.org/2001/XMLSchema#hexBinary",
        _ => "http://www.w3.org/2001/XMLSchema#string",
    }
}

fn xsd_to_field_kind(xsd: &str) -> &str {
    match xsd {
        "http://www.w3.org/2001/XMLSchema#integer" => "int",
        "http://www.w3.org/2001/XMLSchema#double"
        | "http://www.w3.org/2001/XMLSchema#float"
        | "http://www.w3.org/2001/XMLSchema#decimal" => "float",
        "http://www.w3.org/2001/XMLSchema#boolean" => "bool",
        "http://www.w3.org/2001/XMLSchema#hexBinary" => "blob",
        _ => "text",
    }
}

/// Serialize PolarGraph node/edge type definitions as OWL/RDFS Turtle.
///
/// The Turtle can be re-imported via [`parse_schema_rdf`] to register the same
/// types on another PolarGraph instance (schema round-trip).
pub fn serialize_schema_rdf(
    node_types: &[SchemaNodeType],
    edge_types: &[SchemaEdgeType],
) -> String {
    let mut out = String::new();
    out.push_str("@prefix owl:  <http://www.w3.org/2002/07/owl#> .\n");
    out.push_str("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n");
    out.push_str("@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .\n");
    out.push_str("@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n");
    out.push('\n');

    for nt in node_types {
        let class_iri = format!("<{}{}>", TYPE_BASE, nt.type_name);
        out.push_str(&format!("{} a owl:Class", class_iri));
        if !nt.parent_types.is_empty() {
            let parents: Vec<String> = nt
                .parent_types
                .iter()
                .map(|p| format!("<{}{}>", TYPE_BASE, p))
                .collect();
            out.push_str(&format!(" ;\n    rdfs:subClassOf {}", parents.join(", ")));
        }
        out.push_str(" .\n");

        for field in &nt.fields {
            let prop_iri = format!("<{}{}/{}>", PROP_BASE, nt.type_name, field.name);
            let xsd_range = field_kind_to_xsd(&field.kind);
            out.push_str(&format!(
                "{} a owl:DatatypeProperty ;\n    rdfs:domain {} ;\n    rdfs:range <{}> .\n",
                prop_iri, class_iri, xsd_range
            ));
        }
    }

    for et in edge_types {
        let rel_iri = format!("<{}{}>", REL_BASE, et.predicate);
        let mut parts = Vec::new();
        if !et.domain.is_empty() {
            parts.push(format!("    rdfs:domain <{}{}>", TYPE_BASE, et.domain));
        }
        if !et.range.is_empty() {
            parts.push(format!("    rdfs:range <{}{}>", TYPE_BASE, et.range));
        }
        if parts.is_empty() {
            out.push_str(&format!("{} a owl:ObjectProperty .\n", rel_iri));
        } else {
            out.push_str(&format!(
                "{} a owl:ObjectProperty ;\n{} .\n",
                rel_iri,
                parts.join(" ;\n")
            ));
        }

        for field in &et.fields {
            let prop_iri = format!("<{}{}/{}>", PROP_BASE, et.predicate, field.name);
            let xsd_range = field_kind_to_xsd(&field.kind);
            out.push_str(&format!(
                "{} a owl:DatatypeProperty ;\n    rdfs:domain {} ;\n    rdfs:range <{}> .\n",
                prop_iri, rel_iri, xsd_range
            ));
        }
    }

    out
}

/// Parse OWL/RDFS Turtle (as produced by [`serialize_schema_rdf`]) into
/// [`SchemaNodeType`] and [`SchemaEdgeType`] structures.
///
/// Recognised triple patterns:
/// - `<urn:polargraph:type:X> rdf:type owl:Class` → NodeType X
/// - `<urn:polargraph:rel:P> rdf:type owl:ObjectProperty` → EdgeType P
/// - `<urn:polargraph:prop:X/F> rdf:type owl:DatatypeProperty ; rdfs:domain … ; rdfs:range xsd:…`
pub fn parse_schema_rdf(
    input: &[u8],
) -> Result<(Vec<SchemaNodeType>, Vec<SchemaEdgeType>), String> {
    use crate::rdf_import::parse_turtle;
    use std::collections::HashMap;

    let triples = parse_turtle(input)?;

    // Collect raw triples as (subject_iri, predicate_iri, object_iri_or_str)
    let owl_class = "http://www.w3.org/2002/07/owl#Class";
    let owl_obj_prop = "http://www.w3.org/2002/07/owl#ObjectProperty";
    let owl_dt_prop = "http://www.w3.org/2002/07/owl#DatatypeProperty";
    let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let rdfs_domain = "http://www.w3.org/2000/01/rdf-schema#domain";
    let rdfs_range = "http://www.w3.org/2000/01/rdf-schema#range";
    let rdfs_subclass = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

    // Collect rdf:type declarations.
    let mut class_iris: Vec<String> = Vec::new();
    let mut obj_prop_iris: Vec<String> = Vec::new();
    let mut dt_prop_domains: HashMap<String, String> = HashMap::new();
    let mut dt_prop_ranges: HashMap<String, String> = HashMap::new();
    let mut obj_prop_domains: HashMap<String, String> = HashMap::new();
    let mut obj_prop_ranges: HashMap<String, String> = HashMap::new();
    let mut class_parents: HashMap<String, Vec<String>> = HashMap::new();

    for t in &triples {
        let obj_iri = match &t.object {
            crate::rdf_import::ImportedObject::Iri(s) => s.as_str(),
            _ => continue,
        };
        if t.predicate == rdf_type {
            match obj_iri {
                s if s == owl_class => class_iris.push(t.subject.clone()),
                s if s == owl_obj_prop => obj_prop_iris.push(t.subject.clone()),
                s if s == owl_dt_prop => { /* collected via domain/range */ }
                _ => {}
            }
        } else if t.predicate == rdfs_domain {
            if t.subject.starts_with(PROP_BASE) {
                dt_prop_domains.insert(t.subject.clone(), obj_iri.to_string());
            } else if t.subject.starts_with(REL_BASE) {
                obj_prop_domains.insert(t.subject.clone(), obj_iri.to_string());
            }
        } else if t.predicate == rdfs_range {
            if t.subject.starts_with(PROP_BASE) {
                dt_prop_ranges.insert(t.subject.clone(), obj_iri.to_string());
            } else if t.subject.starts_with(REL_BASE) {
                obj_prop_ranges.insert(t.subject.clone(), obj_iri.to_string());
            }
        } else if t.predicate == rdfs_subclass && t.subject.starts_with(TYPE_BASE) {
            if let Some(parent_name) = obj_iri.strip_prefix(TYPE_BASE) {
                class_parents
                    .entry(t.subject.clone())
                    .or_default()
                    .push(parent_name.to_string());
            }
        }
    }

    // Build node types.
    let mut node_types: Vec<SchemaNodeType> = class_iris
        .iter()
        .filter_map(|iri| {
            let type_name = iri.strip_prefix(TYPE_BASE)?.to_string();
            Some(SchemaNodeType {
                type_name: type_name.clone(),
                fields: Vec::new(),
                parent_types: class_parents.get(iri).cloned().unwrap_or_default(),
            })
        })
        .collect();

    // Attach DataypeProperty fields to node types.
    for t in &triples {
        if t.predicate != rdf_type {
            continue;
        }
        let obj_iri = match &t.object {
            crate::rdf_import::ImportedObject::Iri(s) => s.as_str(),
            _ => continue,
        };
        if obj_iri != owl_dt_prop {
            continue;
        }
        // t.subject is the property IRI, e.g. urn:polargraph:prop:Person/name
        let prop_iri = &t.subject;
        if let Some(rest) = prop_iri.strip_prefix(PROP_BASE) {
            // rest is "TypeName/fieldName" or "predicate/fieldName"
            if let Some(slash) = rest.find('/') {
                let owner = &rest[..slash];
                let field_name = &rest[slash + 1..];
                let range = dt_prop_ranges
                    .get(prop_iri)
                    .map(|s| s.as_str())
                    .unwrap_or("");
                let kind = xsd_to_field_kind(range).to_string();
                let field = SchemaField {
                    name: field_name.to_string(),
                    kind,
                    required: false,
                };

                // Try to attach to a matching node type first.
                if let Some(nt) = node_types.iter_mut().find(|n| n.type_name == owner) {
                    nt.fields.push(field);
                }
                // (Edge type fields are handled in the edge type loop below.)
            }
        }
    }

    // Build edge types.
    let mut edge_types: Vec<SchemaEdgeType> = obj_prop_iris
        .iter()
        .filter_map(|iri| {
            let predicate = iri.strip_prefix(REL_BASE)?.to_string();
            let domain = obj_prop_domains
                .get(iri)
                .and_then(|d| d.strip_prefix(TYPE_BASE))
                .unwrap_or("")
                .to_string();
            let range = obj_prop_ranges
                .get(iri)
                .and_then(|r| r.strip_prefix(TYPE_BASE))
                .unwrap_or("")
                .to_string();
            Some(SchemaEdgeType {
                predicate: predicate.clone(),
                domain,
                range,
                fields: Vec::new(),
            })
        })
        .collect();

    // Attach DatatypeProperty fields to edge types.
    for t in &triples {
        if t.predicate != rdf_type {
            continue;
        }
        let obj_iri = match &t.object {
            crate::rdf_import::ImportedObject::Iri(s) => s.as_str(),
            _ => continue,
        };
        if obj_iri != owl_dt_prop {
            continue;
        }
        let prop_iri = &t.subject;
        if let Some(rest) = prop_iri.strip_prefix(PROP_BASE) {
            if let Some(slash) = rest.find('/') {
                let owner = &rest[..slash];
                let field_name = &rest[slash + 1..];
                if let Some(et) = edge_types.iter_mut().find(|e| e.predicate == owner) {
                    let range = dt_prop_ranges
                        .get(prop_iri)
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    et.fields.push(SchemaField {
                        name: field_name.to_string(),
                        kind: xsd_to_field_kind(range).to_string(),
                        required: false,
                    });
                }
            }
        }
    }

    Ok((node_types, edge_types))
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
        assert_eq!(
            out,
            "<urn:uuid:aaa> <http://example.org/knows> <urn:uuid:bbb> .\n"
        );
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
