//! Conversions between protobuf wire types and Rust domain types.
//!
//! Every function that can fail returns `Result<_, tonic::Status>` so callers
//! can propagate errors directly as gRPC status codes.

#![allow(clippy::result_large_err)]

use crate::proto::{
    self, edge_annotation::Value as AnnotationValueProto, term::Kind as TermKind,
    triple::Kind as TripleKind, value::Kind as ValueKind,
};
use polargraph_core::{
    id::{EdgeId, NodeId},
    schema::{
        Cardinality, EdgeTypeDef, FieldDef, FieldKind, NodeTypeDef, StorageMode, VectorSpaceDef,
    },
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use polargraph_query::datalog::{Bindings, Rule, Term, VarPattern};
use polargraph_storage::{EdgeAnnotation, EdgeAnnotationValue};
use tonic::Status;
use uuid::Uuid;

// ── NodeId ────────────────────────────────────────────────────────────────────

pub fn node_id_from_proto(proto: &proto::NodeId) -> Result<NodeId, Status> {
    let bytes: [u8; 16] = proto
        .bytes
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("NodeId must be exactly 16 bytes"))?;
    Ok(NodeId(Uuid::from_bytes(bytes)))
}

pub fn node_id_to_proto(id: NodeId) -> proto::NodeId {
    proto::NodeId {
        bytes: id.as_bytes().to_vec(),
    }
}

// ── Value ─────────────────────────────────────────────────────────────────────

pub fn value_from_proto(proto: &proto::Value) -> Result<Value, Status> {
    match &proto.kind {
        Some(ValueKind::NullVal(_)) | None => Ok(Value::Null),
        Some(ValueKind::BoolVal(b)) => Ok(Value::Bool(*b)),
        Some(ValueKind::IntVal(i)) => Ok(Value::Int(*i)),
        Some(ValueKind::FloatVal(f)) => Ok(Value::Float(*f)),
        Some(ValueKind::TextVal(s)) => Ok(Value::Text(s.clone())),
        Some(ValueKind::BlobVal(b)) => Ok(Value::Blob(b.clone())),
        Some(ValueKind::VecVal(fa)) => Ok(Value::Vector(fa.values.clone())),
    }
}

pub fn value_to_proto(v: &Value) -> proto::Value {
    let kind = match v {
        Value::Null => ValueKind::NullVal(true),
        Value::Bool(b) => ValueKind::BoolVal(*b),
        Value::Int(i) => ValueKind::IntVal(*i),
        Value::Float(f) => ValueKind::FloatVal(*f),
        Value::Text(s) => ValueKind::TextVal(s.clone()),
        Value::Blob(b) => ValueKind::BlobVal(b.clone()),
        Value::Vector(fs) => ValueKind::VecVal(proto::FloatArray { values: fs.clone() }),
    };
    proto::Value { kind: Some(kind) }
}

// ── BiTemporalRange ───────────────────────────────────────────────────────────

pub fn temporal_from_proto(vt_start: i64, vt_end: i64) -> BiTemporalRange {
    BiTemporalRange {
        vt_start: Timestamp(vt_start),
        vt_end: if vt_end == 0 {
            Timestamp::END_OF_TIME
        } else {
            Timestamp(vt_end)
        },
        tt: Timestamp::now(), // filled in by the transaction layer at commit
    }
}

// ── Triple ────────────────────────────────────────────────────────────────────

/// Convert a single proto Triple to a single Rust Triple.
///
/// For `RelationTriple` this always generates a fresh `EdgeId` and ignores any
/// `properties` field. Use [`triples_from_proto`] instead when you need edge
/// properties to be expanded and/or need the `EdgeId` returned.
pub fn triple_from_proto(proto: &proto::Triple) -> Result<Triple, Status> {
    Ok(triples_from_proto(proto)?.0.remove(0))
}

/// Convert a proto Triple into one or more Rust Triples, returning the edge UUID
/// for `RelationTriple` inputs so callers can store or surface it.
///
/// A `RelationTriple` with populated `properties` expands to:
///   - one `Triple::Relation` (the edge itself)
///   - one `Triple::Property` per edge property, with `subject = NodeId(edge_id)`
///
/// Returns `(triples, edge_id)` where `edge_id` is `Some` only for relation triples.
pub fn triples_from_proto(proto: &proto::Triple) -> Result<(Vec<Triple>, Option<EdgeId>), Status> {
    match &proto.kind {
        Some(TripleKind::Relation(r)) => {
            let subject = node_id_from_proto(
                r.subject
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("relation missing subject"))?,
            )?;
            let object = node_id_from_proto(
                r.object
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("relation missing object"))?,
            )?;
            if r.predicate.is_empty() {
                return Err(Status::invalid_argument(
                    "relation predicate must not be empty",
                ));
            }
            let edge_id = EdgeId::new();
            let temporal = temporal_from_proto(r.vt_start, r.vt_end);
            let relation = Triple::Relation {
                subject,
                predicate: Predicate::new(r.predicate.clone()),
                object,
                edge_id,
                temporal,
            };
            // Expand edge properties: stored as Property triples whose subject is
            // NodeId(edge_id.0). Clients retrieve them by querying that UUID.
            let mut triples = vec![relation];
            for ep in &r.properties {
                if ep.name.is_empty() {
                    return Err(Status::invalid_argument(
                        "edge property name must not be empty",
                    ));
                }
                let value = value_from_proto(
                    ep.value
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("edge property missing value"))?,
                )?;
                triples.push(Triple::Property {
                    subject: NodeId(edge_id.0),
                    predicate: Predicate::new(ep.name.clone()),
                    value,
                    temporal,
                });
            }
            Ok((triples, Some(edge_id)))
        }
        Some(TripleKind::Property(p)) => {
            let subject = node_id_from_proto(
                p.subject
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("property missing subject"))?,
            )?;
            if p.predicate.is_empty() {
                return Err(Status::invalid_argument(
                    "property predicate must not be empty",
                ));
            }
            let value = value_from_proto(
                p.value
                    .as_ref()
                    .ok_or_else(|| Status::invalid_argument("property missing value"))?,
            )?;
            Ok((
                vec![Triple::Property {
                    subject,
                    predicate: Predicate::new(p.predicate.clone()),
                    value,
                    temporal: temporal_from_proto(p.vt_start, p.vt_end),
                }],
                None,
            ))
        }
        None => Err(Status::invalid_argument("triple has no kind set")),
    }
}

// ── Term / VarPattern ─────────────────────────────────────────────────────────

pub fn term_from_proto(proto: &proto::Term) -> Result<Term, Status> {
    match &proto.kind {
        Some(TermKind::Bound(id)) => Ok(Term::Bound(node_id_from_proto(id)?)),
        Some(TermKind::Var(name)) => {
            if name.is_empty() {
                return Err(Status::invalid_argument("variable name must not be empty"));
            }
            Ok(Term::Var(name.clone()))
        }
        None => Ok(Term::Any),
    }
}

pub fn var_pattern_from_proto(proto: &proto::VarPattern) -> Result<VarPattern, Status> {
    let subject = match &proto.subject {
        Some(t) => term_from_proto(t)?,
        None => Term::Any,
    };
    let object = match &proto.object {
        Some(t) => term_from_proto(t)?,
        None => Term::Any,
    };
    let predicate = if proto.predicate.is_empty() {
        None
    } else {
        Some(proto.predicate.clone())
    };
    let predicate_var = if proto.predicate_var.is_empty() {
        None
    } else {
        Some(proto.predicate_var.clone())
    };

    Ok(VarPattern {
        subject,
        predicate,
        predicate_var,
        object,
        edge_var: None,
        max_hops: None,
    })
}

// ── Bindings ──────────────────────────────────────────────────────────────────

pub fn binding_to_proto(bindings: &Bindings) -> proto::Binding {
    proto::Binding {
        vars: bindings
            .iter()
            .map(|(k, &v)| (k.clone(), node_id_to_proto(v)))
            .collect(),
        predicates: std::collections::HashMap::new(),
    }
}

pub fn binding_to_proto_full(
    bindings: &Bindings,
    pred_bindings: &polargraph_query::datalog::PredBindings,
) -> proto::Binding {
    proto::Binding {
        vars: bindings
            .iter()
            .map(|(k, &v)| (k.clone(), node_id_to_proto(v)))
            .collect(),
        predicates: pred_bindings.clone(),
    }
}

pub fn binding_to_query_result(bindings: &Bindings) -> proto::QueryResult {
    proto::QueryResult {
        vars: bindings
            .iter()
            .map(|(k, &v)| (k.clone(), node_id_to_proto(v)))
            .collect(),
    }
}

// ── CypherBinding ─────────────────────────────────────────────────────────────

/// Convert an aggregated result row to a proto [`CypherBinding`].
///
/// `node_keys` — map of variable name → NodeId (group-key variables).
/// `val_keys`  — map of alias → Value (computed aggregates / scalar results).
pub fn cypher_binding_to_proto(
    node_keys: &std::collections::HashMap<String, polargraph_core::id::NodeId>,
    val_keys: &std::collections::HashMap<String, Value>,
) -> proto::CypherBinding {
    proto::CypherBinding {
        nodes: node_keys
            .iter()
            .map(|(k, &id)| (k.clone(), node_id_to_proto(id)))
            .collect(),
        values: val_keys
            .iter()
            .map(|(k, v)| (k.clone(), value_to_proto(v)))
            .collect(),
    }
}

// ── Datalog rules ─────────────────────────────────────────────────────────────

pub fn rule_from_proto(proto: &proto::DatalogRule) -> Result<Rule, Status> {
    if proto.head_predicate.is_empty() {
        return Err(Status::invalid_argument(
            "rule head_predicate must not be empty",
        ));
    }
    if proto.head_subject_var.is_empty() {
        return Err(Status::invalid_argument(
            "rule head_subject_var must not be empty",
        ));
    }
    if proto.head_object_var.is_empty() {
        return Err(Status::invalid_argument(
            "rule head_object_var must not be empty",
        ));
    }
    let body = proto
        .body
        .iter()
        .map(var_pattern_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Rule::new(
        proto.head_predicate.clone(),
        proto.head_subject_var.clone(),
        proto.head_object_var.clone(),
    )
    .with_body(body))
}

// ── Node type schema ──────────────────────────────────────────────────────────

pub fn field_kind_from_str(s: &str) -> Result<FieldKind, Status> {
    match s {
        "bool" => Ok(FieldKind::Bool),
        "int" => Ok(FieldKind::Int),
        "float" => Ok(FieldKind::Float),
        "text" => Ok(FieldKind::Text),
        "blob" => Ok(FieldKind::Blob),
        "vector" => Ok(FieldKind::Vector),
        other => Err(Status::invalid_argument(format!(
            "unknown field kind '{other}'"
        ))),
    }
}

pub fn field_kind_to_str(k: FieldKind) -> &'static str {
    match k {
        FieldKind::Bool => "bool",
        FieldKind::Int => "int",
        FieldKind::Float => "float",
        FieldKind::Text => "text",
        FieldKind::Blob => "blob",
        FieldKind::Vector => "vector",
    }
}

pub fn storage_mode_from_str(s: &str) -> StorageMode {
    match s.to_ascii_lowercase().as_str() {
        "mmap" => StorageMode::Mmap,
        _ => StorageMode::Memory,
    }
}

pub fn storage_mode_to_str(mode: StorageMode) -> &'static str {
    match mode {
        StorageMode::Memory => "memory",
        StorageMode::Mmap => "mmap",
    }
}

pub fn vector_space_def_from_proto(
    proto: &proto::VectorSpaceDef,
) -> Result<VectorSpaceDef, Status> {
    if proto.space_name.is_empty() {
        return Err(Status::invalid_argument(
            "vector_space.space_name must not be empty",
        ));
    }
    if proto.dimensions == 0 {
        return Err(Status::invalid_argument(
            "vector_space.dimensions must be > 0",
        ));
    }
    let mut def = VectorSpaceDef::new(proto.space_name.clone(), proto.dimensions);
    if !proto.embedding_model.is_empty() {
        def = def.with_model(proto.embedding_model.clone());
    }
    if !proto.storage_mode.is_empty() {
        def = def.with_storage_mode(storage_mode_from_str(&proto.storage_mode));
    }
    Ok(def)
}

pub fn vector_space_def_to_proto(def: &VectorSpaceDef) -> proto::VectorSpaceDef {
    proto::VectorSpaceDef {
        space_name: def.space_name.clone(),
        dimensions: def.dimensions,
        embedding_model: def.embedding_model.clone().unwrap_or_default(),
        storage_mode: storage_mode_to_str(def.storage_mode).to_string(),
    }
}

pub fn node_type_def_from_proto(proto: &proto::NodeTypeDef) -> Result<NodeTypeDef, Status> {
    if proto.type_name.is_empty() {
        return Err(Status::invalid_argument("type_name must not be empty"));
    }
    let fields = proto
        .fields
        .iter()
        .map(|f| {
            if f.field_name.is_empty() {
                return Err(Status::invalid_argument("field_name must not be empty"));
            }
            let kind = field_kind_from_str(&f.kind)?;
            Ok(if f.required {
                FieldDef::required(f.field_name.clone(), kind)
            } else {
                FieldDef::optional(f.field_name.clone(), kind)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let vector_space = proto
        .vector_space
        .as_ref()
        .map(vector_space_def_from_proto)
        .transpose()?;

    let mut def = NodeTypeDef::new(proto.type_name.clone(), fields);
    if let Some(vs) = vector_space {
        def = def.with_vector_space(vs);
    }
    if !proto.parent_types.is_empty() {
        def.parent_types = proto.parent_types.clone();
    }
    Ok(def)
}

pub fn node_type_def_to_proto(def: &NodeTypeDef) -> proto::NodeTypeDef {
    proto::NodeTypeDef {
        type_name: def.type_name.clone(),
        fields: def
            .fields
            .iter()
            .map(|f| proto::FieldDef {
                field_name: f.name.clone(),
                kind: field_kind_to_str(f.kind).to_string(),
                required: f.required,
            })
            .collect(),
        vector_space: def.vector_space.as_ref().map(vector_space_def_to_proto),
        parent_types: def.parent_types.clone(),
    }
}

// ── Edge type schema ──────────────────────────────────────────────────────────

pub fn cardinality_from_str(s: &str) -> Result<Cardinality, Status> {
    match s {
        "" | "many" => Ok(Cardinality::Many),
        "one_to_many" => Ok(Cardinality::OneToMany),
        "many_to_one" => Ok(Cardinality::ManyToOne),
        "one_to_one" => Ok(Cardinality::OneToOne),
        other => Err(Status::invalid_argument(format!(
            "unknown cardinality '{other}'; expected one of: many, one_to_many, many_to_one, one_to_one"
        ))),
    }
}

pub fn cardinality_to_str(c: Cardinality) -> &'static str {
    match c {
        Cardinality::Many => "many",
        Cardinality::OneToMany => "one_to_many",
        Cardinality::ManyToOne => "many_to_one",
        Cardinality::OneToOne => "one_to_one",
    }
}

pub fn edge_type_def_from_proto(proto: &proto::EdgeTypeDef) -> Result<EdgeTypeDef, Status> {
    if proto.predicate.is_empty() {
        return Err(Status::invalid_argument("predicate must not be empty"));
    }
    let fields = proto
        .fields
        .iter()
        .map(|f| {
            if f.field_name.is_empty() {
                return Err(Status::invalid_argument("field_name must not be empty"));
            }
            let kind = field_kind_from_str(&f.kind)?;
            Ok(if f.required {
                FieldDef::required(f.field_name.clone(), kind)
            } else {
                FieldDef::optional(f.field_name.clone(), kind)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let cardinality = cardinality_from_str(&proto.cardinality)?;
    let inverse_of = if proto.inverse_of.is_empty() {
        None
    } else {
        Some(proto.inverse_of.clone())
    };

    Ok(EdgeTypeDef {
        predicate: proto.predicate.clone(),
        domain: if proto.domain.is_empty() {
            None
        } else {
            Some(proto.domain.clone())
        },
        range: if proto.range.is_empty() {
            None
        } else {
            Some(proto.range.clone())
        },
        fields,
        cardinality,
        inverse_of,
    })
}

pub fn edge_type_def_to_proto(def: &EdgeTypeDef) -> proto::EdgeTypeDef {
    proto::EdgeTypeDef {
        predicate: def.predicate.clone(),
        domain: def.domain.clone().unwrap_or_default(),
        range: def.range.clone().unwrap_or_default(),
        cardinality: cardinality_to_str(def.cardinality).to_string(),
        inverse_of: def.inverse_of.clone().unwrap_or_default(),
        fields: def
            .fields
            .iter()
            .map(|f| proto::FieldDef {
                field_name: f.name.clone(),
                kind: field_kind_to_str(f.kind).to_string(),
                required: f.required,
            })
            .collect(),
    }
}

// ── RDF-star edge annotations ─────────────────────────────────────────────────

/// Convert a proto `EdgeAnnotation` to a Rust `Triple` (EdgeProperty or EdgeRelation).
///
/// Returns the triple ready to be buffered into a `Transaction`.
pub fn edge_annotation_from_proto(proto: &proto::EdgeAnnotation) -> Result<Triple, Status> {
    let edge_bytes: [u8; 16] = proto
        .edge_id
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("edge_id must be exactly 16 bytes"))?;
    let edge = EdgeId(uuid::Uuid::from_bytes(edge_bytes));

    if proto.predicate.is_empty() {
        return Err(Status::invalid_argument(
            "edge annotation predicate must not be empty",
        ));
    }
    let predicate = Predicate::new(proto.predicate.clone());
    let temporal = BiTemporalRange::assert_now(Timestamp::now());

    match &proto.value {
        Some(AnnotationValueProto::NodeId(bytes)) => {
            let node_bytes: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
                Status::invalid_argument("annotation node_id must be exactly 16 bytes")
            })?;
            let object = NodeId(uuid::Uuid::from_bytes(node_bytes));
            Ok(Triple::EdgeRelation {
                edge,
                predicate,
                object,
                temporal,
            })
        }
        Some(AnnotationValueProto::Scalar(v)) => {
            let value = value_from_proto(v)?;
            Ok(Triple::EdgeProperty {
                edge,
                predicate,
                value,
                temporal,
            })
        }
        None => Err(Status::invalid_argument(
            "edge annotation must have a value (scalar or node_id)",
        )),
    }
}

/// Convert a storage `EdgeAnnotation` to its proto representation.
///
/// `edge_id_bytes` must be the 16-byte UUID of the edge being annotated.
pub fn edge_annotation_to_proto(
    annotation: &EdgeAnnotation,
    edge_id_bytes: Vec<u8>,
) -> proto::EdgeAnnotation {
    let value = match &annotation.value {
        EdgeAnnotationValue::Scalar(v) => AnnotationValueProto::Scalar(value_to_proto(v)),
        EdgeAnnotationValue::Node(node_id) => {
            AnnotationValueProto::NodeId(node_id.as_bytes().to_vec())
        }
    };
    proto::EdgeAnnotation {
        edge_id: edge_id_bytes,
        predicate: annotation.predicate.0.clone(),
        value: Some(value),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::id::NodeId;

    fn sample_node_id() -> NodeId {
        NodeId(Uuid::from_bytes([
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
        ]))
    }

    #[test]
    fn node_id_round_trips() {
        let id = sample_node_id();
        let proto = node_id_to_proto(id);
        let back = node_id_from_proto(&proto).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn node_id_wrong_length_errors() {
        let bad = proto::NodeId {
            bytes: vec![0u8; 15],
        };
        assert!(node_id_from_proto(&bad).is_err());
    }

    #[test]
    fn value_round_trips_all_variants() {
        let cases = vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::Float(std::f64::consts::PI),
            Value::Text("hello".into()),
            Value::Blob(vec![0, 127, 255]),
            Value::Vector(vec![1.0, -2.5, 0.0]),
        ];
        for v in cases {
            let proto = value_to_proto(&v);
            let back = value_from_proto(&proto).unwrap();
            assert_eq!(v, back, "round-trip failed for {v:?}");
        }
    }

    #[test]
    fn term_bound_round_trips() {
        let id = sample_node_id();
        let proto = proto::Term {
            kind: Some(TermKind::Bound(node_id_to_proto(id))),
        };
        match term_from_proto(&proto).unwrap() {
            Term::Bound(back) => assert_eq!(back, id),
            other => panic!("expected Bound, got {other:?}"),
        }
    }

    #[test]
    fn term_var_round_trips() {
        let proto = proto::Term {
            kind: Some(TermKind::Var("manager".into())),
        };
        match term_from_proto(&proto).unwrap() {
            Term::Var(name) => assert_eq!(name, "manager"),
            other => panic!("expected Var, got {other:?}"),
        }
    }

    #[test]
    fn term_absent_is_any() {
        let proto = proto::Term { kind: None };
        assert_eq!(term_from_proto(&proto).unwrap(), Term::Any);
    }

    #[test]
    fn empty_var_name_errors() {
        let proto = proto::Term {
            kind: Some(TermKind::Var(String::new())),
        };
        assert!(term_from_proto(&proto).is_err());
    }

    #[test]
    fn vt_end_zero_becomes_end_of_time() {
        let bt = temporal_from_proto(1000, 0);
        assert_eq!(bt.vt_end, Timestamp::END_OF_TIME);
    }

    #[test]
    fn vt_end_nonzero_preserved() {
        let bt = temporal_from_proto(1000, 5000);
        assert_eq!(bt.vt_end, Timestamp(5000));
    }
}
