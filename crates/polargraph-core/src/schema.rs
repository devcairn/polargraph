//! Node type schema definitions.
//!
//! A `NodeTypeDef` names a type and declares which property predicates it
//! expects, their value kind, and whether each field is required.  Schemas
//! are advisory: the store accepts any triple regardless of whether the
//! subject has a registered type.  Validation is a separate, explicit step.

use crate::value::Value;
use serde::{Deserialize, Serialize};

/// The expected value kind for a field.
///
/// Maps 1-to-1 with the non-`Null` variants of [`Value`]; `Null` is not a
/// valid field kind because a field that can be null provides no type information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldKind {
    Bool,
    Int,
    Float,
    Text,
    Blob,
    Vector,
}

impl FieldKind {
    /// Returns `true` if `value` is compatible with this kind.
    pub fn matches(self, value: &Value) -> bool {
        matches!(
            (self, value),
            (FieldKind::Bool,   Value::Bool(_))
            | (FieldKind::Int,    Value::Int(_))
            | (FieldKind::Float,  Value::Float(_))
            | (FieldKind::Text,   Value::Text(_))
            | (FieldKind::Blob,   Value::Blob(_))
            | (FieldKind::Vector, Value::Vector(_))
        )
    }
}

impl std::fmt::Display for FieldKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FieldKind::Bool   => "bool",
            FieldKind::Int    => "int",
            FieldKind::Float  => "float",
            FieldKind::Text   => "text",
            FieldKind::Blob   => "blob",
            FieldKind::Vector => "vector",
        };
        write!(f, "{s}")
    }
}

/// A single field in a node type schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub kind: FieldKind,
    /// If `true`, `validate_properties` treats the absence of this field as
    /// an error.
    pub required: bool,
}

impl FieldDef {
    pub fn required(name: impl Into<String>, kind: FieldKind) -> Self {
        Self { name: name.into(), kind, required: true }
    }

    pub fn optional(name: impl Into<String>, kind: FieldKind) -> Self {
        Self { name: name.into(), kind, required: false }
    }
}

/// Whether a named HNSW vector space keeps vectors in heap RAM or pages them
/// from a memory-mapped file on demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum StorageMode {
    /// All vectors are deserialized from RocksDB into heap RAM at startup and
    /// held there for the lifetime of the store. Lower cold-query latency;
    /// higher steady-state RAM usage. Default.
    #[default]
    Memory,
    /// Vectors are written to a flat binary file (`<data_dir>/vectors/<space>.vecs`)
    /// and accessed via `mmap`. The OS pages vectors in on demand and evicts them
    /// under memory pressure. Cold-query latency increases by ~100 µs per page fault;
    /// steady-state RAM drops from ~7 GB to ~1–2 GB for a 1 M-node, 1 536-dim index.
    Mmap,
}

/// Metadata for a node type's vector embedding space.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorSpaceDef {
    /// Name of the HNSW space this type's vectors live in (e.g. `"default"` or the type name).
    pub space_name: String,
    /// Expected dimensionality — inserts with a different number of floats are rejected.
    pub dimensions: u32,
    /// Optional identifier of the embedding model (informational only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_model: Option<String>,
    /// Storage mode for this space's vector data. Defaults to `Memory`.
    #[serde(default)]
    pub storage_mode: StorageMode,
}

impl VectorSpaceDef {
    pub fn new(space_name: impl Into<String>, dimensions: u32) -> Self {
        Self {
            space_name: space_name.into(),
            dimensions,
            embedding_model: None,
            storage_mode: StorageMode::Memory,
        }
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.embedding_model = Some(model.into());
        self
    }

    pub fn with_storage_mode(mut self, mode: StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }
}

/// Cardinality constraint on a relation predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    /// No constraint. Default.
    #[default]
    Many,
    /// At most one object per subject (functional).
    OneToMany,
    /// At most one subject per object (inverse-functional).
    ManyToOne,
    /// At most one object per subject AND at most one subject per object.
    OneToOne,
}

/// A named node type schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeTypeDef {
    pub type_name: String,
    pub fields: Vec<FieldDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_space: Option<VectorSpaceDef>,
    /// Parent type names. This type inherits all fields declared by each parent.
    /// Cycles are rejected at registration time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_types: Vec<String>,
}

impl NodeTypeDef {
    pub fn new(type_name: impl Into<String>, fields: Vec<FieldDef>) -> Self {
        Self { type_name: type_name.into(), fields, vector_space: None, parent_types: vec![] }
    }

    pub fn with_vector_space(mut self, space: VectorSpaceDef) -> Self {
        self.vector_space = Some(space);
        self
    }

    pub fn with_parent_types(mut self, parents: Vec<impl Into<String>>) -> Self {
        self.parent_types = parents.into_iter().map(Into::into).collect();
        self
    }
}

/// A named edge type schema.
///
/// `predicate` is the relation name (e.g. `"works_at"`).  `domain` and
/// `range` are optional node type names that constrain which node types may
/// appear as subject and object respectively.  `fields` declares expected
/// property predicates on the edge itself.
///
/// Edge properties are stored as `Triple::Property` triples whose subject is
/// `NodeId(edge_id.0)` — the same UUID as the relation's `EdgeId`, reinterpreted
/// as a node subject. Clients receive the edge UUID in `InsertResponse.edge_ids`
/// and can query edge properties by that UUID.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeTypeDef {
    /// The predicate string this schema governs.
    pub predicate: String,
    /// Node type name for the allowed subject. `None` means unconstrained.
    pub domain: Option<String>,
    /// Node type name for the allowed object. `None` means unconstrained.
    pub range: Option<String>,
    /// Expected property predicates on this edge.
    pub fields: Vec<FieldDef>,
    /// Cardinality constraint on the outgoing direction (subject → object).
    #[serde(default)]
    pub cardinality: Cardinality,
    /// If `Some(pred)`, every triple `(A, predicate, B)` must have a
    /// corresponding `(B, pred, A)`. Validated on insert, not enforced in storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inverse_of: Option<String>,
}

impl EdgeTypeDef {
    pub fn new(
        predicate: impl Into<String>,
        domain: Option<impl Into<String>>,
        range: Option<impl Into<String>>,
        fields: Vec<FieldDef>,
    ) -> Self {
        Self {
            predicate: predicate.into(),
            domain: domain.map(Into::into),
            range: range.map(Into::into),
            fields,
            cardinality: Cardinality::Many,
            inverse_of: None,
        }
    }

    pub fn with_cardinality(mut self, cardinality: Cardinality) -> Self {
        self.cardinality = cardinality;
        self
    }

    pub fn with_inverse_of(mut self, inverse: impl Into<String>) -> Self {
        self.inverse_of = Some(inverse.into());
        self
    }
}

/// Policy controlling bitemporal data retention.
///
/// Any triple matching either condition is eligible for deletion by
/// [`CompactionManager::run_retention`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Delete triples whose transaction time (`tt`) is older than this many
    /// seconds relative to the time `run_retention` is called.
    pub tx_age_secs: u64,
    /// If `Some(n)`, also delete triples whose `vt_end` is more than `n`
    /// seconds in the past. `None` keeps all valid-time history.
    pub vt_lookback_secs: Option<u64>,
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_kind_matches_correct_value() {
        assert!(FieldKind::Bool.matches(&Value::Bool(true)));
        assert!(FieldKind::Int.matches(&Value::Int(42)));
        assert!(FieldKind::Float.matches(&Value::Float(3.14)));
        assert!(FieldKind::Text.matches(&Value::Text("hi".into())));
        assert!(FieldKind::Blob.matches(&Value::Blob(vec![1, 2])));
        assert!(FieldKind::Vector.matches(&Value::Vector(vec![0.1])));
    }

    #[test]
    fn field_kind_rejects_wrong_value() {
        assert!(!FieldKind::Bool.matches(&Value::Int(1)));
        assert!(!FieldKind::Int.matches(&Value::Float(1.0)));
        assert!(!FieldKind::Text.matches(&Value::Blob(vec![])));
        assert!(!FieldKind::Vector.matches(&Value::Text("x".into())));
    }

    #[test]
    fn node_type_def_serialises_round_trip() {
        let def = NodeTypeDef::new("Person", vec![
            FieldDef::required("name", FieldKind::Text),
            FieldDef::optional("age", FieldKind::Int),
            FieldDef::optional("embedding", FieldKind::Vector),
        ]);
        let json = serde_json::to_string(&def).unwrap();
        let back: NodeTypeDef = serde_json::from_str(&json).unwrap();
        assert_eq!(def, back);
    }
}
