//! Dynamic node type registry.
//!
//! Node type schemas are stored as ordinary triples so they participate in
//! bitemporal versioning and persist across restarts with zero extra
//! infrastructure.
//!
//! # Storage layout
//!
//! A single well-known [`SCHEMA_REGISTRY_NODE`] acts as the subject for all
//! schema property triples.  The predicate encodes the type name:
//!
//! ```text
//! (SCHEMA_REGISTRY_NODE, "__schema__/<TypeName>", Value::Text(json_of_fields))
//! ```
//!
//! `json_of_fields` is a `serde_json`-serialised `NodeSchemaJson`.
//!
//! On startup [`NodeTypeRegistry::new`] scans by subject to reconstruct the
//! in-memory cache.  On [`NodeTypeRegistry::register_type`] the schema is
//! committed to the store and the cache is updated atomically.

use crate::{error::StorageError, store::TripleStore};
use polargraph_core::{
    id::NodeId,
    schema::{Cardinality, EdgeTypeDef, FieldDef, NodeTypeDef, VectorSpaceDef},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
    value::Value,
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tracing::info;
use uuid::Uuid;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Well-known node that holds all schema property triples as its properties.
///
/// The bytes spell "polargraph_schem" in ASCII — chosen to be unique and
/// human-recognisable in a hex dump.
pub const SCHEMA_REGISTRY_NODE: NodeId = NodeId(Uuid::from_bytes([
    0x70, 0x6f, 0x6c, 0x61, 0x72, 0x67, 0x72, 0x61, 0x70, 0x68, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d,
]));

const SCHEMA_PREDICATE_PREFIX: &str = "__schema__/";
const EDGE_SCHEMA_PREDICATE_PREFIX: &str = "__edge_schema__/";

fn schema_predicate(type_name: &str) -> String {
    format!("{SCHEMA_PREDICATE_PREFIX}{type_name}")
}

fn type_name_from_predicate(pred: &str) -> Option<&str> {
    pred.strip_prefix(SCHEMA_PREDICATE_PREFIX)
}

fn edge_schema_predicate(predicate_name: &str) -> String {
    format!("{EDGE_SCHEMA_PREDICATE_PREFIX}{predicate_name}")
}

fn predicate_name_from_edge_schema(pred: &str) -> Option<&str> {
    pred.strip_prefix(EDGE_SCHEMA_PREDICATE_PREFIX)
}

// ── Node schema wire format ────────────────────────────────────────────────────

/// On-disk JSON layout for a node type schema.
///
/// Old records only have `fields` (written as a bare `Vec<FieldDef>` array);
/// new records use this struct so `vector_space` can be stored alongside them.
/// `load_from_store` tries this struct first; on failure it falls back to
/// parsing a bare `Vec<FieldDef>` for backward compatibility.
#[derive(serde::Serialize, serde::Deserialize)]
struct NodeSchemaJson {
    fields: Vec<FieldDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vector_space: Option<VectorSpaceDef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    parent_types: Vec<String>,
}

// ── Edge schema wire format ────────────────────────────────────────────────────

/// On-disk JSON layout for an edge type schema.
#[derive(serde::Serialize, serde::Deserialize)]
struct EdgeSchemaJson {
    domain: Option<String>,
    range: Option<String>,
    fields: Vec<FieldDef>,
    #[serde(default)]
    cardinality: Cardinality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inverse_of: Option<String>,
}

// ── Validation error ──────────────────────────────────────────────────────────

/// A single validation failure from [`NodeTypeRegistry::validate_properties`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

impl ValidationError {
    fn missing(field: &str) -> Self {
        Self {
            field: field.into(),
            message: format!("required field '{field}' is missing"),
        }
    }
    fn wrong_kind(field: &str, expected: &str, got: &str) -> Self {
        Self {
            field: field.into(),
            message: format!("field '{field}' expected {expected} but got {got}"),
        }
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

// ── Registry ──────────────────────────────────────────────────────────────────

/// Runtime node type registry.
///
/// `Clone` is cheap (Arc-backed cache, store is already Arc-backed).
#[derive(Clone)]
pub struct NodeTypeRegistry {
    store: TripleStore,
    cache: Arc<RwLock<HashMap<String, NodeTypeDef>>>,
}

impl NodeTypeRegistry {
    /// Open the registry, loading all previously registered schemas from the store.
    pub fn new(store: TripleStore) -> Result<Self, StorageError> {
        let cache = Self::load_from_store(&store)?;
        info!(types = cache.len(), "NodeTypeRegistry loaded");
        Ok(Self {
            store,
            cache: Arc::new(RwLock::new(cache)),
        })
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Register (or overwrite) a node type schema.
    ///
    /// The schema is persisted to the triple store immediately, so it survives
    /// process restarts. Returns `Err` if `def.parent_types` contains a cycle.
    pub fn register_type(&self, def: NodeTypeDef) -> Result<(), StorageError> {
        // Cycle detection: walk the declared parent chain before writing.
        {
            let cache = self.cache.read().unwrap();
            Self::check_no_cycle(&def.type_name, &def.parent_types, &cache)?;
        }

        let wire = NodeSchemaJson {
            fields: def.fields.clone(),
            vector_space: def.vector_space.clone(),
            parent_types: def.parent_types.clone(),
        };
        let json = serde_json::to_string(&wire)?;
        let triple = Triple::Property {
            subject: SCHEMA_REGISTRY_NODE,
            predicate: Predicate::new(schema_predicate(&def.type_name)),
            value: Value::Text(json),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        };
        self.store.insert(&triple)?;
        self.cache
            .write()
            .unwrap()
            .insert(def.type_name.clone(), def);
        Ok(())
    }

    /// Look up a type by name. Returns `None` if the type has never been registered.
    pub fn get_type(&self, name: &str) -> Option<NodeTypeDef> {
        self.cache.read().unwrap().get(name).cloned()
    }

    /// Return all registered type definitions in arbitrary order.
    pub fn list_types(&self) -> Vec<NodeTypeDef> {
        self.cache.read().unwrap().values().cloned().collect()
    }

    /// Find a `VectorSpaceDef` by its space name across all registered node types.
    ///
    /// Returns the first `NodeTypeDef` whose `vector_space.space_name` matches,
    /// or `None` if no such space is registered.
    pub fn get_space_def(&self, space_name: &str) -> Option<VectorSpaceDef> {
        self.cache.read().unwrap().values().find_map(|def| {
            def.vector_space
                .as_ref()
                .filter(|vs| vs.space_name == space_name)
                .cloned()
        })
    }

    /// Return all fields available on `type_name`, including inherited fields.
    ///
    /// Resolution order: own fields first, then depth-first parent fields.
    /// If the same field name appears more than once, the first occurrence wins
    /// (own fields shadow parent fields; earlier parents shadow later ones).
    /// Returns an empty vec if the type is not registered.
    pub fn resolved_fields(&self, type_name: &str) -> Vec<FieldDef> {
        let cache = self.cache.read().unwrap();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut result = Vec::new();
        Self::collect_fields(type_name, &cache, &mut seen, &mut result);
        result
    }

    /// Returns `true` if `type_name` is `ancestor` or inherits from it (transitively).
    pub fn is_subtype_of(&self, type_name: &str, ancestor: &str) -> bool {
        if type_name == ancestor {
            return true;
        }
        let cache = self.cache.read().unwrap();
        let mut visited = std::collections::HashSet::new();
        Self::walk_ancestors(type_name, ancestor, &cache, &mut visited)
    }

    /// Validate a property map against a registered schema (including inherited fields).
    ///
    /// Returns `Ok(())` if all required fields are present and every supplied
    /// value matches the declared kind.  Returns `Err(errors)` otherwise.
    ///
    /// Unknown properties (fields not declared in the schema) are silently
    /// ignored — the store model is open-world.
    ///
    /// Returns a single-element error vec if the type name is not registered.
    pub fn validate_properties(
        &self,
        type_name: &str,
        props: &HashMap<String, Value>,
    ) -> Result<(), Vec<ValidationError>> {
        if self.get_type(type_name).is_none() {
            return Err(vec![ValidationError {
                field: String::new(),
                message: format!("unknown type '{type_name}'"),
            }]);
        }

        let fields = self.resolved_fields(type_name);
        let mut errors = Vec::new();

        for field in &fields {
            match props.get(&field.name) {
                None if field.required => {
                    errors.push(ValidationError::missing(&field.name));
                }
                Some(v) if !field.kind.matches(v) => {
                    errors.push(ValidationError::wrong_kind(
                        &field.name,
                        &field.kind.to_string(),
                        value_kind_name(v),
                    ));
                }
                _ => {}
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn load_from_store(store: &TripleStore) -> Result<HashMap<String, NodeTypeDef>, StorageError> {
        let snap = store.snapshot(store.begin().read_ts);
        let triples = snap.scan_by_subject(&SCHEMA_REGISTRY_NODE)?;

        let mut cache = HashMap::new();
        for triple in triples {
            if let Triple::Property {
                predicate,
                value: Value::Text(json),
                ..
            } = triple
            {
                if let Some(type_name) = type_name_from_predicate(predicate.0.as_str()) {
                    // Try new struct format first; fall back to bare Vec<FieldDef> for old records.
                    let def = if let Ok(wire) = serde_json::from_str::<NodeSchemaJson>(&json) {
                        NodeTypeDef {
                            type_name: type_name.to_string(),
                            fields: wire.fields,
                            vector_space: wire.vector_space,
                            parent_types: wire.parent_types,
                        }
                    } else if let Ok(fields) = serde_json::from_str::<Vec<FieldDef>>(&json) {
                        NodeTypeDef {
                            type_name: type_name.to_string(),
                            fields,
                            vector_space: None,
                            parent_types: vec![],
                        }
                    } else {
                        tracing::warn!(type_name, "skipping malformed schema");
                        continue;
                    };
                    cache.insert(type_name.to_string(), def);
                }
            }
        }
        Ok(cache)
    }

    /// Depth-first collection of all fields reachable from `type_name` via the
    /// parent_types graph. Own fields come first; first occurrence by name wins.
    fn collect_fields(
        type_name: &str,
        cache: &HashMap<String, NodeTypeDef>,
        seen_names: &mut std::collections::HashSet<String>,
        out: &mut Vec<FieldDef>,
    ) {
        let Some(def) = cache.get(type_name) else {
            return;
        };
        for field in &def.fields {
            if seen_names.insert(field.name.clone()) {
                out.push(field.clone());
            }
        }
        for parent in &def.parent_types {
            Self::collect_fields(parent, cache, seen_names, out);
        }
    }

    /// Transitive ancestor walk. Returns `true` if `ancestor` is reachable.
    fn walk_ancestors(
        current: &str,
        ancestor: &str,
        cache: &HashMap<String, NodeTypeDef>,
        visited: &mut std::collections::HashSet<String>,
    ) -> bool {
        let Some(def) = cache.get(current) else {
            return false;
        };
        for parent in &def.parent_types {
            if parent == ancestor {
                return true;
            }
            if visited.insert(parent.clone())
                && Self::walk_ancestors(parent, ancestor, cache, visited)
            {
                return true;
            }
        }
        false
    }

    /// Walk the parent chain for `type_name` and return an error if a cycle is detected.
    fn check_no_cycle(
        type_name: &str,
        parents: &[String],
        cache: &HashMap<String, NodeTypeDef>,
    ) -> Result<(), StorageError> {
        let mut visited = std::collections::HashSet::new();
        visited.insert(type_name.to_string());
        for parent in parents {
            if !Self::dfs_cycle_check(parent, &visited, cache) {
                return Err(StorageError::Validation(format!(
                    "cycle detected in type hierarchy: '{type_name}' → '{parent}'"
                )));
            }
        }
        Ok(())
    }

    /// Returns `false` if a cycle is found (i.e., `current` is already in `ancestors`).
    fn dfs_cycle_check(
        current: &str,
        ancestors: &std::collections::HashSet<String>,
        cache: &HashMap<String, NodeTypeDef>,
    ) -> bool {
        if ancestors.contains(current) {
            return false; // cycle
        }
        let mut next_ancestors = ancestors.clone();
        next_ancestors.insert(current.to_string());
        if let Some(def) = cache.get(current) {
            for parent in &def.parent_types {
                if !Self::dfs_cycle_check(parent, &next_ancestors, cache) {
                    return false;
                }
            }
        }
        true
    }
}

// ── EdgeTypeRegistry ──────────────────────────────────────────────────────────

/// Runtime edge type registry.
///
/// Schemas are stored as triples under the same [`SCHEMA_REGISTRY_NODE`] as
/// node schemas but with the predicate prefix `__edge_schema__/<predicate>`.
/// The JSON value encodes domain, range, and field list.
///
/// `Clone` is cheap (Arc-backed cache).
#[derive(Clone)]
pub struct EdgeTypeRegistry {
    store: TripleStore,
    cache: Arc<RwLock<HashMap<String, EdgeTypeDef>>>,
}

impl EdgeTypeRegistry {
    /// Open the registry, loading all previously registered schemas from the store.
    pub fn new(store: TripleStore) -> Result<Self, StorageError> {
        let cache = Self::load_from_store(&store)?;
        info!(edge_types = cache.len(), "EdgeTypeRegistry loaded");
        Ok(Self {
            store,
            cache: Arc::new(RwLock::new(cache)),
        })
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Register (or overwrite) an edge type schema.
    pub fn register_edge_type(&self, def: EdgeTypeDef) -> Result<(), StorageError> {
        let wire = EdgeSchemaJson {
            domain: def.domain.clone(),
            range: def.range.clone(),
            fields: def.fields.clone(),
            cardinality: def.cardinality,
            inverse_of: def.inverse_of.clone(),
        };
        let json = serde_json::to_string(&wire)?;
        let triple = Triple::Property {
            subject: SCHEMA_REGISTRY_NODE,
            predicate: Predicate::new(edge_schema_predicate(&def.predicate)),
            value: Value::Text(json),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        };
        self.store.insert(&triple)?;
        self.cache
            .write()
            .unwrap()
            .insert(def.predicate.clone(), def);
        Ok(())
    }

    /// Look up a schema by predicate name.
    pub fn get_edge_type(&self, predicate: &str) -> Option<EdgeTypeDef> {
        self.cache.read().unwrap().get(predicate).cloned()
    }

    /// Return all registered edge type schemas in arbitrary order.
    pub fn list_edge_types(&self) -> Vec<EdgeTypeDef> {
        self.cache.read().unwrap().values().cloned().collect()
    }

    /// Return all predicate names whose domain and range both match the
    /// supplied node type names.  `None` in the stored schema means
    /// "unconstrained" — an unconstrained domain/range matches any type.
    pub fn list_predicates_between(&self, domain_type: &str, range_type: &str) -> Vec<String> {
        self.cache
            .read()
            .unwrap()
            .values()
            .filter(|def| {
                def.domain.as_deref().map_or(true, |d| d == domain_type)
                    && def.range.as_deref().map_or(true, |r| r == range_type)
            })
            .map(|def| def.predicate.clone())
            .collect()
    }

    /// Validate an edge against its registered schema.
    ///
    /// `subject_type` and `object_type` are the node types of the edge's
    /// endpoints (the caller is responsible for resolving these).  Pass `None`
    /// if the endpoint's type is unknown — an unconstrained domain/range slot
    /// always passes, but a constrained slot requires the type to match.
    ///
    /// Returns `Ok(())` if the edge is valid or if no schema is registered for
    /// the predicate (open-world assumption).  Returns `Err(errors)` on failure.
    pub fn validate_edge(
        &self,
        predicate: &str,
        subject_type: Option<&str>,
        object_type: Option<&str>,
        props: &HashMap<String, Value>,
    ) -> Result<(), Vec<ValidationError>> {
        let def = match self.get_edge_type(predicate) {
            Some(d) => d,
            None => return Ok(()), // no schema → always valid
        };

        let mut errors = Vec::new();

        // Domain check.
        if let Some(expected_domain) = &def.domain {
            match subject_type {
                None => errors.push(ValidationError {
                    field: "subject_type".into(),
                    message: format!(
                        "predicate '{predicate}' requires domain '{expected_domain}' but subject type is unknown"
                    ),
                }),
                Some(t) if t != expected_domain => errors.push(ValidationError {
                    field: "subject_type".into(),
                    message: format!(
                        "predicate '{predicate}' requires domain '{expected_domain}' but got '{t}'"
                    ),
                }),
                _ => {}
            }
        }

        // Range check.
        if let Some(expected_range) = &def.range {
            match object_type {
                None => errors.push(ValidationError {
                    field: "object_type".into(),
                    message: format!(
                        "predicate '{predicate}' requires range '{expected_range}' but object type is unknown"
                    ),
                }),
                Some(t) if t != expected_range => errors.push(ValidationError {
                    field: "object_type".into(),
                    message: format!(
                        "predicate '{predicate}' requires range '{expected_range}' but got '{t}'"
                    ),
                }),
                _ => {}
            }
        }

        // Field checks (required presence + kind).
        for field in &def.fields {
            match props.get(&field.name) {
                None if field.required => {
                    errors.push(ValidationError::missing(&field.name));
                }
                Some(v) if !field.kind.matches(v) => {
                    errors.push(ValidationError::wrong_kind(
                        &field.name,
                        &field.kind.to_string(),
                        value_kind_name(v),
                    ));
                }
                _ => {}
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Check whether inserting `(subject, predicate, object)` would violate the
    /// registered cardinality constraint for `predicate`.
    ///
    /// - `OneToMany` / `OneToOne`: rejects if `subject` already has any object
    ///   for this predicate in the committed store.
    /// - `ManyToOne` / `OneToOne`: rejects if `object` already has any subject
    ///   for this predicate in the committed store.
    /// - `Many`: always returns `Ok(())`.
    pub fn validate_cardinality(
        &self,
        predicate: &str,
        subject: NodeId,
        object: NodeId,
        store: &TripleStore,
    ) -> Result<(), ValidationError> {
        let def = match self.get_edge_type(predicate) {
            Some(d) => d,
            None => return Ok(()),
        };

        match def.cardinality {
            Cardinality::Many => {}

            Cardinality::OneToMany | Cardinality::OneToOne => {
                let existing = store
                    .scan_by_subject_predicate(&subject, predicate)
                    .unwrap_or_default();
                let has_relation = existing
                    .iter()
                    .any(|t| matches!(t, Triple::Relation { .. }));
                if has_relation {
                    return Err(ValidationError {
                        field: "subject".into(),
                        message: format!(
                            "cardinality violation: predicate '{predicate}' allows at most one object per subject"
                        ),
                    });
                }
            }

            Cardinality::ManyToOne => {
                let existing = store
                    .scan_by_predicate_object(predicate, &object)
                    .unwrap_or_default();
                let has_relation = existing
                    .iter()
                    .any(|t| matches!(t, Triple::Relation { .. }));
                if has_relation {
                    return Err(ValidationError {
                        field: "object".into(),
                        message: format!(
                            "cardinality violation: predicate '{predicate}' allows at most one subject per object"
                        ),
                    });
                }
            }
        }

        // OneToOne: already checked subject side above; now check object side.
        if def.cardinality == Cardinality::OneToOne {
            let existing = store
                .scan_by_predicate_object(predicate, &object)
                .unwrap_or_default();
            let has_relation = existing
                .iter()
                .any(|t| matches!(t, Triple::Relation { .. }));
            if has_relation {
                return Err(ValidationError {
                    field: "object".into(),
                    message: format!(
                        "cardinality violation: predicate '{predicate}' allows at most one subject per object"
                    ),
                });
            }
        }

        Ok(())
    }

    /// Return the declared inverse predicate name for `predicate`, if any.
    pub fn inverse_predicate(&self, predicate: &str) -> Option<String> {
        self.cache
            .read()
            .unwrap()
            .get(predicate)?
            .inverse_of
            .clone()
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn load_from_store(store: &TripleStore) -> Result<HashMap<String, EdgeTypeDef>, StorageError> {
        let snap = store.snapshot(store.begin().read_ts);
        let triples = snap.scan_by_subject(&SCHEMA_REGISTRY_NODE)?;

        let mut cache = HashMap::new();
        for triple in triples {
            if let Triple::Property {
                predicate,
                value: Value::Text(json),
                ..
            } = triple
            {
                if let Some(pred_name) = predicate_name_from_edge_schema(predicate.0.as_str()) {
                    match serde_json::from_str::<EdgeSchemaJson>(&json) {
                        Ok(wire) => {
                            cache.insert(
                                pred_name.to_string(),
                                EdgeTypeDef {
                                    predicate: pred_name.to_string(),
                                    domain: wire.domain,
                                    range: wire.range,
                                    fields: wire.fields,
                                    cardinality: wire.cardinality,
                                    inverse_of: wire.inverse_of,
                                },
                            );
                        }
                        Err(e) => {
                            tracing::warn!(predicate = pred_name, error = %e, "skipping malformed edge schema");
                        }
                    }
                }
            }
        }
        Ok(cache)
    }
}

fn value_kind_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Text(_) => "text",
        Value::Blob(_) => "blob",
        Value::Vector(_) => "vector",
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::schema::FieldKind;
    use tempfile::TempDir;

    fn open() -> (NodeTypeRegistry, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let registry = NodeTypeRegistry::new(store).unwrap();
        (registry, dir)
    }

    fn person_def() -> NodeTypeDef {
        NodeTypeDef::new(
            "Person",
            vec![
                FieldDef::required("name", FieldKind::Text),
                FieldDef::optional("age", FieldKind::Int),
                FieldDef::optional("embedding", FieldKind::Vector),
            ],
        )
    }

    // ── register / get / list ─────────────────────────────────────────────────

    #[test]
    fn register_and_get_type() {
        let (reg, _dir) = open();
        let def = person_def();
        reg.register_type(def.clone()).unwrap();
        assert_eq!(reg.get_type("Person"), Some(def));
    }

    #[test]
    fn get_unknown_type_returns_none() {
        let (reg, _dir) = open();
        assert_eq!(reg.get_type("Ghost"), None);
    }

    #[test]
    fn list_types_returns_all_registered() {
        let (reg, _dir) = open();
        reg.register_type(person_def()).unwrap();
        reg.register_type(NodeTypeDef::new(
            "Project",
            vec![FieldDef::required("title", FieldKind::Text)],
        ))
        .unwrap();

        let names: Vec<String> = {
            let mut v: Vec<_> = reg
                .list_types()
                .iter()
                .map(|d| d.type_name.clone())
                .collect();
            v.sort();
            v
        };
        assert_eq!(names, vec!["Person", "Project"]);
    }

    #[test]
    fn register_overwrites_previous_definition() {
        let (reg, _dir) = open();
        reg.register_type(NodeTypeDef::new(
            "Thing",
            vec![FieldDef::required("x", FieldKind::Int)],
        ))
        .unwrap();
        let updated = NodeTypeDef::new(
            "Thing",
            vec![
                FieldDef::required("x", FieldKind::Int),
                FieldDef::optional("y", FieldKind::Float),
            ],
        );
        reg.register_type(updated.clone()).unwrap();
        assert_eq!(reg.get_type("Thing"), Some(updated));
    }

    // ── persistence ───────────────────────────────────────────────────────────

    #[test]
    fn schemas_persist_across_reopen() {
        let dir = TempDir::new().unwrap();
        let def = person_def();

        {
            let store = TripleStore::open(dir.path()).unwrap();
            let reg = NodeTypeRegistry::new(store).unwrap();
            reg.register_type(def.clone()).unwrap();
        }

        let store2 = TripleStore::open(dir.path()).unwrap();
        let reg2 = NodeTypeRegistry::new(store2).unwrap();
        assert_eq!(reg2.get_type("Person"), Some(def));
    }

    // ── validation ────────────────────────────────────────────────────────────

    #[test]
    fn validate_valid_node() {
        let (reg, _dir) = open();
        reg.register_type(person_def()).unwrap();

        let props = HashMap::from([
            ("name".to_string(), Value::Text("Alice".into())),
            ("age".to_string(), Value::Int(30)),
        ]);
        assert!(reg.validate_properties("Person", &props).is_ok());
    }

    #[test]
    fn validate_missing_required_field() {
        let (reg, _dir) = open();
        reg.register_type(person_def()).unwrap();

        let props = HashMap::from([
            ("age".to_string(), Value::Int(30)),
            // "name" is missing
        ]);
        let errs = reg.validate_properties("Person", &props).unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("name"));
        assert!(errs[0].message.contains("missing"));
    }

    #[test]
    fn validate_wrong_value_kind() {
        let (reg, _dir) = open();
        reg.register_type(person_def()).unwrap();

        let props = HashMap::from([
            ("name".to_string(), Value::Int(42)), // should be Text
        ]);
        let errs = reg.validate_properties("Person", &props).unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("name"));
        assert!(errs[0].message.contains("text"));
    }

    #[test]
    fn validate_unknown_type_returns_error() {
        let (reg, _dir) = open();
        let errs = reg
            .validate_properties("Ghost", &HashMap::new())
            .unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("Ghost"));
    }

    #[test]
    fn validate_unknown_fields_are_ignored() {
        let (reg, _dir) = open();
        reg.register_type(person_def()).unwrap();

        let props = HashMap::from([
            ("name".to_string(), Value::Text("Bob".into())),
            ("extra_field".to_string(), Value::Bool(true)), // not in schema — OK
        ]);
        assert!(reg.validate_properties("Person", &props).is_ok());
    }

    #[test]
    fn validate_optional_field_absent_is_ok() {
        let (reg, _dir) = open();
        reg.register_type(person_def()).unwrap();

        // Only required "name" supplied; optional "age" and "embedding" absent.
        let props = HashMap::from([("name".to_string(), Value::Text("Carol".into()))]);
        assert!(reg.validate_properties("Person", &props).is_ok());
    }

    #[test]
    fn validate_multiple_errors_collected() {
        let (reg, _dir) = open();
        reg.register_type(NodeTypeDef::new(
            "Item",
            vec![
                FieldDef::required("title", FieldKind::Text),
                FieldDef::required("count", FieldKind::Int),
            ],
        ))
        .unwrap();

        // Both required fields missing.
        let errs = reg
            .validate_properties("Item", &HashMap::new())
            .unwrap_err();
        assert_eq!(errs.len(), 2);
    }

    // ── EdgeTypeRegistry tests ────────────────────────────────────────────────

    fn open_edge() -> (EdgeTypeRegistry, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        let reg = EdgeTypeRegistry::new(store).unwrap();
        (reg, dir)
    }

    fn works_at_def() -> EdgeTypeDef {
        EdgeTypeDef::new(
            "works_at",
            Some("Person"),
            Some("Company"),
            vec![FieldDef::required("since", FieldKind::Int)],
        )
    }

    #[test]
    fn edge_register_and_get() {
        let (reg, _dir) = open_edge();
        let def = works_at_def();
        reg.register_edge_type(def.clone()).unwrap();
        assert_eq!(reg.get_edge_type("works_at"), Some(def));
    }

    #[test]
    fn edge_get_unknown_returns_none() {
        let (reg, _dir) = open_edge();
        assert_eq!(reg.get_edge_type("phantom"), None);
    }

    #[test]
    fn edge_list_returns_all_registered() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap();
        reg.register_edge_type(EdgeTypeDef::new(
            "knows",
            None::<String>,
            None::<String>,
            vec![],
        ))
        .unwrap();

        let mut names: Vec<_> = reg
            .list_edge_types()
            .iter()
            .map(|d| d.predicate.clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["knows", "works_at"]);
    }

    #[test]
    fn edge_register_overwrites() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap();
        let updated = EdgeTypeDef::new("works_at", Some("Employee"), Some("Company"), vec![]);
        reg.register_edge_type(updated.clone()).unwrap();
        assert_eq!(reg.get_edge_type("works_at"), Some(updated));
    }

    #[test]
    fn edge_schemas_persist_across_reopen() {
        let dir = TempDir::new().unwrap();
        let def = works_at_def();

        {
            let store = TripleStore::open(dir.path()).unwrap();
            let reg = EdgeTypeRegistry::new(store).unwrap();
            reg.register_edge_type(def.clone()).unwrap();
        }

        let store2 = TripleStore::open(dir.path()).unwrap();
        let reg2 = EdgeTypeRegistry::new(store2).unwrap();
        assert_eq!(reg2.get_edge_type("works_at"), Some(def));
    }

    #[test]
    fn edge_validate_valid() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap();

        let props = HashMap::from([("since".to_string(), Value::Int(2020))]);
        assert!(reg
            .validate_edge("works_at", Some("Person"), Some("Company"), &props)
            .is_ok());
    }

    #[test]
    fn edge_validate_domain_mismatch() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap();

        let props = HashMap::from([("since".to_string(), Value::Int(2020))]);
        let errs = reg
            .validate_edge("works_at", Some("Robot"), Some("Company"), &props)
            .unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("Person"));
        assert!(errs[0].message.contains("Robot"));
    }

    #[test]
    fn edge_validate_range_mismatch() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap();

        let props = HashMap::from([("since".to_string(), Value::Int(2020))]);
        let errs = reg
            .validate_edge("works_at", Some("Person"), Some("Project"), &props)
            .unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("Company"));
        assert!(errs[0].message.contains("Project"));
    }

    #[test]
    fn edge_validate_missing_required_prop() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap();

        // "since" is required but absent.
        let errs = reg
            .validate_edge("works_at", Some("Person"), Some("Company"), &HashMap::new())
            .unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("since"));
    }

    #[test]
    fn edge_validate_wrong_prop_type() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap();

        let props = HashMap::from([("since".to_string(), Value::Text("2020".into()))]);
        let errs = reg
            .validate_edge("works_at", Some("Person"), Some("Company"), &props)
            .unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("since"));
        assert!(errs[0].message.contains("int"));
    }

    #[test]
    fn edge_validate_unknown_predicate_is_valid() {
        let (reg, _dir) = open_edge();
        // No schema registered → open-world, always valid.
        assert!(reg
            .validate_edge("anything", Some("A"), Some("B"), &HashMap::new())
            .is_ok());
    }

    #[test]
    fn edge_validate_unknown_subject_type_on_constrained_domain_errors() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap();

        let props = HashMap::from([("since".to_string(), Value::Int(2020))]);
        let errs = reg
            .validate_edge("works_at", None, Some("Company"), &props)
            .unwrap_err();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("unknown"));
    }

    #[test]
    fn list_predicates_between_filters_correctly() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(works_at_def()).unwrap(); // Person → Company
        reg.register_edge_type(EdgeTypeDef::new(
            "manages",
            Some("Person"),
            Some("Person"),
            vec![],
        ))
        .unwrap();
        // No unconstrained predicates — test focuses on constrained filtering.

        // Exact match Person → Company should return only "works_at".
        let preds = reg.list_predicates_between("Person", "Company");
        assert_eq!(preds, vec!["works_at"]);

        // Person → Person should return only "manages".
        let preds = reg.list_predicates_between("Person", "Person");
        assert_eq!(preds, vec!["manages"]);

        // Querying a pair that matches neither constrained predicate returns empty.
        let preds = reg.list_predicates_between("Robot", "Company");
        assert!(preds.is_empty());
    }

    #[test]
    fn list_predicates_between_includes_unconstrained() {
        let (reg, _dir) = open_edge();
        reg.register_edge_type(EdgeTypeDef::new(
            "knows",
            None::<String>,
            None::<String>,
            vec![],
        ))
        .unwrap();

        // Unconstrained predicate matches any pair.
        let preds = reg.list_predicates_between("Foo", "Bar");
        assert_eq!(preds, vec!["knows"]);
    }
}
