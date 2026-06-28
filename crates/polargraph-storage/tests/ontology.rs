//! Integration tests for the schema and ontology layer:
//! cardinality constraints, inverse-predicate declarations, and subtype inheritance.

use polargraph_core::{
    id::NodeId,
    schema::{Cardinality, EdgeTypeDef, FieldDef, FieldKind, NodeTypeDef},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
};
use polargraph_storage::{EdgeTypeRegistry, NodeTypeRegistry, TripleStore};
use std::collections::HashMap;
use tempfile::TempDir;

fn open_store() -> (TripleStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let store = TripleStore::open(dir.path()).unwrap();
    (store, dir)
}

fn relation(subject: NodeId, predicate: &str, object: NodeId) -> Triple {
    Triple::Relation {
        subject,
        predicate: Predicate::new(predicate),
        object,
        edge_id: polargraph_core::id::EdgeId::new(),
        temporal: BiTemporalRange::assert_now(Timestamp::now()),
    }
}

// ── Cardinality ───────────────────────────────────────────────────────────────

#[test]
fn test_cardinality_one_to_many() {
    let (store, _dir) = open_store();
    let reg = EdgeTypeRegistry::new(store.clone()).unwrap();

    reg.register_edge_type(
        EdgeTypeDef::new("manages", None::<String>, None::<String>, vec![])
            .with_cardinality(Cardinality::OneToMany),
    )
    .unwrap();

    let subject = NodeId::new();
    let object1 = NodeId::new();
    let object2 = NodeId::new();

    // First insert: no existing triple → ok.
    assert!(reg
        .validate_cardinality("manages", subject, object1, &store)
        .is_ok());

    // Commit the first triple.
    store
        .insert(&relation(subject, "manages", object1))
        .unwrap();

    // Second insert with same subject → cardinality violation.
    let result = reg.validate_cardinality("manages", subject, object2, &store);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .message
        .contains("one object per subject"));

    // Different subject → still ok.
    let other_subject = NodeId::new();
    assert!(reg
        .validate_cardinality("manages", other_subject, object1, &store)
        .is_ok());
}

#[test]
fn test_cardinality_many_to_one() {
    let (store, _dir) = open_store();
    let reg = EdgeTypeRegistry::new(store.clone()).unwrap();

    reg.register_edge_type(
        EdgeTypeDef::new("owned_by", None::<String>, None::<String>, vec![])
            .with_cardinality(Cardinality::ManyToOne),
    )
    .unwrap();

    let subject1 = NodeId::new();
    let subject2 = NodeId::new();
    let object = NodeId::new();

    // First insert: ok.
    assert!(reg
        .validate_cardinality("owned_by", subject1, object, &store)
        .is_ok());

    store
        .insert(&relation(subject1, "owned_by", object))
        .unwrap();

    // Second insert with same object, different subject → violation.
    let result = reg.validate_cardinality("owned_by", subject2, object, &store);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .message
        .contains("one subject per object"));

    // Different object → ok.
    let other_object = NodeId::new();
    assert!(reg
        .validate_cardinality("owned_by", subject2, other_object, &store)
        .is_ok());
}

#[test]
fn test_cardinality_one_to_one() {
    let (store, _dir) = open_store();
    let reg = EdgeTypeRegistry::new(store.clone()).unwrap();

    reg.register_edge_type(
        EdgeTypeDef::new("paired_with", None::<String>, None::<String>, vec![])
            .with_cardinality(Cardinality::OneToOne),
    )
    .unwrap();

    let a = NodeId::new();
    let b = NodeId::new();
    let c = NodeId::new();

    // First insert ok.
    assert!(reg
        .validate_cardinality("paired_with", a, b, &store)
        .is_ok());
    store.insert(&relation(a, "paired_with", b)).unwrap();

    // Same subject, different object → subject already has one.
    assert!(reg
        .validate_cardinality("paired_with", a, c, &store)
        .is_err());
    // Different subject, same object → object already has one.
    assert!(reg
        .validate_cardinality("paired_with", c, b, &store)
        .is_err());
}

#[test]
fn test_cardinality_many_always_ok() {
    let (store, _dir) = open_store();
    let reg = EdgeTypeRegistry::new(store.clone()).unwrap();

    reg.register_edge_type(EdgeTypeDef::new(
        "knows",
        None::<String>,
        None::<String>,
        vec![],
    ))
    .unwrap();

    let a = NodeId::new();
    let b = NodeId::new();
    let c = NodeId::new();

    store.insert(&relation(a, "knows", b)).unwrap();
    store.insert(&relation(a, "knows", c)).unwrap();

    // Many cardinality: no violation even with multiple objects.
    assert!(reg.validate_cardinality("knows", a, b, &store).is_ok());
}

// ── Inverse predicate ─────────────────────────────────────────────────────────

#[test]
fn test_inverse_of_lookup() {
    let (store, _dir) = open_store();
    let reg = EdgeTypeRegistry::new(store.clone()).unwrap();

    reg.register_edge_type(
        EdgeTypeDef::new("parent_of", None::<String>, None::<String>, vec![])
            .with_inverse_of("child_of"),
    )
    .unwrap();
    reg.register_edge_type(EdgeTypeDef::new(
        "sibling_of",
        None::<String>,
        None::<String>,
        vec![],
    ))
    .unwrap();

    assert_eq!(
        reg.inverse_predicate("parent_of"),
        Some("child_of".to_string())
    );
    assert_eq!(reg.inverse_predicate("sibling_of"), None);
    assert_eq!(reg.inverse_predicate("unknown"), None);
}

#[test]
fn test_inverse_of_persists_across_reopen() {
    let dir = TempDir::new().unwrap();

    {
        let store = TripleStore::open(dir.path()).unwrap();
        let reg = EdgeTypeRegistry::new(store).unwrap();
        reg.register_edge_type(
            EdgeTypeDef::new("follows", None::<String>, None::<String>, vec![])
                .with_inverse_of("followed_by"),
        )
        .unwrap();
    }

    let store2 = TripleStore::open(dir.path()).unwrap();
    let reg2 = EdgeTypeRegistry::new(store2).unwrap();
    assert_eq!(
        reg2.inverse_predicate("follows"),
        Some("followed_by".to_string())
    );
}

// ── Subtype inheritance ───────────────────────────────────────────────────────

#[test]
fn test_subtype_resolved_fields() {
    let (store, _dir) = open_store();
    let reg = NodeTypeRegistry::new(store).unwrap();

    reg.register_type(NodeTypeDef::new(
        "Animal",
        vec![
            FieldDef::required("name", FieldKind::Text),
            FieldDef::optional("weight", FieldKind::Float),
        ],
    ))
    .unwrap();

    reg.register_type(
        NodeTypeDef::new("Dog", vec![FieldDef::required("breed", FieldKind::Text)])
            .with_parent_types(vec!["Animal"]),
    )
    .unwrap();

    let fields = reg.resolved_fields("Dog");
    let names: Vec<_> = fields.iter().map(|f| f.name.as_str()).collect();

    // "breed" is Dog's own field (comes first), then Animal's fields.
    assert!(names.contains(&"breed"));
    assert!(names.contains(&"name"));
    assert!(names.contains(&"weight"));
    assert_eq!(names[0], "breed"); // own field first
}

#[test]
fn test_subtype_resolved_fields_own_field_wins() {
    let (store, _dir) = open_store();
    let reg = NodeTypeRegistry::new(store).unwrap();

    reg.register_type(NodeTypeDef::new(
        "Base",
        vec![FieldDef::optional("x", FieldKind::Int)],
    ))
    .unwrap();

    // Child declares 'x' as required — own declaration should win.
    reg.register_type(
        NodeTypeDef::new("Child", vec![FieldDef::required("x", FieldKind::Int)])
            .with_parent_types(vec!["Base"]),
    )
    .unwrap();

    let fields = reg.resolved_fields("Child");
    let x_field = fields.iter().find(|f| f.name == "x").unwrap();
    assert!(
        x_field.required,
        "child's required declaration should shadow parent's optional"
    );
    assert_eq!(fields.len(), 1, "field 'x' should appear only once");
}

#[test]
fn test_subtype_is_subtype_of() {
    let (store, _dir) = open_store();
    let reg = NodeTypeRegistry::new(store).unwrap();

    reg.register_type(NodeTypeDef::new("A", vec![])).unwrap();
    reg.register_type(NodeTypeDef::new("B", vec![]).with_parent_types(vec!["A"]))
        .unwrap();
    reg.register_type(NodeTypeDef::new("C", vec![]).with_parent_types(vec!["B"]))
        .unwrap();

    assert!(reg.is_subtype_of("A", "A")); // identity
    assert!(reg.is_subtype_of("B", "A")); // direct parent
    assert!(reg.is_subtype_of("C", "A")); // transitive
    assert!(reg.is_subtype_of("C", "B")); // direct parent
    assert!(!reg.is_subtype_of("A", "B")); // parent is not a subtype of child
    assert!(!reg.is_subtype_of("B", "C")); // parent is not a subtype of grandchild
}

#[test]
fn test_subtype_validate_inherits_required_fields() {
    let (store, _dir) = open_store();
    let reg = NodeTypeRegistry::new(store).unwrap();

    reg.register_type(NodeTypeDef::new(
        "Vehicle",
        vec![
            FieldDef::required("make", FieldKind::Text),
            FieldDef::required("year", FieldKind::Int),
        ],
    ))
    .unwrap();

    reg.register_type(
        NodeTypeDef::new("Car", vec![FieldDef::required("doors", FieldKind::Int)])
            .with_parent_types(vec!["Vehicle"]),
    )
    .unwrap();

    // Missing inherited fields → error.
    let errs = reg
        .validate_properties(
            "Car",
            &HashMap::from([
                ("doors".to_string(), polargraph_core::value::Value::Int(4)),
                // "make" and "year" missing
            ]),
        )
        .unwrap_err();
    let messages: Vec<_> = errs.iter().map(|e| e.field.as_str()).collect();
    assert!(messages.contains(&"make"));
    assert!(messages.contains(&"year"));

    // All fields supplied → ok.
    let result = reg.validate_properties(
        "Car",
        &HashMap::from([
            ("doors".to_string(), polargraph_core::value::Value::Int(4)),
            (
                "make".to_string(),
                polargraph_core::value::Value::Text("Toyota".into()),
            ),
            ("year".to_string(), polargraph_core::value::Value::Int(2020)),
        ]),
    );
    assert!(result.is_ok());
}

// ── Cycle detection ───────────────────────────────────────────────────────────

#[test]
fn test_subtype_cycle_rejected() {
    let (store, _dir) = open_store();
    let reg = NodeTypeRegistry::new(store).unwrap();

    reg.register_type(NodeTypeDef::new("X", vec![])).unwrap();
    reg.register_type(NodeTypeDef::new("Y", vec![]).with_parent_types(vec!["X"]))
        .unwrap();

    // Z extends Y, but Y extends X and X has no parents — ok so far.
    reg.register_type(NodeTypeDef::new("Z", vec![]).with_parent_types(vec!["Y"]))
        .unwrap();

    // Now try to make X extend Z — this would create X→Y→Z→X cycle.
    let result = reg.register_type(NodeTypeDef::new("X", vec![]).with_parent_types(vec!["Z"]));
    assert!(result.is_err(), "expected cycle detection error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("cycle"),
        "error should mention cycle: {err_msg}"
    );
}

#[test]
fn test_self_cycle_rejected() {
    let (store, _dir) = open_store();
    let reg = NodeTypeRegistry::new(store).unwrap();

    let result =
        reg.register_type(NodeTypeDef::new("Loop", vec![]).with_parent_types(vec!["Loop"]));
    assert!(result.is_err());
}
