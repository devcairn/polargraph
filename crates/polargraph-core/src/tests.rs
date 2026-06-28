//! Unit tests for polargraph-core types.

// ── id ────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod id_tests {
    use crate::id::{EdgeId, NodeId};

    #[test]
    fn node_id_new_is_unique() {
        let a = NodeId::new();
        let b = NodeId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn edge_id_new_is_unique() {
        let a = EdgeId::new();
        let b = EdgeId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn node_id_as_bytes_round_trips() {
        let id = NodeId::new();
        let bytes = id.as_bytes();
        let rebuilt = NodeId(uuid::Uuid::from_bytes(*bytes));
        assert_eq!(id, rebuilt);
    }

    #[test]
    fn node_id_default_is_unique_from_another_new() {
        let a = NodeId::default();
        let b = NodeId::default();
        assert_ne!(a, b);
    }

    #[test]
    fn node_id_display_is_hyphenated_uuid() {
        let id = NodeId::new();
        let s = id.to_string();
        // UUID v7 canonical form: 8-4-4-4-12
        assert_eq!(s.len(), 36);
        assert_eq!(s.chars().filter(|&c| c == '-').count(), 4);
    }

    #[test]
    fn node_id_ordering_matches_creation_order() {
        // UUID v7 is time-ordered, so later IDs sort higher.
        // Sleep isn't reliable in unit tests, but we can at least verify
        // that the Ord impl doesn't panic and is consistent.
        let a = NodeId::new();
        let b = NodeId::new();
        // a <= b (they may be equal in the same millisecond, but never a > b
        // when created sequentially in the same thread on a monotonic clock).
        assert!(a <= b);
    }
}

// ── temporal ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod temporal_tests {
    use crate::temporal::{BiTemporalRange, Timestamp};

    #[test]
    fn timestamp_now_is_positive() {
        assert!(Timestamp::now().0 > 0);
    }

    #[test]
    fn timestamp_end_of_time_is_max() {
        assert_eq!(Timestamp::END_OF_TIME.0, i64::MAX);
    }

    #[test]
    fn timestamp_be_bytes_round_trips() {
        let ts = Timestamp::now();
        let bytes = ts.to_be_bytes();
        let rebuilt = Timestamp::from_be_bytes(bytes);
        assert_eq!(ts, rebuilt);
    }

    #[test]
    fn timestamp_end_of_time_be_bytes_round_trips() {
        let ts = Timestamp::END_OF_TIME;
        assert_eq!(Timestamp::from_be_bytes(ts.to_be_bytes()), ts);
    }

    #[test]
    fn timestamp_ordering_is_lexicographic() {
        let earlier = Timestamp(1_000_000);
        let later = Timestamp(2_000_000);
        assert!(earlier < later);

        // BE bytes must also sort the same way (critical for RocksDB key ordering).
        assert!(earlier.to_be_bytes() < later.to_be_bytes());
    }

    #[test]
    fn bitemporal_assert_now_has_open_vt_end() {
        let vt_start = Timestamp::now();
        let bt = BiTemporalRange::assert_now(vt_start);
        assert_eq!(bt.vt_start, vt_start);
        assert_eq!(bt.vt_end, Timestamp::END_OF_TIME);
        assert!(bt.tt.0 > 0);
    }

    #[test]
    fn bitemporal_tt_is_at_least_vt_start_for_current_facts() {
        let vt_start = Timestamp::now();
        let bt = BiTemporalRange::assert_now(vt_start);
        // tt should be >= vt_start (recorded after or at the same moment).
        assert!(bt.tt >= vt_start);
    }
}

// ── value ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod value_tests {
    use crate::value::Value;

    #[test]
    fn from_bool() {
        assert_eq!(Value::from(true), Value::Bool(true));
        assert_eq!(Value::from(false), Value::Bool(false));
    }

    #[test]
    fn from_i64() {
        assert_eq!(Value::from(42i64), Value::Int(42));
        assert_eq!(Value::from(-1i64), Value::Int(-1));
    }

    #[test]
    fn from_f64() {
        match Value::from(3.14f64) {
            Value::Float(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn from_string() {
        assert_eq!(Value::from("hello"), Value::Text("hello".into()));
        assert_eq!(Value::from("hello".to_owned()), Value::Text("hello".into()));
    }

    #[test]
    fn null_variant() {
        let v = Value::Null;
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn blob_variant() {
        let data = vec![0u8, 1, 2, 255];
        let v = Value::Blob(data.clone());
        match v {
            Value::Blob(b) => assert_eq!(b, data),
            other => panic!("expected Blob, got {other:?}"),
        }
    }

    #[test]
    fn serde_round_trip_all_variants() {
        let cases = vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::Float(1.23456789),
            Value::Text("hello world".into()),
            Value::Blob(vec![0, 127, 255]),
        ];
        for v in cases {
            let json = serde_json::to_string(&v).unwrap();
            let rebuilt: Value = serde_json::from_str(&json).unwrap();
            assert_eq!(v, rebuilt, "serde round-trip failed for {json}");
        }
    }
}

// ── triple ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod triple_tests {
    use crate::{
        id::{EdgeId, NodeId},
        temporal::{BiTemporalRange, Timestamp},
        triple::{Predicate, Triple},
        value::Value,
    };

    fn temporal() -> BiTemporalRange {
        BiTemporalRange::assert_now(Timestamp::now())
    }

    #[test]
    fn relation_accessors() {
        let s = NodeId::new();
        let o = NodeId::new();
        let e = EdgeId::new();
        let t = Triple::Relation {
            subject: s,
            predicate: Predicate::new("knows"),
            object: o,
            edge_id: e,
            temporal: temporal(),
        };
        assert_eq!(t.subject(), s);
        assert_eq!(t.predicate().0, "knows");
        assert_eq!(t.temporal().vt_end, Timestamp::END_OF_TIME);
    }

    #[test]
    fn property_accessors() {
        let s = NodeId::new();
        let t = Triple::Property {
            subject: s,
            predicate: Predicate::new("age"),
            value: Value::Int(30),
            temporal: temporal(),
        };
        assert_eq!(t.subject(), s);
        assert_eq!(t.predicate().0, "age");
    }

    #[test]
    fn predicate_display() {
        let p = Predicate::new("reports-to");
        assert_eq!(p.to_string(), "reports-to");
    }
}

// ── view ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod view_tests {
    use crate::view::{EdgePresentation, NodeFilter, View, ViewId};
    

    fn make_view() -> View {
        let mut v = View::new("org-chart", "Org Chart");
        v.visible_predicates = ["reports-to", "name"].iter().map(|s| s.to_string()).collect();
        v.edge_presentations.insert(
            "reports-to".into(),
            EdgePresentation { label: "manages".into(), reverse_direction: true },
        );
        v
    }

    #[test]
    fn view_id_display() {
        let id = ViewId::new("my-view");
        assert_eq!(id.to_string(), "my-view");
    }

    #[test]
    fn edge_label_with_override() {
        let v = make_view();
        assert_eq!(v.edge_label("reports-to"), "manages");
    }

    #[test]
    fn edge_label_falls_back_to_canonical() {
        let v = make_view();
        assert_eq!(v.edge_label("name"), "name");
        assert_eq!(v.edge_label("unknown-pred"), "unknown-pred");
    }

    #[test]
    fn is_reversed_true_when_set() {
        let v = make_view();
        assert!(v.is_reversed("reports-to"));
    }

    #[test]
    fn is_reversed_false_when_not_set() {
        let v = make_view();
        assert!(!v.is_reversed("name"));
        assert!(!v.is_reversed("unknown"));
    }

    #[test]
    fn shows_predicate_when_in_filter() {
        let v = make_view();
        assert!(v.shows_predicate("reports-to"));
        assert!(v.shows_predicate("name"));
    }

    #[test]
    fn hides_predicate_not_in_filter() {
        let v = make_view();
        assert!(!v.shows_predicate("salary"));
        assert!(!v.shows_predicate("manages"));
    }

    #[test]
    fn empty_visible_predicates_shows_everything() {
        let v = View::new("all", "All");
        // visible_predicates is empty → show all
        assert!(v.shows_predicate("anything"));
        assert!(v.shows_predicate("reports-to"));
    }

    #[test]
    fn node_filter_by_types() {
        let f = NodeFilter::by_types(["Person", "Team"]);
        assert!(f.include_types.contains("Person"));
        assert!(f.include_types.contains("Team"));
        assert!(!f.include_types.contains("Project"));
    }

    #[test]
    fn view_new_has_no_filter() {
        let v = View::new("x", "X");
        assert!(v.node_filter.is_none());
        assert!(v.visible_predicates.is_empty());
        assert!(v.edge_presentations.is_empty());
    }
}
