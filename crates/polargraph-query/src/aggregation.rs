//! Post-query aggregation: GROUP BY, ORDER BY, SKIP.
//!
//! Invoked after `execute_query` / `execute_recursive` when the Cypher RETURN clause
//! contains aggregate functions (`count(*)`, `count(var)`, `collect(var)`).
//!
//! Non-aggregation queries with ORDER BY or SKIP also flow through
//! [`apply_aggregations`] — `specs` will simply be empty in that case.

use std::collections::HashMap;

use polargraph_core::{id::NodeId, value::Value};

use crate::datalog::Bindings;

// ── Public types ──────────────────────────────────────────────────────────────

/// Supported aggregate functions.
#[derive(Debug, Clone)]
pub enum AggFunc {
    /// `count(*)` — count all rows in the group.
    CountStar,
    /// `count(var)` — count rows where `var` is bound.
    Count(String),
    /// `collect(var)` — collect all NodeId values of `var` as a JSON array of UUID strings.
    Collect(String),
}

/// A single aggregation in the RETURN clause (e.g. `count(*) AS n`).
#[derive(Debug, Clone)]
pub struct AggregationSpec {
    pub func: AggFunc,
    /// The alias used in the RETURN clause (e.g. `n` in `count(*) AS n`).
    pub alias: String,
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    Asc,
    Desc,
}

/// A single ORDER BY sort key.
#[derive(Debug, Clone)]
pub struct OrderSpec {
    /// Variable name or aggregate alias to sort by.
    pub key: String,
    pub direction: SortDir,
}

/// One result row after aggregation and pagination.
#[derive(Debug)]
pub struct AggregatedRow {
    /// Non-aggregated group-by variable bindings (NodeId values).
    pub group_keys: HashMap<String, NodeId>,
    /// Computed aggregate values, keyed by alias.
    pub agg_values: HashMap<String, Value>,
}

// ── Core function ─────────────────────────────────────────────────────────────

/// Apply GROUP BY aggregations, ORDER BY, SKIP, and LIMIT to a flat binding set.
///
/// When `specs` is empty, no grouping is performed and the bindings are wrapped
/// as-is into [`AggregatedRow`] values. ORDER BY and SKIP are still applied.
///
/// Ordering on aggregate aliases uses Value comparison (Int < Float < Text).
/// Ordering on group-key variables falls back to UUID string comparison.
pub fn apply_aggregations(
    bindings: Vec<Bindings>,
    group_keys: &[String],
    specs: &[AggregationSpec],
    order_by: &[OrderSpec],
    skip: Option<usize>,
    limit: Option<usize>,
) -> Vec<AggregatedRow> {
    if specs.is_empty() {
        let mut rows: Vec<AggregatedRow> = bindings
            .into_iter()
            .map(|b| AggregatedRow { group_keys: b, agg_values: HashMap::new() })
            .collect();
        apply_order_skip_limit(&mut rows, order_by, skip, limit);
        return rows;
    }

    // Bucket bindings by their serialized group-key tuple, maintaining insertion order.
    let mut bucket_order: Vec<Vec<u8>> = Vec::new();
    let mut buckets: HashMap<Vec<u8>, Vec<Bindings>> = HashMap::new();

    for binding in bindings {
        let key = group_key_bytes(&binding, group_keys);
        if !buckets.contains_key(&key) {
            bucket_order.push(key.clone());
        }
        buckets.entry(key).or_default().push(binding);
    }

    let mut rows: Vec<AggregatedRow> = bucket_order
        .into_iter()
        .map(|key| {
            let group = &buckets[&key];
            let group_keys_map: HashMap<String, NodeId> = group
                .first()
                .map(|b| {
                    group_keys
                        .iter()
                        .filter_map(|k| b.get(k).map(|&id| (k.clone(), id)))
                        .collect()
                })
                .unwrap_or_default();
            let agg_values: HashMap<String, Value> = specs
                .iter()
                .map(|spec| (spec.alias.clone(), compute_agg(&spec.func, group)))
                .collect();
            AggregatedRow { group_keys: group_keys_map, agg_values }
        })
        .collect();

    apply_order_skip_limit(&mut rows, order_by, skip, limit);
    rows
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn group_key_bytes(binding: &Bindings, group_keys: &[String]) -> Vec<u8> {
    let mut out = Vec::with_capacity(group_keys.len() * 17);
    for k in group_keys {
        match binding.get(k) {
            Some(id) => {
                out.push(1u8);
                out.extend_from_slice(id.as_bytes());
            }
            None => {
                out.push(0u8);
                out.extend_from_slice(&[0u8; 16]);
            }
        }
    }
    out
}

fn compute_agg(func: &AggFunc, group: &[Bindings]) -> Value {
    match func {
        AggFunc::CountStar => Value::Int(group.len() as i64),
        AggFunc::Count(var) => {
            let n = group.iter().filter(|b| b.contains_key(var.as_str())).count();
            Value::Int(n as i64)
        }
        AggFunc::Collect(var) => {
            let mut ids: Vec<String> = group
                .iter()
                .filter_map(|b| b.get(var.as_str()))
                .map(|id| id.to_string())
                .collect();
            ids.dedup();
            Value::Text(format_json_array(&ids))
        }
    }
}

fn format_json_array(items: &[String]) -> String {
    if items.is_empty() {
        return "[]".to_string();
    }
    let quoted: Vec<String> = items.iter().map(|s| format!("\"{}\"", s)).collect();
    format!("[{}]", quoted.join(","))
}

fn apply_order_skip_limit(
    rows: &mut Vec<AggregatedRow>,
    order_by: &[OrderSpec],
    skip: Option<usize>,
    limit: Option<usize>,
) {
    if !order_by.is_empty() {
        rows.sort_by(|a, b| {
            for spec in order_by {
                let va = get_sort_key(a, &spec.key);
                let vb = get_sort_key(b, &spec.key);
                let ord = compare_values(&va, &vb);
                let ord = if spec.direction == SortDir::Desc { ord.reverse() } else { ord };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(s) = skip {
        if s >= rows.len() {
            rows.clear();
            return;
        }
        rows.drain(..s);
    }

    if let Some(l) = limit {
        rows.truncate(l);
    }
}

/// Resolve the sort key for a result row: check aggregate aliases first, then group keys.
fn get_sort_key(row: &AggregatedRow, key: &str) -> Option<Value> {
    if let Some(v) = row.agg_values.get(key) {
        return Some(v.clone());
    }
    if let Some(id) = row.group_keys.get(key) {
        return Some(Value::Text(id.to_string()));
    }
    None
}

fn compare_values(a: &Option<Value>, b: &Option<Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(Value::Int(x)), Some(Value::Int(y))) => x.cmp(y),
        (Some(Value::Float(x)), Some(Value::Float(y))) => {
            x.partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        (Some(Value::Text(x)), Some(Value::Text(y))) => x.cmp(y),
        (Some(Value::Bool(x)), Some(Value::Bool(y))) => x.cmp(y),
        _ => Ordering::Equal,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::id::NodeId;

    fn make_bindings(pairs: &[(&str, NodeId)]) -> Bindings {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    // ── count(*) with grouping ────────────────────────────────────────────────

    #[test]
    fn count_star_groups_by_key() {
        let alice = NodeId::new();
        let bob = NodeId::new();

        let bindings = vec![
            make_bindings(&[("a", alice)]),
            make_bindings(&[("a", alice)]),
            make_bindings(&[("a", bob)]),
        ];

        let specs = vec![AggregationSpec { func: AggFunc::CountStar, alias: "n".into() }];
        let mut rows = apply_aggregations(bindings, &["a".to_string()], &specs, &[], None, None);

        // Two groups: alice (count=2) and bob (count=1)
        assert_eq!(rows.len(), 2);
        rows.sort_by_key(|r| *r.group_keys.get("a").unwrap() == alice);

        let alice_row = rows.iter().find(|r| r.group_keys.get("a") == Some(&alice)).unwrap();
        let bob_row   = rows.iter().find(|r| r.group_keys.get("a") == Some(&bob)).unwrap();
        assert_eq!(alice_row.agg_values.get("n"), Some(&Value::Int(2)));
        assert_eq!(bob_row.agg_values.get("n"), Some(&Value::Int(1)));
    }

    #[test]
    fn count_star_no_groups_counts_all() {
        let ids: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();
        let bindings: Vec<Bindings> = ids.iter().map(|&id| make_bindings(&[("a", id)])).collect();

        // group_keys is empty → all 5 rows are in the same universal group → one row with count=5
        let specs = vec![AggregationSpec { func: AggFunc::CountStar, alias: "n".into() }];
        let rows = apply_aggregations(bindings, &[], &specs, &[], None, None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].agg_values.get("n"), Some(&Value::Int(5)));
    }

    // ── collect() ─────────────────────────────────────────────────────────────

    #[test]
    fn collect_produces_json_array() {
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let bindings = vec![
            make_bindings(&[("a", alice), ("b", bob)]),
            make_bindings(&[("a", alice), ("b", carol)]),
        ];

        let specs = vec![AggregationSpec { func: AggFunc::Collect("b".into()), alias: "bs".into() }];
        let rows = apply_aggregations(bindings, &["a".to_string()], &specs, &[], None, None);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.group_keys.get("a"), Some(&alice));
        let bs = row.agg_values.get("bs").unwrap();
        if let Value::Text(s) = bs {
            assert!(s.starts_with('[') && s.ends_with(']'));
            assert!(s.contains(&bob.to_string()));
            assert!(s.contains(&carol.to_string()));
        } else {
            panic!("expected Value::Text for collect, got {:?}", bs);
        }
    }

    // ── count(var) ────────────────────────────────────────────────────────────

    #[test]
    fn count_var_counts_bound_rows() {
        let alice = NodeId::new();
        let bob = NodeId::new();

        // Three rows with group key `a=alice`: two have `b` bound, one does not.
        let bindings = vec![
            make_bindings(&[("a", alice), ("b", bob)]),
            make_bindings(&[("a", alice), ("b", bob)]),
            make_bindings(&[("a", alice)]), // `b` absent
        ];

        let specs = vec![AggregationSpec { func: AggFunc::Count("b".into()), alias: "n".into() }];
        let rows = apply_aggregations(bindings, &["a".to_string()], &specs, &[], None, None);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].agg_values.get("n"), Some(&Value::Int(2)));
    }

    // ── ORDER BY ──────────────────────────────────────────────────────────────

    #[test]
    fn order_by_asc_on_aggregate() {
        let ids: Vec<NodeId> = (0..3).map(|_| NodeId::new()).collect();
        // Groups: id[0]→3 rows, id[1]→1 row, id[2]→2 rows
        let mut bindings = Vec::new();
        for _ in 0..3 { bindings.push(make_bindings(&[("a", ids[0])])); }
        for _ in 0..1 { bindings.push(make_bindings(&[("a", ids[1])])); }
        for _ in 0..2 { bindings.push(make_bindings(&[("a", ids[2])])); }

        let specs = vec![AggregationSpec { func: AggFunc::CountStar, alias: "n".into() }];
        let order_by = vec![OrderSpec { key: "n".into(), direction: SortDir::Asc }];
        let rows = apply_aggregations(bindings, &["a".to_string()], &specs, &order_by, None, None);

        let counts: Vec<i64> = rows.iter()
            .map(|r| if let Some(Value::Int(n)) = r.agg_values.get("n") { *n } else { 0 })
            .collect();
        assert_eq!(counts, vec![1, 2, 3]);
    }

    #[test]
    fn order_by_desc_on_aggregate() {
        let ids: Vec<NodeId> = (0..3).map(|_| NodeId::new()).collect();
        let mut bindings = Vec::new();
        for _ in 0..3 { bindings.push(make_bindings(&[("a", ids[0])])); }
        for _ in 0..1 { bindings.push(make_bindings(&[("a", ids[1])])); }
        for _ in 0..2 { bindings.push(make_bindings(&[("a", ids[2])])); }

        let specs = vec![AggregationSpec { func: AggFunc::CountStar, alias: "n".into() }];
        let order_by = vec![OrderSpec { key: "n".into(), direction: SortDir::Desc }];
        let rows = apply_aggregations(bindings, &["a".to_string()], &specs, &order_by, None, None);

        let counts: Vec<i64> = rows.iter()
            .map(|r| if let Some(Value::Int(n)) = r.agg_values.get("n") { *n } else { 0 })
            .collect();
        assert_eq!(counts, vec![3, 2, 1]);
    }

    // ── SKIP + LIMIT ──────────────────────────────────────────────────────────

    #[test]
    fn skip_and_limit_combined() {
        let ids: Vec<NodeId> = (0..10).map(|_| NodeId::new()).collect();
        let bindings: Vec<Bindings> = ids.iter().map(|&id| make_bindings(&[("a", id)])).collect();

        // No aggregation, just skip + limit
        let rows = apply_aggregations(bindings, &[], &[], &[], Some(3), Some(4));

        assert_eq!(rows.len(), 4);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.group_keys.get("a"), Some(&ids[3 + i]));
        }
    }

    #[test]
    fn skip_beyond_end_returns_empty() {
        let ids: Vec<NodeId> = (0..3).map(|_| NodeId::new()).collect();
        let bindings: Vec<Bindings> = ids.iter().map(|&id| make_bindings(&[("a", id)])).collect();

        let rows = apply_aggregations(bindings, &[], &[], &[], Some(10), None);
        assert!(rows.is_empty());
    }

    // ── No aggregation — passthrough ──────────────────────────────────────────

    #[test]
    fn no_aggregation_passthrough() {
        let ids: Vec<NodeId> = (0..5).map(|_| NodeId::new()).collect();
        let bindings: Vec<Bindings> = ids.iter().map(|&id| make_bindings(&[("a", id)])).collect();

        let rows = apply_aggregations(bindings, &[], &[], &[], None, None);
        assert_eq!(rows.len(), 5);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.group_keys.get("a"), Some(&ids[i]));
            assert!(row.agg_values.is_empty());
        }
    }
}
