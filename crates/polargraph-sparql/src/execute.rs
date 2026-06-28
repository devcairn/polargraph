//! In-process SPARQL execution helpers.
//!
//! These functions operate on pre-evaluated [`SparqlBindings`] — binding rows
//! that may have been produced by gRPC calls or assembled from mock data in
//! tests.  They implement SPARQL semantics that cannot be expressed as a simple
//! pattern query:
//!
//! - [`left_join`]: OPTIONAL / LEFT JOIN semantics
//! - [`execute_sparql_aggregations`]: GROUP BY + aggregate functions + HAVING

use std::collections::HashMap;

use crate::{
    response::{SparqlBindings, SparqlValue},
    translate::{SparqlAggFunc, SparqlAggregateSpec, SparqlFilter, SparqlLiteral},
};

// ── Left join (OPTIONAL) ──────────────────────────────────────────────────────

/// Apply SPARQL OPTIONAL (left join) semantics to two sets of binding rows.
///
/// For each `left` binding:
/// - Find all `right` bindings that are *compatible* (agree on every variable
///   present in both rows).
/// - If at least one compatible `right` binding exists, produce one merged row
///   per compatible right binding.
/// - If no compatible `right` binding exists, keep the `left` row unchanged
///   (right-side variables are absent from the result).
///
/// The optional `filter` is applied to merged rows — only merged rows that
/// satisfy the filter are kept.  Left rows that had no match are always kept.
pub fn left_join(
    left: Vec<SparqlBindings>,
    right: Vec<SparqlBindings>,
    filter: Option<&SparqlFilter>,
) -> Vec<SparqlBindings> {
    let mut result = Vec::with_capacity(left.len());

    for lb in &left {
        // Find all right bindings compatible with this left binding.
        let mut merged_any = false;
        for rb in &right {
            if compatible(lb, rb) {
                let merged = merge(lb, rb);
                // Apply the OPTIONAL filter if provided.
                if filter.map_or(true, |f| apply_sparql_filter(&merged, f)) {
                    result.push(merged);
                    merged_any = true;
                }
            }
        }
        if !merged_any {
            // No compatible right binding — keep left binding unchanged.
            result.push(lb.clone());
        }
    }

    result
}

/// Return true if two binding rows agree on every variable present in both.
fn compatible(a: &SparqlBindings, b: &SparqlBindings) -> bool {
    for (k, av) in a {
        if let Some(bv) = b.get(k) {
            if av != bv {
                return false;
            }
        }
    }
    true
}

/// Merge two compatible binding rows (right values override left for shared keys,
/// though compatible rows should agree on shared keys already).
fn merge(left: &SparqlBindings, right: &SparqlBindings) -> SparqlBindings {
    let mut out = left.clone();
    for (k, v) in right {
        out.insert(k.clone(), v.clone());
    }
    out
}

// ── Aggregation ───────────────────────────────────────────────────────────────

/// Apply GROUP BY aggregation to a flat list of binding rows.
///
/// Returns one result row per group.  Each row contains:
/// - The group-by variable bindings (same values for all rows in the group).
/// - One entry per aggregate spec, keyed by its alias.
///
/// If `having` is provided, only groups satisfying the filter are returned.
pub fn execute_sparql_aggregations(
    bindings: Vec<SparqlBindings>,
    group_by: &[String],
    aggregates: &[SparqlAggregateSpec],
    having: Option<&SparqlFilter>,
) -> Vec<SparqlBindings> {
    if aggregates.is_empty() && group_by.is_empty() {
        return bindings;
    }

    // Group rows by the serialized tuple of group-by variable values.
    // We keep an insertion-order list of group keys to preserve output order.
    let mut groups: HashMap<Vec<String>, Vec<SparqlBindings>> = HashMap::new();
    let mut group_order: Vec<Vec<String>> = Vec::new();

    for b in bindings {
        let key: Vec<String> = group_by
            .iter()
            .map(|v| match b.get(v) {
                Some(val) => serialize_for_key(val),
                None => String::new(),
            })
            .collect();
        let entry = groups.entry(key.clone()).or_insert_with(|| {
            group_order.push(key);
            Vec::new()
        });
        entry.push(b);
    }

    let mut result = Vec::new();

    for key in &group_order {
        let rows = &groups[key];

        // Build the result row.
        let mut out: SparqlBindings = HashMap::new();

        // Copy group-by variable values from the first row.
        if let Some(first) = rows.first() {
            for var in group_by {
                if let Some(val) = first.get(var) {
                    out.insert(var.clone(), val.clone());
                }
            }
        }

        // Compute each aggregate.
        for spec in aggregates {
            let agg_val = compute_aggregate(rows, &spec.func);
            out.insert(spec.alias.clone(), agg_val);
        }

        // Apply HAVING filter.
        if having.map_or(true, |f| apply_sparql_filter(&out, f)) {
            result.push(out);
        }
    }

    result
}

fn compute_aggregate(rows: &[SparqlBindings], func: &SparqlAggFunc) -> SparqlValue {
    match func {
        SparqlAggFunc::CountStar => SparqlValue::LiteralInt(rows.len() as i64),

        SparqlAggFunc::CountVar(var) => {
            let count = rows.iter().filter(|r| r.contains_key(var)).count();
            SparqlValue::LiteralInt(count as i64)
        }

        SparqlAggFunc::Sum(var) => {
            let mut sum_i: i64 = 0;
            let mut sum_f: f64 = 0.0;
            let mut is_float = false;
            for r in rows {
                match r.get(var) {
                    Some(SparqlValue::LiteralInt(n)) => sum_i += n,
                    Some(SparqlValue::LiteralFloat(f)) => {
                        sum_f += f;
                        is_float = true;
                    }
                    _ => {}
                }
            }
            if is_float {
                SparqlValue::LiteralFloat(sum_f + sum_i as f64)
            } else {
                SparqlValue::LiteralInt(sum_i)
            }
        }

        SparqlAggFunc::Avg(var) => {
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;
            for r in rows {
                match r.get(var) {
                    Some(SparqlValue::LiteralInt(n)) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Some(SparqlValue::LiteralFloat(f)) => {
                        sum += f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count == 0 {
                SparqlValue::LiteralFloat(0.0)
            } else {
                SparqlValue::LiteralFloat(sum / count as f64)
            }
        }

        SparqlAggFunc::Min(var) => {
            let mut min_i: Option<i64> = None;
            let mut min_f: Option<f64> = None;
            let mut is_float = false;
            for r in rows {
                match r.get(var) {
                    Some(SparqlValue::LiteralInt(n)) => {
                        min_i = Some(min_i.map_or(*n, |m: i64| m.min(*n)));
                    }
                    Some(SparqlValue::LiteralFloat(f)) => {
                        min_f = Some(min_f.map_or(*f, |m: f64| if *f < m { *f } else { m }));
                        is_float = true;
                    }
                    _ => {}
                }
            }
            if is_float {
                SparqlValue::LiteralFloat(min_f.unwrap_or(0.0) + min_i.unwrap_or(0) as f64)
            } else {
                SparqlValue::LiteralInt(min_i.unwrap_or(0))
            }
        }

        SparqlAggFunc::Max(var) => {
            let mut max_i: Option<i64> = None;
            let mut max_f: Option<f64> = None;
            let mut is_float = false;
            for r in rows {
                match r.get(var) {
                    Some(SparqlValue::LiteralInt(n)) => {
                        max_i = Some(max_i.map_or(*n, |m: i64| m.max(*n)));
                    }
                    Some(SparqlValue::LiteralFloat(f)) => {
                        max_f = Some(max_f.map_or(*f, |m: f64| if *f > m { *f } else { m }));
                        is_float = true;
                    }
                    _ => {}
                }
            }
            if is_float {
                SparqlValue::LiteralFloat(max_f.unwrap_or(0.0) + max_i.unwrap_or(0) as f64)
            } else {
                SparqlValue::LiteralInt(max_i.unwrap_or(0))
            }
        }

        SparqlAggFunc::GroupConcat { var, separator } => {
            let sep = separator.as_deref().unwrap_or(" ");
            let parts: Vec<String> = rows
                .iter()
                .filter_map(|r| r.get(var))
                .map(|v| match v {
                    SparqlValue::Uri(id) => format!("urn:uuid:{}", id.0),
                    SparqlValue::Literal(s) => s.clone(),
                    SparqlValue::LiteralInt(n) => n.to_string(),
                    SparqlValue::LiteralFloat(f) => f.to_string(),
                    SparqlValue::LiteralBool(b) => b.to_string(),
                })
                .collect();
            SparqlValue::Literal(parts.join(sep))
        }

        SparqlAggFunc::Sample(var) => rows
            .first()
            .and_then(|r| r.get(var))
            .cloned()
            .unwrap_or(SparqlValue::Literal(String::new())),
    }
}

// ── Filter evaluation ─────────────────────────────────────────────────────────

/// Evaluate a `SparqlFilter` against a single binding row.
pub fn apply_sparql_filter(binding: &SparqlBindings, filter: &SparqlFilter) -> bool {
    match filter {
        SparqlFilter::Bound(var) => binding.contains_key(var),

        SparqlFilter::VarEq(a, b) => match (binding.get(a), binding.get(b)) {
            (Some(av), Some(bv)) => av == bv,
            _ => false,
        },

        SparqlFilter::IsIri(var) => {
            matches!(binding.get(var), Some(SparqlValue::Uri(_)))
        }

        SparqlFilter::Not(inner) => !apply_sparql_filter(binding, inner),
        SparqlFilter::And(a, b) => {
            apply_sparql_filter(binding, a) && apply_sparql_filter(binding, b)
        }
        SparqlFilter::Or(a, b) => {
            apply_sparql_filter(binding, a) || apply_sparql_filter(binding, b)
        }

        SparqlFilter::GreaterThan(var, lit) => compare_var_lit(binding, var, lit) > 0,
        SparqlFilter::LessThan(var, lit) => compare_var_lit(binding, var, lit) < 0,
        SparqlFilter::GreaterOrEqual(var, lit) => compare_var_lit(binding, var, lit) >= 0,
        SparqlFilter::LessOrEqual(var, lit) => compare_var_lit(binding, var, lit) <= 0,
        SparqlFilter::EqualLiteral(var, lit) => compare_var_lit(binding, var, lit) == 0,
        SparqlFilter::NotEqualLiteral(var, lit) => compare_var_lit(binding, var, lit) != 0,
    }
}

/// Compare a bound variable's value against a literal. Returns an integer like `cmp()`:
/// negative = var < lit, 0 = equal, positive = var > lit.
/// Returns `i32::MIN` when the variable is unbound or types are incomparable.
fn compare_var_lit(binding: &SparqlBindings, var: &str, lit: &SparqlLiteral) -> i32 {
    let Some(val) = binding.get(var) else {
        return i32::MIN;
    };
    match (val, lit) {
        (SparqlValue::LiteralInt(v), SparqlLiteral::Int(l)) => v.cmp(l) as i32,
        (SparqlValue::LiteralFloat(v), SparqlLiteral::Float(l)) => {
            v.partial_cmp(l).map(|o| o as i32).unwrap_or(i32::MIN)
        }
        (SparqlValue::LiteralInt(v), SparqlLiteral::Float(l)) => (*v as f64)
            .partial_cmp(l)
            .map(|o| o as i32)
            .unwrap_or(i32::MIN),
        (SparqlValue::LiteralFloat(v), SparqlLiteral::Int(l)) => v
            .partial_cmp(&(*l as f64))
            .map(|o| o as i32)
            .unwrap_or(i32::MIN),
        (SparqlValue::Literal(v), SparqlLiteral::Str(l)) => v.cmp(l) as i32,
        (SparqlValue::LiteralBool(v), SparqlLiteral::Bool(l)) => v.cmp(l) as i32,
        // Numeric vs string: incomparable
        _ => i32::MIN,
    }
}

/// Produce a stable string key for grouping.
fn serialize_for_key(val: &SparqlValue) -> String {
    match val {
        SparqlValue::Uri(id) => format!("uri:{}", id.0),
        SparqlValue::Literal(s) => format!("lit:{}", s),
        SparqlValue::LiteralInt(n) => format!("int:{}", n),
        SparqlValue::LiteralFloat(f) => format!("flt:{}", f.to_bits()),
        SparqlValue::LiteralBool(b) => format!("bool:{}", b),
    }
}
