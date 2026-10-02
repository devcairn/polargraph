//! In-process SPARQL execution helpers.
//!
//! These functions operate on pre-evaluated [`SparqlBindings`] — binding rows
//! that may have been produced by gRPC calls or assembled from mock data in
//! tests.  They implement SPARQL semantics that cannot be expressed as a simple
//! pattern query:
//!
//! - [`left_join`]: OPTIONAL / LEFT JOIN semantics
//! - [`execute_sparql_aggregations`]: GROUP BY + aggregate functions + HAVING
//! - [`order_bindings`]: ORDER BY
//! - [`apply_sparql_filter`] / [`eval_filter`]: FILTER with SPARQL's error rules

use std::{cmp::Ordering, collections::HashMap};

use crate::{
    names::IriNames,
    response::{SparqlBindings, SparqlValue},
    translate::{CmpOp, FilterExpr, SparqlAggFunc, SparqlAggregateSpec, SparqlFilter, SparqlOrder},
    values::{numeric, order_cmp, sparql_cmp, sparql_eq, term_key, Num, XSD},
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
            if term_key(av) != term_key(bv) {
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
/// - One entry per aggregate spec, keyed by its alias — absent when the
///   aggregate is unbound (`MIN` / `MAX` / `SAMPLE` of an empty group, or a
///   non-numeric value in `SUM` / `AVG`, as SPARQL 1.1 §18.5 defines).
///
/// Groups are keyed by RDF term identity (`1` and `1.0` are different
/// groups). If `having` is provided, only groups it accepts are returned.
///
/// `names` renders URIs inside `GROUP_CONCAT` results and orders IRIs for
/// `MIN` / `MAX`.
pub fn execute_sparql_aggregations(
    bindings: Vec<SparqlBindings>,
    group_by: &[String],
    aggregates: &[SparqlAggregateSpec],
    having: Option<&SparqlFilter>,
    names: &IriNames,
) -> Vec<SparqlBindings> {
    if aggregates.is_empty() && group_by.is_empty() {
        return bindings;
    }

    // Group rows by the term keys of the group-by variables, keeping
    // first-seen group order.
    let mut groups: HashMap<Vec<String>, Vec<SparqlBindings>> = HashMap::new();
    let mut group_order: Vec<Vec<String>> = Vec::new();

    for b in bindings {
        let key: Vec<String> = group_by
            .iter()
            .map(|v| b.get(v).map(term_key).unwrap_or_default())
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
            if let Some(agg_val) = compute_aggregate(rows, &spec.func, names) {
                out.insert(spec.alias.clone(), agg_val);
            }
        }

        // Apply HAVING filter.
        if having.map_or(true, |f| apply_sparql_filter_named(&out, f, names)) {
            result.push(out);
        }
    }

    result
}

/// The values `var` is bound to across `rows`.
fn bound<'a>(rows: &'a [SparqlBindings], var: &'a str) -> impl Iterator<Item = &'a SparqlValue> {
    rows.iter().filter_map(move |r| r.get(var))
}

/// One aggregate over a group; `None` when it is unbound.
fn compute_aggregate(
    rows: &[SparqlBindings],
    func: &SparqlAggFunc,
    names: &IriNames,
) -> Option<SparqlValue> {
    Some(match func {
        SparqlAggFunc::CountStar => SparqlValue::LiteralInt(rows.len() as i64),

        SparqlAggFunc::CountVar(var) => SparqlValue::LiteralInt(bound(rows, var).count() as i64),

        // Integer if every input is an integer (and the sum fits), else a
        // double; a non-numeric input makes the sum unbound.
        SparqlAggFunc::Sum(var) => {
            let mut int_sum: Option<i64> = Some(0);
            let mut dbl_sum = 0.0;
            for v in bound(rows, var) {
                let n = numeric(v)?;
                dbl_sum += n.as_f64();
                int_sum = match (int_sum, n) {
                    (Some(acc), Num::Int(x)) => acc.checked_add(x),
                    _ => None,
                };
            }
            match int_sum {
                Some(n) => SparqlValue::LiteralInt(n),
                None => SparqlValue::LiteralFloat(dbl_sum),
            }
        }

        // A double (the spec's xsd:decimal isn't stored); 0 for no input.
        SparqlAggFunc::Avg(var) => {
            let mut sum = 0.0;
            let mut count = 0usize;
            for v in bound(rows, var) {
                sum += numeric(v)?.as_f64();
                count += 1;
            }
            if count == 0 {
                SparqlValue::LiteralInt(0)
            } else {
                SparqlValue::LiteralFloat(sum / count as f64)
            }
        }

        // The least / greatest value in ORDER BY order; unbound when empty.
        SparqlAggFunc::Min(var) => bound(rows, var)
            .min_by(|a, b| order_cmp(Some(a), Some(b), names))?
            .clone(),
        SparqlAggFunc::Max(var) => bound(rows, var)
            .max_by(|a, b| order_cmp(Some(a), Some(b), names))?
            .clone(),

        SparqlAggFunc::GroupConcat { var, separator } => {
            let sep = separator.as_deref().unwrap_or(" ");
            let parts: Vec<String> = bound(rows, var)
                .map(|v| match v {
                    SparqlValue::Uri(id) => names.iri(id),
                    literal => literal.lexical().unwrap_or_default(),
                })
                .collect();
            SparqlValue::Literal(parts.join(sep))
        }

        SparqlAggFunc::Sample(var) => bound(rows, var).next()?.clone(),
    })
}

// ── ORDER BY ──────────────────────────────────────────────────────────────────

/// Sort rows by `order` keys (SPARQL `ORDER BY`), stably, using the total
/// order of [`order_cmp`].
pub fn order_bindings(rows: &mut [SparqlBindings], order: &[SparqlOrder], names: &IriNames) {
    if order.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for key in order {
            let o = order_cmp(a.get(&key.var), b.get(&key.var), names);
            let o = if key.descending { o.reverse() } else { o };
            if o != Ordering::Equal {
                return o;
            }
        }
        Ordering::Equal
    });
}

// ── Filter evaluation ─────────────────────────────────────────────────────────

/// Evaluate a `SparqlFilter` against a single binding row: a row is kept only
/// when the filter is `true` — `false` and errors (an unbound variable, a
/// comparison of incomparable values) both remove it (SPARQL 1.1 §17.2).
///
/// `STR` of a node renders `urn:uuid:…` here; use
/// [`apply_sparql_filter_named`] when the filter uses IRI text
/// ([`SparqlFilter::uses_iri_text`]).
pub fn apply_sparql_filter(binding: &SparqlBindings, filter: &SparqlFilter) -> bool {
    apply_sparql_filter_named(binding, filter, &IriNames::new(Default::default(), false))
}

/// [`apply_sparql_filter`] with IRIs for `STR` of a node.
pub fn apply_sparql_filter_named(
    binding: &SparqlBindings,
    filter: &SparqlFilter,
    names: &IriNames,
) -> bool {
    eval_filter(binding, filter, names) == Some(true)
}

/// Three-valued filter evaluation: `Some(true)`, `Some(false)`, or `None` for
/// an error. `!` keeps errors; `&&` / `||` follow SPARQL's error rules
/// (`false && error = false`, `true || error = true`).
pub fn eval_filter(
    binding: &SparqlBindings,
    filter: &SparqlFilter,
    names: &IriNames,
) -> Option<bool> {
    let get = |var: &str| binding.get(var);
    match filter {
        SparqlFilter::Bound(var) => Some(binding.contains_key(var)),

        SparqlFilter::VarEq(a, b) => sparql_eq(get(a)?, get(b)?),
        SparqlFilter::SameTerm(a, b) => Some(term_key(get(a)?) == term_key(get(b)?)),

        SparqlFilter::IsIri(var) => Some(matches!(
            get(var)?,
            SparqlValue::Uri(_) | SparqlValue::Iri(_)
        )),
        SparqlFilter::IsLiteral(var) => Some(!matches!(
            get(var)?,
            SparqlValue::Uri(_) | SparqlValue::Iri(_)
        )),
        // PolarGraph has no blank nodes at query time.
        SparqlFilter::IsBlank(var) => get(var).map(|_| false),

        SparqlFilter::Not(inner) => eval_filter(binding, inner, names).map(|b| !b),
        SparqlFilter::And(a, b) => match (
            eval_filter(binding, a, names),
            eval_filter(binding, b, names),
        ) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        SparqlFilter::Or(a, b) => match (
            eval_filter(binding, a, names),
            eval_filter(binding, b, names),
        ) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },

        SparqlFilter::GreaterThan(var, lit) => {
            sparql_cmp(get(var)?, &lit.to_value()).map(Ordering::is_gt)
        }
        SparqlFilter::LessThan(var, lit) => {
            sparql_cmp(get(var)?, &lit.to_value()).map(Ordering::is_lt)
        }
        SparqlFilter::GreaterOrEqual(var, lit) => {
            sparql_cmp(get(var)?, &lit.to_value()).map(Ordering::is_ge)
        }
        SparqlFilter::LessOrEqual(var, lit) => {
            sparql_cmp(get(var)?, &lit.to_value()).map(Ordering::is_le)
        }
        SparqlFilter::EqualLiteral(var, lit) => sparql_eq(get(var)?, &lit.to_value()),
        SparqlFilter::NotEqualLiteral(var, lit) => {
            sparql_eq(get(var)?, &lit.to_value()).map(|b| !b)
        }
        SparqlFilter::Compare { op, left, right } => {
            let (l, r) = (
                eval_expr(binding, left, names)?,
                eval_expr(binding, right, names)?,
            );
            match op {
                CmpOp::Eq => sparql_eq(&l, &r),
                CmpOp::Lt => sparql_cmp(&l, &r).map(Ordering::is_lt),
                CmpOp::Le => sparql_cmp(&l, &r).map(Ordering::is_le),
                CmpOp::Gt => sparql_cmp(&l, &r).map(Ordering::is_gt),
                CmpOp::Ge => sparql_cmp(&l, &r).map(Ordering::is_ge),
            }
        }
    }
}

const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

/// Evaluate a filter operand; `None` is an error (an unbound variable, or a
/// built-in applied to the wrong kind of term).
pub fn eval_expr(
    binding: &SparqlBindings,
    e: &FilterExpr,
    names: &IriNames,
) -> Option<SparqlValue> {
    Some(match e {
        FilterExpr::Var(v) => binding.get(v)?.clone(),
        FilterExpr::Const(c) => c.clone(),
        FilterExpr::Str(x) => SparqlValue::Literal(match eval_expr(binding, x, names)? {
            SparqlValue::Uri(id) => names.iri(&id),
            SparqlValue::Iri(iri) => iri,
            literal => literal.lexical()?,
        }),
        FilterExpr::Lang(x) => match eval_expr(binding, x, names)? {
            SparqlValue::Uri(_) | SparqlValue::Iri(_) => return None,
            SparqlValue::LangLiteral { lang, .. } => SparqlValue::Literal(lang),
            _ => SparqlValue::Literal(String::new()),
        },
        FilterExpr::Datatype(x) => SparqlValue::Iri(match eval_expr(binding, x, names)? {
            SparqlValue::Uri(_) | SparqlValue::Iri(_) => return None,
            SparqlValue::Literal(_) => format!("{XSD}string"),
            SparqlValue::LiteralInt(_) => format!("{XSD}integer"),
            SparqlValue::LiteralFloat(_) => format!("{XSD}double"),
            SparqlValue::LiteralBool(_) => format!("{XSD}boolean"),
            SparqlValue::LangLiteral { .. } => RDF_LANG_STRING.to_string(),
            SparqlValue::TypedLiteral { datatype, .. } => datatype,
        }),
    })
}
