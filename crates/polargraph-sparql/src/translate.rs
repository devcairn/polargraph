//! Translate a parsed `spargebra::Query` into PolarGraph native query types.

use crate::SparqlError;
use polargraph_core::id::NodeId;
use polargraph_query::{Rule, Term, VarPattern};
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, PropertyPathExpression,
};
use spargebra::term::{NamedNodePattern, TermPattern};

// ── Aggregate types ───────────────────────────────────────────────────────────

/// A SPARQL aggregate function applied to a group.
#[derive(Debug, Clone)]
pub enum SparqlAggFunc {
    /// `COUNT(*)` — count all rows in the group.
    CountStar,
    /// `COUNT(?var)` — count rows where `var` is bound.
    CountVar(String),
    /// `SUM(?var)` — sum numeric values of `var`.
    Sum(String),
    /// `AVG(?var)` — arithmetic mean of numeric values of `var`.
    Avg(String),
    /// `MIN(?var)` — minimum numeric value.
    Min(String),
    /// `MAX(?var)` — maximum numeric value.
    Max(String),
    /// `GROUP_CONCAT(?var; SEPARATOR=sep)` — concatenate values.
    GroupConcat { var: String, separator: Option<String> },
    /// `SAMPLE(?var)` — return an arbitrary value from the group.
    Sample(String),
}

/// A single aggregation in a GROUP BY clause.
#[derive(Debug, Clone)]
pub struct SparqlAggregateSpec {
    /// Output variable name (the alias after `AS`).
    pub alias: String,
    pub func: SparqlAggFunc,
}

// ── SPARQL-star ───────────────────────────────────────────────────────────────

/// An edge annotation lookup step derived from a SPARQL-star quoted triple.
///
/// Represents a pattern like `<<:Alice :knows :Bob>> :since ?date` where:
/// - The inner triple `<<:Alice :knows :Bob>>` identifies an edge
/// - `:since` is the annotation predicate
/// - `?date` is the variable to bind the annotation value to
#[derive(Debug, Clone)]
pub struct EdgeAnnotationStep {
    /// Subject of the inner (quoted) triple.
    pub edge_subject: Term,
    /// Predicate of the inner (quoted) triple.
    pub edge_predicate: String,
    /// Object of the inner (quoted) triple.
    pub edge_object: Term,
    /// Annotation predicate (the outer predicate).
    pub annot_predicate: String,
    /// Variable to bind the annotation value.
    pub value_var: String,
}

// ── Filter literal ────────────────────────────────────────────────────────────

/// A typed literal used as the right-hand side of a FILTER comparison.
#[derive(Debug, Clone, PartialEq)]
pub enum SparqlLiteral {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

// ── Public output types ───────────────────────────────────────────────────────

/// A single disjunct branch produced by translation.
/// Each branch is a conjunction of patterns evaluated together.
#[derive(Debug, Default, Clone)]
pub struct Branch {
    pub patterns: Vec<VarPattern>,
    pub rules: Vec<Rule>,
    pub filters: Vec<SparqlFilter>,
    /// Optional branches (from OPTIONAL / LEFT JOIN).
    ///
    /// After evaluating `patterns`, each optional branch is left-joined:
    /// its patterns are evaluated and the result is merged with the main
    /// branch bindings. If no match is found for a given left binding, the
    /// left binding is kept unchanged (right vars absent).
    pub optional_branches: Vec<Branch>,
    /// SPARQL-star annotation lookup steps.
    ///
    /// Each step resolves a quoted triple to an edge ID and fetches annotation
    /// values, binding the result to `value_var`.
    pub edge_annotation_steps: Vec<EdgeAnnotationStep>,
    /// Named graph IRI (from `GRAPH <iri> { ... }`).
    ///
    /// When set, pattern evaluation is scoped to the named graph / View
    /// identified by this IRI.
    pub graph_iri: Option<String>,
}

/// A post-filter applied in-process after gRPC query results are received.
#[derive(Debug, Clone)]
pub enum SparqlFilter {
    /// `FILTER(BOUND(?x))`
    Bound(String),
    /// `FILTER(?x = ?y)` — two variables
    VarEq(String, String),
    /// `FILTER(isIRI(?x))`
    IsIri(String),
    /// `FILTER(!...)`
    Not(Box<SparqlFilter>),
    /// `FILTER(... && ...)`
    And(Box<SparqlFilter>, Box<SparqlFilter>),
    /// `FILTER(... || ...)`
    Or(Box<SparqlFilter>, Box<SparqlFilter>),
    // ── Phase 2: comparison filters ───────────────────────────────────────────
    /// `FILTER(?var > literal)` or `HAVING(?agg > literal)`
    GreaterThan(String, SparqlLiteral),
    LessThan(String, SparqlLiteral),
    GreaterOrEqual(String, SparqlLiteral),
    LessOrEqual(String, SparqlLiteral),
    /// `FILTER(?var = literal)` — variable against a literal value
    EqualLiteral(String, SparqlLiteral),
    NotEqualLiteral(String, SparqlLiteral),
}

/// The full output of translating one SPARQL query.
#[derive(Debug, Default, Clone)]
pub struct SparqlTranslation {
    /// Each branch represents one disjunct (UNION creates multiple branches).
    pub branches: Vec<Branch>,
    /// Variables to project in output. None = SELECT *.
    pub projection: Option<Vec<String>>,
    pub distinct: bool,
    pub offset: usize,
    pub limit: Option<usize>,
    /// True for ASK queries.
    pub is_ask: bool,
    // ── Phase 2: aggregation ──────────────────────────────────────────────────
    /// GROUP BY variable names.
    pub group_by: Vec<String>,
    /// Aggregate function specifications from the SELECT clause.
    pub aggregates: Vec<SparqlAggregateSpec>,
    /// HAVING filter applied after aggregation.
    pub having_filter: Option<SparqlFilter>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Translate a parsed SPARQL query into a `SparqlTranslation`.
pub fn translate_query(query: &spargebra::Query) -> Result<SparqlTranslation, SparqlError> {
    let mut translation = SparqlTranslation::default();
    let mut counter = 0usize;

    match query {
        spargebra::Query::Select { pattern, .. } => {
            let branches = translate_pattern(pattern, &mut counter, &mut translation)?;
            translation.branches = branches;
        }
        spargebra::Query::Ask { pattern, .. } => {
            let branches = translate_pattern(pattern, &mut counter, &mut translation)?;
            translation.branches = branches;
            translation.is_ask = true;
        }
        spargebra::Query::Construct { .. } | spargebra::Query::Describe { .. } => {
            return Err(SparqlError::Unsupported(
                "CONSTRUCT/DESCRIBE not supported in Phase 1".to_string(),
            ));
        }
    }

    // Ensure there is at least one (possibly empty) branch so callers can
    // iterate without special-casing an empty translation.
    if translation.branches.is_empty() {
        translation.branches.push(Branch::default());
    }

    Ok(translation)
}

// ── Internal algebra walker ───────────────────────────────────────────────────

fn translate_pattern(
    pattern: &GraphPattern,
    counter: &mut usize,
    translation: &mut SparqlTranslation,
) -> Result<Vec<Branch>, SparqlError> {
    match pattern {
        // ── BGP ──────────────────────────────────────────────────────────────
        GraphPattern::Bgp { patterns } => {
            let mut branch = Branch::default();
            for tp in patterns {
                // SPARQL-star: subject is a quoted triple <<s p o>>
                if let TermPattern::Triple(inner_tp) = &tp.subject {
                    let step = translate_sparql_star_subject(inner_tp, &tp.predicate, &tp.object)?;
                    branch.edge_annotation_steps.push(step);
                } else {
                    let vp = translate_triple_pattern(tp)?;
                    branch.patterns.push(vp);
                }
            }
            Ok(vec![branch])
        }

        // ── Property path ─────────────────────────────────────────────────────
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            let subj_term = translate_term_pattern(subject)?;
            let obj_term = translate_term_pattern(object)?;
            let mut branch = Branch::default();
            translate_path(path, subj_term, obj_term, &mut branch, counter)?;
            Ok(vec![branch])
        }

        // ── Join ──────────────────────────────────────────────────────────────
        GraphPattern::Join { left, right } => {
            let left_branches = translate_pattern(left, counter, translation)?;
            let right_branches = translate_pattern(right, counter, translation)?;
            // Cross-product merge
            let mut merged = Vec::new();
            for lb in &left_branches {
                for rb in &right_branches {
                    let mut combined = Branch::default();
                    combined.patterns.extend(lb.patterns.clone());
                    combined.patterns.extend(rb.patterns.clone());
                    combined.rules.extend(lb.rules.clone());
                    combined.rules.extend(rb.rules.clone());
                    combined.filters.extend(lb.filters.clone());
                    combined.filters.extend(rb.filters.clone());
                    combined.edge_annotation_steps.extend(lb.edge_annotation_steps.clone());
                    combined.edge_annotation_steps.extend(rb.edge_annotation_steps.clone());
                    combined.optional_branches.extend(lb.optional_branches.clone());
                    combined.optional_branches.extend(rb.optional_branches.clone());
                    // Named graph: prefer the most specific (non-None) graph_iri
                    combined.graph_iri = lb.graph_iri.clone().or_else(|| rb.graph_iri.clone());
                    merged.push(combined);
                }
            }
            Ok(merged)
        }

        // ── Union ─────────────────────────────────────────────────────────────
        GraphPattern::Union { left, right } => {
            let mut branches = translate_pattern(left, counter, translation)?;
            branches.extend(translate_pattern(right, counter, translation)?);
            Ok(branches)
        }

        // ── Filter ────────────────────────────────────────────────────────────
        GraphPattern::Filter { expr, inner } => {
            // HAVING clause: a Filter that directly wraps a Group
            if matches!(inner.as_ref(), GraphPattern::Group { .. }) {
                let having = translate_filter(expr)?;
                let branches = translate_pattern(inner, counter, translation)?;
                translation.having_filter = Some(having);
                return Ok(branches);
            }
            let filter = translate_filter(expr)?;
            let mut branches = translate_pattern(inner, counter, translation)?;
            for b in &mut branches {
                b.filters.push(filter.clone());
            }
            Ok(branches)
        }

        // ── LeftJoin (OPTIONAL) ───────────────────────────────────────────────
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let left_branches = translate_pattern(left, counter, translation)?;
            let mut right_branches = translate_pattern(right, counter, translation)?;

            // If there is an ON expression (OPTIONAL … FILTER …), attach it
            // as a filter on each right branch so the REST gateway applies it
            // after merging.
            if let Some(expr) = expression {
                if let Ok(f) = translate_filter(expr) {
                    for rb in &mut right_branches {
                        rb.filters.push(f.clone());
                    }
                }
            }

            // Attach right branches as optional_branches on every left branch.
            let mut result = Vec::new();
            for mut lb in left_branches {
                lb.optional_branches.extend(right_branches.clone());
                result.push(lb);
            }
            Ok(result)
        }

        // ── Group (GROUP BY + aggregates) ─────────────────────────────────────
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let branches = translate_pattern(inner, counter, translation)?;
            translation.group_by =
                variables.iter().map(|v| v.as_str().to_string()).collect();
            translation.aggregates = aggregates
                .iter()
                .map(|(alias_var, agg_expr)| translate_aggregate(alias_var, agg_expr))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(branches)
        }

        // ── Named graph ───────────────────────────────────────────────────────
        GraphPattern::Graph { name, inner } => {
            let iri = match name {
                NamedNodePattern::NamedNode(n) => n.as_str().to_string(),
                NamedNodePattern::Variable(v) => {
                    return Err(SparqlError::Unsupported(format!(
                        "variable graph name (?{}) not supported",
                        v.as_str()
                    )))
                }
            };
            let mut branches = translate_pattern(inner, counter, translation)?;
            for b in &mut branches {
                b.graph_iri = Some(iri.clone());
            }
            Ok(branches)
        }

        // ── Project ───────────────────────────────────────────────────────────
        GraphPattern::Project { inner, variables } => {
            let branches = translate_pattern(inner, counter, translation)?;
            translation.projection =
                Some(variables.iter().map(|v| v.as_str().to_string()).collect());
            Ok(branches)
        }

        // ── Distinct ──────────────────────────────────────────────────────────
        GraphPattern::Distinct { inner } => {
            let branches = translate_pattern(inner, counter, translation)?;
            translation.distinct = true;
            Ok(branches)
        }

        // ── Reduced (treated like Distinct for our purposes) ──────────────────
        GraphPattern::Reduced { inner } => translate_pattern(inner, counter, translation),

        // ── Slice (LIMIT/OFFSET) ──────────────────────────────────────────────
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let branches = translate_pattern(inner, counter, translation)?;
            translation.offset = *start;
            translation.limit = *length;
            Ok(branches)
        }

        // ── OrderBy (ignore ordering for Phase 1/2) ───────────────────────────
        GraphPattern::OrderBy { inner, .. } => {
            translate_pattern(inner, counter, translation)
        }

        // ── Extend (BIND / AS alias) ──────────────────────────────────────────
        //
        // spargebra represents `(COUNT(*) AS ?n)` as:
        //   Extend { variable: ?n, expression: Variable("internal_uuid"), inner: Group { ... } }
        //
        // We detect this pattern and rename the auto-generated aggregate alias
        // to the user-visible name.
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let branches = translate_pattern(inner, counter, translation)?;
            // If the expression is a bare Variable reference pointing to an
            // existing aggregate alias, this is an `AS alias` rename.
            if let Expression::Variable(src_var) = expression {
                let src = src_var.as_str();
                let target = variable.as_str().to_string();
                // Rename matching aggregate alias (in-place).
                for spec in &mut translation.aggregates {
                    if spec.alias == src {
                        spec.alias = target.clone();
                        break;
                    }
                }
            }
            Ok(branches)
        }

        // ── Unsupported ───────────────────────────────────────────────────────
        GraphPattern::Service { .. } => Err(SparqlError::Unsupported(
            "SERVICE federation not supported".to_string(),
        )),
        GraphPattern::Minus { .. } => Err(SparqlError::Unsupported(
            "MINUS not supported in Phase 1".to_string(),
        )),
        GraphPattern::Values { .. } => Err(SparqlError::Unsupported(
            "VALUES inline data not supported in Phase 1".to_string(),
        )),
    }
}

// ── Triple pattern translation ────────────────────────────────────────────────

fn translate_triple_pattern(
    tp: &spargebra::term::TriplePattern,
) -> Result<VarPattern, SparqlError> {
    let subj = translate_term_pattern(&tp.subject)?;
    let obj = translate_term_pattern(&tp.object)?;

    let predicate = match &tp.predicate {
        NamedNodePattern::NamedNode(n) => Some(n.as_str().to_string()),
        NamedNodePattern::Variable(_) => None,
    };

    Ok(VarPattern {
        subject: subj,
        predicate,
        object: obj,
        edge_var: None,
        max_hops: None,
        predicate_var: None,
    })
}

fn translate_term_pattern(tp: &TermPattern) -> Result<Term, SparqlError> {
    match tp {
        TermPattern::Variable(v) => Ok(Term::Var(v.as_str().to_string())),
        TermPattern::NamedNode(n) => {
            let iri = n.as_str();
            if let Some(uuid_str) = iri.strip_prefix("urn:uuid:") {
                if let Ok(u) = uuid::Uuid::parse_str(uuid_str) {
                    return Ok(Term::Bound(NodeId(u)));
                }
            }
            // Non-UUID IRIs used as subjects/objects become wildcards.
            Ok(Term::Any)
        }
        TermPattern::BlankNode(b) => Ok(Term::Var(format!("_bn_{}", b.as_str()))),
        TermPattern::Literal(_) => Ok(Term::Any),
        TermPattern::Triple(_) => Err(SparqlError::Unsupported(
            "nested quoted triples (SPARQL-star in object position) not supported".to_string(),
        )),
    }
}

// ── SPARQL-star translation ───────────────────────────────────────────────────

/// Translate a SPARQL-star pattern where the subject is a quoted triple.
///
/// Pattern: `<<edge_subject edge_predicate edge_object>> annot_predicate ?value_var`
fn translate_sparql_star_subject(
    inner: &spargebra::term::TriplePattern,
    outer_predicate: &NamedNodePattern,
    outer_object: &TermPattern,
) -> Result<EdgeAnnotationStep, SparqlError> {
    let edge_subject = translate_term_pattern(&inner.subject)?;
    let edge_predicate = match &inner.predicate {
        NamedNodePattern::NamedNode(n) => n.as_str().to_string(),
        NamedNodePattern::Variable(_) => {
            return Err(SparqlError::Unsupported(
                "variable predicate in quoted triple not supported".to_string(),
            ))
        }
    };
    let edge_object = translate_term_pattern(&inner.object)?;

    let annot_predicate = match outer_predicate {
        NamedNodePattern::NamedNode(n) => n.as_str().to_string(),
        NamedNodePattern::Variable(_) => {
            return Err(SparqlError::Unsupported(
                "variable annotation predicate not supported in SPARQL-star".to_string(),
            ))
        }
    };

    let value_var = match outer_object {
        TermPattern::Variable(v) => v.as_str().to_string(),
        _ => {
            return Err(SparqlError::Unsupported(
                "annotation object must be a variable".to_string(),
            ))
        }
    };

    Ok(EdgeAnnotationStep {
        edge_subject,
        edge_predicate,
        edge_object,
        annot_predicate,
        value_var,
    })
}

// ── Filter expression translation ────────────────────────────────────────────

pub(crate) fn translate_filter(expr: &Expression) -> Result<SparqlFilter, SparqlError> {
    match expr {
        Expression::Bound(v) => Ok(SparqlFilter::Bound(v.as_str().to_string())),

        Expression::Equal(a, b) => {
            // Variable = Variable
            if let (Expression::Variable(va), Expression::Variable(vb)) =
                (a.as_ref(), b.as_ref())
            {
                return Ok(SparqlFilter::VarEq(
                    va.as_str().to_string(),
                    vb.as_str().to_string(),
                ));
            }
            // Variable = Literal
            translate_var_literal_comparison(a, b, SparqlFilter::EqualLiteral)
                .or_else(|_| translate_var_literal_comparison(b, a, SparqlFilter::EqualLiteral))
        }

        Expression::SameTerm(a, b) => {
            // Treat sameTerm like equality for our purposes
            if let (Expression::Variable(va), Expression::Variable(vb)) =
                (a.as_ref(), b.as_ref())
            {
                return Ok(SparqlFilter::VarEq(
                    va.as_str().to_string(),
                    vb.as_str().to_string(),
                ));
            }
            Err(SparqlError::Unsupported(
                "sameTerm with non-variable operands not supported".to_string(),
            ))
        }

        Expression::Greater(a, b) => {
            translate_var_literal_comparison(a, b, SparqlFilter::GreaterThan)
        }
        Expression::GreaterOrEqual(a, b) => {
            translate_var_literal_comparison(a, b, SparqlFilter::GreaterOrEqual)
        }
        Expression::Less(a, b) => {
            translate_var_literal_comparison(a, b, SparqlFilter::LessThan)
        }
        Expression::LessOrEqual(a, b) => {
            translate_var_literal_comparison(a, b, SparqlFilter::LessOrEqual)
        }

        Expression::FunctionCall(func, args) => {
            use spargebra::algebra::Function;
            match func {
                Function::IsIri => {
                    if let Some(Expression::Variable(v)) = args.first() {
                        return Ok(SparqlFilter::IsIri(v.as_str().to_string()));
                    }
                    Err(SparqlError::Unsupported(
                        "isIRI with non-variable argument not supported".to_string(),
                    ))
                }
                _ => Err(SparqlError::Unsupported(format!(
                    "filter function {:?} not supported",
                    func
                ))),
            }
        }

        Expression::Not(inner) => {
            let f = translate_filter(inner)?;
            Ok(SparqlFilter::Not(Box::new(f)))
        }

        Expression::And(a, b) => {
            let fa = translate_filter(a)?;
            let fb = translate_filter(b)?;
            Ok(SparqlFilter::And(Box::new(fa), Box::new(fb)))
        }

        Expression::Or(a, b) => {
            let fa = translate_filter(a)?;
            let fb = translate_filter(b)?;
            Ok(SparqlFilter::Or(Box::new(fa), Box::new(fb)))
        }

        other => Err(SparqlError::Unsupported(format!(
            "filter expression '{}' not supported",
            other
        ))),
    }
}

/// Translate `?var OP literal` into a comparison SparqlFilter.
fn translate_var_literal_comparison(
    var_expr: &Expression,
    lit_expr: &Expression,
    make: fn(String, SparqlLiteral) -> SparqlFilter,
) -> Result<SparqlFilter, SparqlError> {
    let var_name = match var_expr {
        Expression::Variable(v) => v.as_str().to_string(),
        _ => {
            return Err(SparqlError::Unsupported(
                "comparison filter: left side must be a variable".to_string(),
            ))
        }
    };
    let lit = match lit_expr {
        Expression::Literal(l) => sparql_literal_to_value(l),
        _ => {
            return Err(SparqlError::Unsupported(
                "comparison filter: right side must be a literal".to_string(),
            ))
        }
    };
    Ok(make(var_name, lit))
}

fn sparql_literal_to_value(lit: &spargebra::term::Literal) -> SparqlLiteral {
    let dt = lit.datatype().as_str();
    let val = lit.value();
    // Integer types
    if dt.ends_with("#integer")
        || dt.ends_with("#int")
        || dt.ends_with("#long")
        || dt.ends_with("#short")
    {
        if let Ok(n) = val.parse::<i64>() {
            return SparqlLiteral::Int(n);
        }
    }
    // Float/decimal types
    if dt.ends_with("#double") || dt.ends_with("#float") || dt.ends_with("#decimal") {
        if let Ok(f) = val.parse::<f64>() {
            return SparqlLiteral::Float(f);
        }
    }
    // Boolean
    if dt.ends_with("#boolean") {
        if val == "true" {
            return SparqlLiteral::Bool(true);
        }
        if val == "false" {
            return SparqlLiteral::Bool(false);
        }
    }
    // Default: plain string
    SparqlLiteral::Str(val.to_string())
}

// ── Aggregate translation ─────────────────────────────────────────────────────

fn translate_aggregate(
    alias: &spargebra::term::Variable,
    agg_expr: &AggregateExpression,
) -> Result<SparqlAggregateSpec, SparqlError> {
    let alias_str = alias.as_str().to_string();
    let func = match agg_expr {
        AggregateExpression::CountSolutions { .. } => SparqlAggFunc::CountStar,
        AggregateExpression::FunctionCall { name, expr, .. } => {
            let var = extract_var_from_expr(expr)?;
            match name {
                AggregateFunction::Count => SparqlAggFunc::CountVar(var),
                AggregateFunction::Sum => SparqlAggFunc::Sum(var),
                AggregateFunction::Avg => SparqlAggFunc::Avg(var),
                AggregateFunction::Min => SparqlAggFunc::Min(var),
                AggregateFunction::Max => SparqlAggFunc::Max(var),
                AggregateFunction::Sample => SparqlAggFunc::Sample(var),
                AggregateFunction::GroupConcat { separator } => SparqlAggFunc::GroupConcat {
                    var,
                    separator: separator.clone(),
                },
                AggregateFunction::Custom(iri) => {
                    return Err(SparqlError::Unsupported(format!(
                        "custom aggregate function {} not supported",
                        iri.as_str()
                    )))
                }
            }
        }
    };
    Ok(SparqlAggregateSpec { alias: alias_str, func })
}

fn extract_var_from_expr(expr: &Expression) -> Result<String, SparqlError> {
    match expr {
        Expression::Variable(v) => Ok(v.as_str().to_string()),
        other => Err(SparqlError::Unsupported(format!(
            "aggregate expression must be a variable, got: {}",
            other
        ))),
    }
}

// ── Property path translation ─────────────────────────────────────────────────

fn translate_path(
    path: &PropertyPathExpression,
    subject: Term,
    object: Term,
    branch: &mut Branch,
    counter: &mut usize,
) -> Result<(), SparqlError> {
    match path {
        PropertyPathExpression::NamedNode(n) => {
            branch.patterns.push(VarPattern {
                subject,
                predicate: Some(n.as_str().to_string()),
                object,
                edge_var: None,
                max_hops: None,
                predicate_var: None,
            });
            Ok(())
        }

        PropertyPathExpression::Reverse(inner) => {
            translate_path(inner, object, subject, branch, counter)
        }

        PropertyPathExpression::Sequence(a, b) => {
            let mid_var = format!("_seq_{}", *counter);
            *counter += 1;
            let mid_term = Term::Var(mid_var);
            translate_path(a, subject, mid_term.clone(), branch, counter)?;
            translate_path(b, mid_term, object, branch, counter)?;
            Ok(())
        }

        PropertyPathExpression::Alternative(a, _b) => {
            // Phase 1: only the first alternative (full UNION requires separate branches)
            translate_path(a, subject, object, branch, counter)
        }

        PropertyPathExpression::OneOrMore(inner) => {
            translate_one_or_more(inner, subject, object, branch, counter, false)
        }

        PropertyPathExpression::ZeroOrMore(inner) => {
            translate_one_or_more(inner, subject, object, branch, counter, true)
        }

        PropertyPathExpression::ZeroOrOne(inner) => {
            translate_path(inner, subject, object, branch, counter)
        }

        PropertyPathExpression::NegatedPropertySet(_) => Err(SparqlError::Unsupported(
            "negated property set paths not supported".to_string(),
        )),
    }
}

fn translate_one_or_more(
    inner: &PropertyPathExpression,
    subject: Term,
    object: Term,
    branch: &mut Branch,
    counter: &mut usize,
    _zero_or_more: bool,
) -> Result<(), SparqlError> {
    let base_pred = match inner {
        PropertyPathExpression::NamedNode(n) => n.as_str().to_string(),
        other => {
            return Err(SparqlError::Unsupported(format!(
                "complex path inside OneOrMore not supported: {}",
                other
            )))
        }
    };

    let tc_name = format!("_tc{}", *counter);
    *counter += 1;
    let x = format!("_tc_x{}", *counter);
    let y = format!("_tc_y{}", *counter);
    let z = format!("_tc_z{}", *counter);
    *counter += 1;

    branch.rules.push(
        Rule::new(&tc_name, &x, &y).with_body(vec![VarPattern {
            subject: Term::Var(x.clone()),
            predicate: Some(base_pred.clone()),
            object: Term::Var(y.clone()),
            edge_var: None,
            max_hops: None,
            predicate_var: None,
        }]),
    );

    branch.rules.push(
        Rule::new(&tc_name, &x, &z).with_body(vec![
            VarPattern {
                subject: Term::Var(x.clone()),
                predicate: Some(tc_name.clone()),
                object: Term::Var(y.clone()),
                edge_var: None,
                max_hops: None,
                predicate_var: None,
            },
            VarPattern {
                subject: Term::Var(y.clone()),
                predicate: Some(base_pred.clone()),
                object: Term::Var(z.clone()),
                edge_var: None,
                max_hops: None,
                predicate_var: None,
            },
        ]),
    );

    branch.patterns.push(VarPattern {
        subject,
        predicate: Some(tc_name),
        object,
        edge_var: None,
        max_hops: None,
        predicate_var: None,
    });

    Ok(())
}
