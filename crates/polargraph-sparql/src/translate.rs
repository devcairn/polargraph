//! Translate a parsed `spargebra::Query` into PolarGraph native query types.

use crate::SparqlError;
use polargraph_core::id::NodeId;
use polargraph_query::{Rule, Term, VarPattern};
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern};

// ── Public output types ───────────────────────────────────────────────────────

/// A single disjunct branch produced by translation.
/// Each branch is a conjunction of patterns evaluated together.
#[derive(Debug, Default, Clone)]
pub struct Branch {
    pub patterns: Vec<VarPattern>,
    pub rules: Vec<Rule>,
    pub filters: Vec<SparqlFilter>,
}

/// A post-filter applied in-process after gRPC query results are received.
#[derive(Debug, Clone)]
pub enum SparqlFilter {
    /// FILTER(BOUND(?x))
    Bound(String),
    /// FILTER(?x = ?y)
    VarEq(String, String),
    /// FILTER(isIRI(?x))
    IsIri(String),
    /// FILTER(!...)
    Not(Box<SparqlFilter>),
    /// FILTER(... && ...)
    And(Box<SparqlFilter>, Box<SparqlFilter>),
    /// FILTER(... || ...)
    Or(Box<SparqlFilter>, Box<SparqlFilter>),
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
                let vp = translate_triple_pattern(tp)?;
                branch.patterns.push(vp);
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
            let filter = translate_filter(expr)?;
            let mut branches = translate_pattern(inner, counter, translation)?;
            for b in &mut branches {
                b.filters.push(filter.clone());
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
        GraphPattern::Reduced { inner } => {
            translate_pattern(inner, counter, translation)
        }

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

        // ── OrderBy (ignore ordering for Phase 1) ─────────────────────────────
        GraphPattern::OrderBy { inner, .. } => {
            translate_pattern(inner, counter, translation)
        }

        // ── LeftJoin (OPTIONAL) ───────────────────────────────────────────────
        GraphPattern::LeftJoin { .. } => Err(SparqlError::Unsupported(
            "OPTIONAL not supported in Phase 1".to_string(),
        )),

        // ── Everything else ───────────────────────────────────────────────────
        GraphPattern::Graph { .. } => Err(SparqlError::Unsupported(
            "GRAPH named graph patterns not supported in Phase 1".to_string(),
        )),
        GraphPattern::Service { .. } => Err(SparqlError::Unsupported(
            "SERVICE federation not supported in Phase 1".to_string(),
        )),
        GraphPattern::Group { .. } => Err(SparqlError::Unsupported(
            "GROUP BY / aggregates not supported in Phase 1".to_string(),
        )),
        GraphPattern::Extend { inner, .. } => {
            // BIND expressions — ignore the binding but translate the inner pattern
            translate_pattern(inner, counter, translation)
        }
        GraphPattern::Minus { .. } => {
            // MINUS — not supported in Phase 1
            Err(SparqlError::Unsupported(
                "MINUS not supported in Phase 1".to_string(),
            ))
        }
        GraphPattern::Values { .. } => {
            // Inline VALUES — not supported yet
            Err(SparqlError::Unsupported(
                "VALUES inline data not supported in Phase 1".to_string(),
            ))
        }
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
        NamedNodePattern::Variable(_) => {
            // Variable predicates are not bindable in Phase 1 — treat as wildcard
            None
        }
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
            // Try to parse as urn:uuid:<uuid>
            let iri = n.as_str();
            if let Some(uuid_str) = iri.strip_prefix("urn:uuid:") {
                if let Ok(u) = uuid::Uuid::parse_str(uuid_str) {
                    let id = NodeId(u);
                    return Ok(Term::Bound(id));
                }
            }
            // For non-UUID IRIs used as subjects/objects we cannot bind to a NodeId
            // without a storage lookup. Use wildcard (best-effort for Phase 1).
            Ok(Term::Any)
        }
        TermPattern::BlankNode(b) => {
            // Blank nodes become fresh variables
            Ok(Term::Var(format!("_bn_{}", b.as_str())))
        }
        TermPattern::Literal(_) => {
            // Literal objects are not bindable in the current system
            Ok(Term::Any)
        }
    }
}

// ── Filter expression translation ────────────────────────────────────────────

fn translate_filter(expr: &Expression) -> Result<SparqlFilter, SparqlError> {
    match expr {
        Expression::Bound(v) => Ok(SparqlFilter::Bound(v.as_str().to_string())),

        Expression::Equal(a, b) => {
            if let (Expression::Variable(va), Expression::Variable(vb)) = (a.as_ref(), b.as_ref()) {
                return Ok(SparqlFilter::VarEq(
                    va.as_str().to_string(),
                    vb.as_str().to_string(),
                ));
            }
            Err(SparqlError::Unsupported(
                "filter expression: equality with non-variable operands not supported in Phase 1".to_string()
            ))
        }

        Expression::FunctionCall(func, args) => {
            use spargebra::algebra::Function;
            match func {
                Function::IsIri => {
                    if let Some(Expression::Variable(v)) = args.first() {
                        return Ok(SparqlFilter::IsIri(v.as_str().to_string()));
                    }
                    Err(SparqlError::Unsupported(
                        "filter expression: isIRI with non-variable argument not supported in Phase 1".to_string(),
                    ))
                }
                _ => Err(SparqlError::Unsupported(format!(
                    "filter expression: function {:?} not supported in Phase 1",
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
            "filter expression '{}' not supported in Phase 1",
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
            // Swap subject and object
            translate_path(inner, object, subject, branch, counter)
        }

        PropertyPathExpression::Sequence(a, b) => {
            // Generate an intermediate variable
            let mid_var = format!("_seq_{}", *counter);
            *counter += 1;
            let mid_term = Term::Var(mid_var);
            translate_path(a, subject, mid_term.clone(), branch, counter)?;
            translate_path(b, mid_term, object, branch, counter)?;
            Ok(())
        }

        PropertyPathExpression::Alternative(a, _b) => {
            // UNION semantics require multiple branches, but translate_path
            // operates on a single &mut Branch. For Phase 1, translate only
            // the first alternative. The second is silently dropped.
            translate_path(a, subject, object, branch, counter)
        }

        PropertyPathExpression::OneOrMore(inner) => {
            translate_one_or_more(inner, subject, object, branch, counter, false)
        }

        PropertyPathExpression::ZeroOrMore(inner) => {
            translate_one_or_more(inner, subject, object, branch, counter, true)
        }

        PropertyPathExpression::ZeroOrOne(inner) => {
            // For Phase 1, treat as a single step (the "one" case)
            translate_path(inner, subject, object, branch, counter)
        }

        PropertyPathExpression::NegatedPropertySet(_) => {
            Err(SparqlError::Unsupported(
                "negated property set paths not supported in Phase 1".to_string(),
            ))
        }
    }
}

/// Generate transitive closure rules for `OneOrMore` (`+`) and `ZeroOrMore` (`*`) paths.
fn translate_one_or_more(
    inner: &PropertyPathExpression,
    subject: Term,
    object: Term,
    branch: &mut Branch,
    counter: &mut usize,
    _zero_or_more: bool,
) -> Result<(), SparqlError> {
    // Extract the base predicate for simple NamedNode paths
    let base_pred = match inner {
        PropertyPathExpression::NamedNode(n) => n.as_str().to_string(),
        other => {
            // For complex sub-paths, recursively translate into a helper predicate
            // For Phase 1, only handle simple named-node base predicates
            return Err(SparqlError::Unsupported(format!(
                "complex property path inside OneOrMore not supported in Phase 1: {}",
                other
            )));
        }
    };

    let tc_name = format!("_tc{}", *counter);
    *counter += 1;

    // Variable names for the TC rules (unique to avoid collisions)
    let x = format!("_tc_x{}", *counter);
    let y = format!("_tc_y{}", *counter);
    let z = format!("_tc_z{}", *counter);
    *counter += 1;

    // Rule 1: tc(x, y) :- base(x, y)  [base case]
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

    // Rule 2: tc(x, z) :- tc(x, y), base(y, z)  [recursive case]
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

    // The pattern to use the TC predicate with the original subject/object terms
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
