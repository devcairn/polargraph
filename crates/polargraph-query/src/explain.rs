//! Static query plan analysis — produces an execution plan without touching storage.
//!
//! `explain_query` walks a `Query`'s patterns in order, tracking which variables
//! become bound after each step, and maps each pattern to the cheapest hexastore
//! column family via the same logic used by the live evaluator. Recursive rules
//! are detected by checking whether a rule's head predicate appears in its own
//! body.

use crate::{
    datalog::{Query, Rule, Term, VarPattern},
    planner::{choose_index, IndexChoice, Pattern},
};
use polargraph_core::id::NodeId;
use std::collections::HashSet;
use uuid::Uuid;

// ── Public plan types ─────────────────────────────────────────────────────────

/// A single step in the static query plan.
#[derive(Debug, Clone)]
pub struct ExplainStep {
    /// Node kind: "PatternScan" for a pattern, "RecursiveJoin" for a rule body.
    pub node_type: String,
    /// Human-readable description of the pattern or rule.
    pub description: String,
    /// Column family chosen for the scan (e.g. "SPO", "POS").
    pub index_used: String,
    /// Variables introduced (bound for the first time) by this step.
    pub binds: Vec<String>,
    /// Child steps (body patterns of a RecursiveJoin).
    pub children: Vec<ExplainStep>,
}

/// The complete static execution plan for a query.
#[derive(Debug)]
pub struct ExplainPlan {
    pub steps: Vec<ExplainStep>,
    pub has_recursive_rules: bool,
    pub plan_text: String,
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Compute the static execution plan for `query` (and any accompanying `rules`).
///
/// No I/O is performed. Binding state is tracked symbolically: after each
/// pattern runs, every variable it introduces is considered "bound" for all
/// subsequent patterns.
pub fn explain_query(query: &Query, rules: &[Rule]) -> ExplainPlan {
    let mut bound_vars: HashSet<String> = HashSet::new();
    let mut steps: Vec<ExplainStep> = Vec::new();

    for vp in &query.patterns {
        let step = analyse_pattern(vp, &mut bound_vars);
        steps.push(step);
    }

    let has_recursive_rules = detect_recursive(rules);

    let plan_text = render_plan_text(&steps, rules, has_recursive_rules);

    ExplainPlan { steps, has_recursive_rules, plan_text }
}

// ── Pattern analysis ──────────────────────────────────────────────────────────

fn analyse_pattern(vp: &VarPattern, bound_vars: &mut HashSet<String>) -> ExplainStep {
    let pattern = static_pattern(vp, bound_vars);
    let choice = choose_index(&pattern);
    let index_used = format!("{}  ({})", index_name(&choice), index_reason(&choice));
    let description = format_var_pattern(vp);

    let binds = new_vars(vp, bound_vars);
    for v in &binds {
        bound_vars.insert(v.clone());
    }

    ExplainStep {
        node_type: "PatternScan".to_string(),
        description,
        index_used,
        binds,
        children: vec![],
    }
}

/// Build a `Pattern` for static index selection from a `VarPattern`.
///
/// Uses a nil-UUID sentinel wherever a variable is already bound; unbound
/// variables and wildcards become `None`. Predicate is passed through as-is
/// (it's always concrete in `VarPattern`).
fn static_pattern(vp: &VarPattern, bound_vars: &HashSet<String>) -> Pattern {
    let sentinel = NodeId(Uuid::nil());
    Pattern {
        subject: match &vp.subject {
            Term::Bound(id) => Some(*id),
            Term::Var(name) if bound_vars.contains(name) => Some(sentinel),
            _ => None,
        },
        predicate: vp.predicate.clone(),
        object: match &vp.object {
            Term::Bound(id) => Some(*id),
            Term::Var(name) if bound_vars.contains(name) => Some(sentinel),
            _ => None,
        },
    }
}

/// Collect variables introduced for the first time by `vp`.
fn new_vars(vp: &VarPattern, bound_vars: &HashSet<String>) -> Vec<String> {
    let mut out = Vec::new();
    for term in [&vp.subject, &vp.object] {
        if let Term::Var(name) = term {
            if !bound_vars.contains(name) && !out.contains(name) {
                out.push(name.clone());
            }
        }
    }
    out
}

// ── Formatting helpers ────────────────────────────────────────────────────────

fn index_name(choice: &IndexChoice) -> String {
    match choice {
        IndexChoice::SpoExact { .. } => "SPO (exact lookup)".to_string(),
        IndexChoice::SpoPrefixSP { .. } => "SPO".to_string(),
        IndexChoice::SpoPrefixS { .. } => "SPO".to_string(),
        IndexChoice::SopPrefixSO { .. } => "SOP".to_string(),
        IndexChoice::PosPrefixPO { .. } => "POS".to_string(),
        IndexChoice::PsoPrefixP { .. } => "PSO".to_string(),
        IndexChoice::OspPrefixO { .. } => "OSP".to_string(),
        IndexChoice::FullScan => "SPO (full scan)".to_string(),
    }
}

fn index_reason(choice: &IndexChoice) -> &'static str {
    match choice {
        IndexChoice::SpoExact { .. } => "all slots bound",
        IndexChoice::SpoPrefixSP { .. } => "subject + predicate bound",
        IndexChoice::SpoPrefixS { .. } => "subject bound",
        IndexChoice::SopPrefixSO { .. } => "subject + object bound",
        IndexChoice::PosPrefixPO { .. } => "predicate + object bound",
        IndexChoice::PsoPrefixP { .. } => "predicate bound",
        IndexChoice::OspPrefixO { .. } => "object bound",
        IndexChoice::FullScan => "no constraints",
    }
}

fn format_var_pattern(vp: &VarPattern) -> String {
    let s = format_term(&vp.subject);
    let p = vp.predicate.as_deref().map(|p| format!(":{p}")).unwrap_or_else(|| "?".to_string());
    let o = format_term(&vp.object);
    format!("[{s}, {p}, {o}]")
}

fn format_term(term: &Term) -> String {
    match term {
        Term::Bound(id) => format!("<{}>", &id.to_string()[..8]),
        Term::Var(name) => format!("?{name}"),
        Term::Any => "_".to_string(),
        Term::Param(name) => format!("${name}"),
    }
}

// ── Recursion detection ───────────────────────────────────────────────────────

fn detect_recursive(rules: &[Rule]) -> bool {
    rules.iter().any(|rule| {
        rule.body.iter().any(|vp| {
            vp.predicate.as_deref() == Some(&rule.head_predicate)
        })
    })
}

// ── Plan text rendering ───────────────────────────────────────────────────────

fn render_plan_text(steps: &[ExplainStep], rules: &[Rule], has_recursive: bool) -> String {
    let mut out = String::new();
    out.push_str("Query Plan\n");
    out.push_str("──────────\n");

    for (i, step) in steps.iter().enumerate() {
        out.push_str(&format!(
            "Step {}: {}  {}\n",
            i + 1,
            step.node_type,
            step.description
        ));
        out.push_str(&format!("  Index: {}\n", step.index_used));
        if !step.binds.is_empty() {
            let vars: Vec<_> = step.binds.iter().map(|v| format!("?{v}")).collect();
            out.push_str(&format!("  Binds: {}\n", vars.join(", ")));
        } else {
            out.push_str("  Binds: (none)\n");
        }
        out.push('\n');
    }

    // Rules section
    if rules.is_empty() {
        out.push_str("Recursive rules: none\n");
    } else {
        let label = if has_recursive { "yes (recursive)" } else { "yes (non-recursive)" };
        out.push_str(&format!("Recursive rules: {label}\n"));
        for rule in rules {
            let recursive = rule.body.iter().any(|vp| {
                vp.predicate.as_deref() == Some(&rule.head_predicate)
            });
            let flag = if recursive { " [recursive]" } else { "" };
            out.push_str(&format!(
                "  {} (?{}, ?{}) :- {} body pattern(s){}\n",
                rule.head_predicate,
                rule.head_subject_var,
                rule.head_object_var,
                rule.body.len(),
                flag
            ));
        }
    }

    out.push_str(&format!("Estimated steps: {}\n", steps.len()));

    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datalog::{Query, Rule, Term, VarPattern};
    use polargraph_core::id::NodeId;
    use uuid::Uuid;

    fn bound_node(seed: u8) -> NodeId {
        NodeId(Uuid::from_bytes([seed; 16]))
    }

    /// Two-pattern query: first pattern has predicate bound → POS;
    /// second pattern shares ?s (bound after step 1) + predicate → SPO.
    #[test]
    fn index_selection_progresses_with_binding_state() {
        let query = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(Term::var("s"))
                    .predicate("knows")
                    .object(Term::var("o")),
            )
            .pattern(
                VarPattern::new()
                    .subject(Term::var("s"))
                    .predicate("name")
                    .object(Term::var("n")),
            );

        let plan = explain_query(&query, &[]);

        // Step 1: only predicate bound → PSO (subject and object are unbound vars)
        assert!(plan.steps[0].index_used.starts_with("PSO"), "step 1 index: {}", plan.steps[0].index_used);
        assert!(plan.steps[0].binds.contains(&"s".to_string()));
        assert!(plan.steps[0].binds.contains(&"o".to_string()));

        // Step 2: subject (?s) now bound from step 1, predicate also bound → SPO
        assert!(plan.steps[1].index_used.starts_with("SPO"), "step 2 index: {}", plan.steps[1].index_used);
        assert!(plan.steps[1].binds.contains(&"n".to_string()));
        assert!(!plan.steps[1].binds.contains(&"s".to_string())); // already bound
    }

    /// A rule whose body references its own head predicate is recursive.
    #[test]
    fn recursive_rule_detected() {
        let rules = vec![
            Rule::new("reachable", "x", "z").with_body(vec![
                VarPattern::new()
                    .subject(Term::var("x"))
                    .predicate("reachable") // head predicate in body → recursive
                    .object(Term::var("y")),
                VarPattern::new()
                    .subject(Term::var("y"))
                    .predicate("edge")
                    .object(Term::var("z")),
            ]),
        ];

        let query = Query::new();
        let plan = explain_query(&query, &rules);

        assert!(plan.has_recursive_rules);
        assert!(plan.plan_text.contains("recursive"));
    }

    /// A rule that doesn't reference its own head is non-recursive.
    #[test]
    fn non_recursive_rule_not_flagged() {
        let rules = vec![
            Rule::new("colleague", "x", "y").with_body(vec![
                VarPattern::new()
                    .subject(Term::var("x"))
                    .predicate("works_at")
                    .object(Term::var("org")),
                VarPattern::new()
                    .subject(Term::var("y"))
                    .predicate("works_at")
                    .object(Term::var("org")),
            ]),
        ];

        let query = Query::new();
        let plan = explain_query(&query, &rules);

        assert!(!plan.has_recursive_rules);
    }

    /// Bound term (constant NodeId) makes subject slot bound → POS when predicate + object are also bound.
    #[test]
    fn bound_term_uses_exact_lookup() {
        let s = bound_node(1);
        let o = bound_node(2);
        let query = Query::new().pattern(
            VarPattern::new()
                .subject(Term::Bound(s))
                .predicate("knows")
                .object(Term::Bound(o)),
        );

        let plan = explain_query(&query, &[]);
        assert!(plan.steps[0].index_used.starts_with("SPO (exact lookup)"), "got: {}", plan.steps[0].index_used);
    }
}
