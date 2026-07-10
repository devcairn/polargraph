//! Conjunctive query evaluator — multi-pattern joins over a snapshot.
//!
//! # Model
//!
//! A `Query` is an ordered list of `VarPattern`s. Each pattern is like a
//! triple pattern but slots can be named **variables** that get bound
//! progressively as patterns are evaluated left-to-right.
//!
//! ```text
//! // "Who are Alice's colleagues?" (share the same manager)
//! Query::new()
//!     .pattern(var("alice").reports_to(Var("mgr")))
//!     .pattern(Var("colleague").reports_to(Var("mgr")))
//! ```
//!
//! Variables are strings; a variable that appears in more than one pattern
//! acts as a join key. The result is a `Vec<Bindings>` — every complete
//! assignment of variable names to `NodeId`s that satisfies all patterns.
//!
//! # Join strategy
//!
//! Naive nested-loop: evaluate each pattern in order, substituting already-
//! bound variables before calling the storage layer. This is correct for any
//! query size and fast when early patterns are selective (which they usually
//! are with a bound subject or predicate). A cost-based join reorderer is a
//! future optimization.
//!
//! # Recursive Datalog
//!
//! `execute_recursive` runs a set of rules to a fixed point via semi-naïve
//! bottom-up evaluation. Derived facts live in memory (`DerivedFacts`) and are
//! never written back to storage. Cycles terminate naturally because the
//! fixed-point loop only continues while new facts are produced.
//!
//! `reachable_from` is a convenience wrapper for the most common graph use
//! case: find every node reachable transitively from a given start node.
//!
//! # Limitations (current phase)
//!
//! - Object variables only bind to `NodeId`s (relation triples). Property
//!   triples match subject variables but their scalar value is not bindable.
//! - No negation, no aggregation.

use crate::{
    eval::{evaluate, evaluate_with_overlay, evaluate_with_registry},
    planner::Pattern,
};
use polargraph_core::{
    id::{EdgeId, NodeId},
    temporal::{BiTemporalRange, Timestamp},
    triple::{Predicate, Triple},
};
use polargraph_storage::{EdgeTypeRegistry, Snapshot, StorageError};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("query timed out")]
    Timeout,
}

// ── Types ─────────────────────────────────────────────────────────────────────

/// A slot in a VarPattern — either a concrete value, a named variable, or a
/// wildcard that matches anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Term {
    /// Concrete, already-known node ID.
    Bound(NodeId),
    /// Named variable. Gets bound on first match; subsequent occurrences act
    /// as equality constraints.
    Var(String),
    /// Wildcard — matches anything, binds nothing.
    Any,
    /// A named query parameter (e.g. `$name`) that should be substituted before
    /// evaluation. Treated like `Any` at evaluation time — the actual substitution
    /// is handled at the Cypher layer via `CompiledQuery::substitute_params`.
    Param(String),
}

impl Term {
    pub fn var(name: impl Into<String>) -> Self {
        Term::Var(name.into())
    }
}

/// A triple pattern where subject and object slots can be variables.
///
/// `predicate` is `Option<String>`: `Some("knows")` matches that specific
/// predicate; `None` matches any predicate.
///
/// `predicate_var` names a variable that receives the matched predicate string
/// (for SPARQL-style `?s ?p ?o` BGPs and OWL 2 RL rules that bind predicates).
/// When set alongside a `None` predicate, the evaluator enumerates predicates
/// via a PSO or SPO scan and binds each one into [`PredBindings`].
/// When a predicate variable was bound in an earlier pattern and appears again
/// here with `predicate = None`, `substitute_with_preds` fills in the concrete
/// predicate before dispatching to storage.
#[derive(Debug, Clone)]
pub struct VarPattern {
    pub subject: Term,
    pub predicate: Option<String>,
    /// Names the variable that receives the matched predicate string.
    pub predicate_var: Option<String>,
    pub object: Term,
    /// When set, the EdgeId from a matched Relation triple is bound to this variable
    /// as a `NodeId` (same UUID bytes, reinterpreted).
    pub edge_var: Option<String>,
    /// When `Some(n)`, this pattern performs a bounded BFS up to `n` hops rather
    /// than a single-step triple scan. Used by the Cypher compiler for `[r*1..n]`
    /// patterns. Requires the subject to be bound before evaluation.
    pub max_hops: Option<usize>,
}

impl VarPattern {
    pub fn new() -> Self {
        Self {
            subject: Term::Any,
            predicate: None,
            predicate_var: None,
            object: Term::Any,
            edge_var: None,
            max_hops: None,
        }
    }

    pub fn subject(mut self, s: Term) -> Self {
        self.subject = s;
        self
    }
    pub fn predicate(mut self, p: impl Into<String>) -> Self {
        self.predicate = Some(p.into());
        self
    }
    pub fn predicate_var(mut self, v: impl Into<String>) -> Self {
        self.predicate_var = Some(v.into());
        self
    }
    pub fn object(mut self, o: Term) -> Self {
        self.object = o;
        self
    }
    pub fn with_edge_var(mut self, v: impl Into<String>) -> Self {
        self.edge_var = Some(v.into());
        self
    }
}

impl Default for VarPattern {
    fn default() -> Self {
        Self::new()
    }
}

/// A map from node variable name to its bound `NodeId`.
pub type Bindings = HashMap<String, NodeId>;

/// A map from predicate variable name to its bound predicate string.
///
/// Predicate bindings run alongside [`Bindings`] through the evaluation loop so
/// that a predicate variable bound in one pattern can be substituted as a
/// concrete predicate in a later pattern (OWL 2 RL / SPARQL BGP use case).
pub type PredBindings = HashMap<String, String>;

/// A conjunctive query: an ordered list of patterns, all of which must be
/// satisfied simultaneously (logical AND / natural join).
#[derive(Debug, Clone, Default)]
pub struct Query {
    pub patterns: Vec<VarPattern>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn pattern(mut self, p: VarPattern) -> Self {
        self.patterns.push(p);
        self
    }
}

// ── Core helpers ──────────────────────────────────────────────────────────────

/// Substitute already-bound variables in `vp` to produce a concrete `Pattern`
/// the storage evaluator can execute.
///
/// Unbound variables become `None` (wildcard) — the storage layer returns all
/// candidates; `extend_full` then filters and binds them.
///
/// When `predicate_var` names a variable already present in `pred_bindings`,
/// the bound predicate string is used as the concrete predicate, enabling
/// predicate variables to act as predicates in subsequent patterns.
fn substitute_with_preds(
    vp: &VarPattern,
    bindings: &Bindings,
    pred_bindings: &PredBindings,
) -> Pattern {
    Pattern {
        subject: resolve_term(&vp.subject, bindings),
        predicate: vp.predicate.clone().or_else(|| {
            vp.predicate_var
                .as_ref()
                .and_then(|v| pred_bindings.get(v).cloned())
        }),
        object: resolve_term(&vp.object, bindings),
    }
}

fn resolve_term(term: &Term, bindings: &Bindings) -> Option<NodeId> {
    match term {
        Term::Bound(id) => Some(*id),
        Term::Var(name) => bindings.get(name).copied(),
        Term::Any => None,
        // Params should be substituted before evaluation; treat as wildcard.
        Term::Param(_) => None,
    }
}

/// Try to extend `bindings` and `pred_bindings` with the variable assignments
/// implied by matching `triple` against `vp`.
///
/// Returns `None` if the triple conflicts with an already-bound variable.
///
/// Predicate variable binding: when `vp.predicate_var` is `Some(name)`, the
/// matched triple's predicate string is bound to `name` in `PredBindings`.
/// Conflicts (same variable already bound to a different predicate) discard
/// this solution.
fn extend_full(
    triple: &Triple,
    vp: &VarPattern,
    bindings: &Bindings,
    pred_bindings: &PredBindings,
) -> Option<(Bindings, PredBindings)> {
    let mut node_out = bindings.clone();
    let mut pred_out = pred_bindings.clone();

    // Bind / check subject slot.
    if let Some(updated) = bind_term(&vp.subject, triple.subject(), &node_out) {
        node_out = updated;
    } else {
        return None;
    }

    // Bind / check object slot — only Relation triples have a NodeId object.
    match &vp.object {
        Term::Any | Term::Param(_) => {} // nothing to bind
        Term::Bound(_) => {}             // substitution already handled this
        Term::Var(_) => {
            match triple {
                Triple::Relation { object, .. } => {
                    if let Some(updated) = bind_term(&vp.object, *object, &node_out) {
                        node_out = updated;
                    } else {
                        return None;
                    }
                }
                Triple::Property { .. }
                | Triple::EdgeProperty { .. }
                | Triple::EdgeRelation { .. } => {
                    // Object variable can't bind to a scalar/annotation value — skip.
                    return None;
                }
            }
        }
    }

    // Bind edge variable if requested (only applicable to Relation triples).
    if let (Some(ev), Triple::Relation { edge_id, .. }) = (&vp.edge_var, triple) {
        let as_node = NodeId(edge_id.0);
        if let Some(updated) = bind_term(&Term::Var(ev.clone()), as_node, &node_out) {
            node_out = updated;
        } else {
            return None;
        }
    }

    // Bind predicate variable.
    if let Some(pv) = &vp.predicate_var {
        let pred_str = triple.predicate().0.clone();
        if let Some(existing) = pred_out.get(pv) {
            if *existing != pred_str {
                return None; // predicate variable conflict
            }
        } else {
            pred_out.insert(pv.clone(), pred_str);
        }
    }

    Some((node_out, pred_out))
}

/// Attempt to bind `term` to `value` within `bindings`.
///
/// - `Bound`: already substituted; returns unchanged bindings (storage
///   already filtered on the value).
/// - `Var(name)`: if already bound, check for equality; if unbound, add it.
/// - `Any`: nothing to do.
///
/// Returns `None` on conflict.
fn bind_term(term: &Term, value: NodeId, bindings: &Bindings) -> Option<Bindings> {
    match term {
        Term::Any | Term::Bound(_) | Term::Param(_) => Some(bindings.clone()),
        Term::Var(name) => {
            if let Some(&existing) = bindings.get(name) {
                if existing == value {
                    Some(bindings.clone())
                } else {
                    None // conflict
                }
            } else {
                let mut out = bindings.clone();
                out.insert(name.clone(), value);
                Some(out)
            }
        }
    }
}

// ── Evaluator ─────────────────────────────────────────────────────────────────

/// Inner evaluation loop that tracks `(Bindings, PredBindings)` pairs through
/// every pattern so predicate variables are threaded correctly across joins.
fn evaluate_seeded_inner(
    query: &Query,
    snapshot: &Snapshot,
    initial: Vec<(Bindings, PredBindings)>,
    deadline: Option<Instant>,
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<(Bindings, PredBindings)>, QueryError> {
    if initial.is_empty() {
        return Ok(vec![]);
    }

    let mut solutions = initial;

    for vp in &query.patterns {
        if deadline.map(|d| Instant::now() > d).unwrap_or(false) {
            return Err(QueryError::Timeout);
        }

        let mut next = Vec::new();

        if let Some(hops) = vp.max_hops {
            // Bounded transitive: BFS up to `hops` steps. Predicate must be fixed.
            if let Some(pred) = &vp.predicate {
                for (bindings, pred_bindings) in solutions {
                    let start = resolve_term(&vp.subject, &bindings);

                    // Collect starting nodes: use the bound subject directly, or
                    // enumerate all subjects with outgoing `pred` edges when unbound.
                    let starts: Vec<NodeId> = if let Some(id) = start {
                        vec![id]
                    } else {
                        let all = snapshot.scan_by_predicate(pred)?;
                        let mut seen = HashSet::new();
                        all.into_iter()
                            .filter_map(|t| match t {
                                Triple::Relation { subject, .. } if seen.insert(subject) => {
                                    Some(subject)
                                }
                                _ => None,
                            })
                            .collect()
                    };

                    for start_id in starts {
                        // Bind the subject variable (if any) to the starting node.
                        let mut base = bindings.clone();
                        if let Term::Var(sv) = &vp.subject {
                            if let Some(&existing) = base.get(sv.as_str()) {
                                if existing != start_id {
                                    continue;
                                }
                            } else {
                                base.insert(sv.clone(), start_id);
                            }
                        }

                        let reachable =
                            reachable_from_hops(start_id, pred, snapshot, hops, deadline)?;
                        for target in reachable {
                            match &vp.object {
                                Term::Any | Term::Param(_) => {
                                    next.push((base.clone(), pred_bindings.clone()))
                                }
                                Term::Bound(id) => {
                                    if *id == target {
                                        next.push((base.clone(), pred_bindings.clone()));
                                    }
                                }
                                Term::Var(name) => {
                                    if let Some(&existing) = base.get(name.as_str()) {
                                        if existing == target {
                                            next.push((base.clone(), pred_bindings.clone()));
                                        }
                                    } else {
                                        let mut b = base.clone();
                                        b.insert(name.clone(), target);
                                        next.push((b, pred_bindings.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            for (bindings, pred_bindings) in solutions {
                let pattern = substitute_with_preds(vp, &bindings, &pred_bindings);
                let triples = evaluate_with_registry(&pattern, snapshot, registry)?;

                for triple in triples {
                    if let Some(pair) = extend_full(&triple, vp, &bindings, &pred_bindings) {
                        next.push(pair);
                    }
                }
            }
        }

        solutions = next;

        if solutions.is_empty() {
            return Ok(vec![]);
        }
    }

    Ok(solutions)
}

/// Evaluate a conjunctive query against a snapshot, starting from a given set
/// of initial bindings instead of a single empty binding.
///
/// Each element of `initial` is an independent partial solution. Patterns are
/// evaluated in order, joining against every element; only solutions that
/// satisfy all patterns survive. An empty `initial` returns immediately with
/// an empty result. An empty `query.patterns` returns `initial` unchanged.
///
/// When `registry` is `Some`, each pattern evaluation consults the
/// `EdgeTypeRegistry` and short-circuits when the bound subject or object does
/// not satisfy the predicate's domain/range constraint. Pass `None` to disable
/// schema-aware pruning and preserve the existing behaviour.
pub fn execute_query_seeded(
    query: &Query,
    snapshot: &Snapshot,
    initial: Vec<Bindings>,
    deadline: Option<Instant>,
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<Bindings>, QueryError> {
    let seeds = initial
        .into_iter()
        .map(|b| (b, PredBindings::new()))
        .collect();
    Ok(
        evaluate_seeded_inner(query, snapshot, seeds, deadline, registry)?
            .into_iter()
            .map(|(b, _pb)| b)
            .collect(),
    )
}

/// Like [`execute_query_seeded`] but also returns predicate variable bindings.
///
/// Use this when the query contains patterns with `predicate_var` set and the
/// caller needs to inspect which predicate string was bound (e.g. for OWL 2 RL
/// rule application or SPARQL-style `?s ?p ?o` projection).
pub fn execute_query_seeded_full(
    query: &Query,
    snapshot: &Snapshot,
    initial: Vec<Bindings>,
    deadline: Option<Instant>,
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<(Bindings, PredBindings)>, QueryError> {
    let seeds = initial
        .into_iter()
        .map(|b| (b, PredBindings::new()))
        .collect();
    evaluate_seeded_inner(query, snapshot, seeds, deadline, registry)
}

/// Evaluate a conjunctive query against a snapshot.
///
/// Returns all binding sets that satisfy every pattern in the query.
/// An empty query returns a single empty binding (vacuously satisfied).
/// A query with unsatisfiable patterns returns an empty vec.
///
/// Pass `registry: Some(reg)` to enable schema-aware pruning (see
/// [`execute_query_seeded`]). Pass `None` to preserve the existing behaviour.
pub fn execute_query(
    query: &Query,
    snapshot: &Snapshot,
    deadline: Option<Instant>,
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<Bindings>, QueryError> {
    execute_query_seeded(query, snapshot, vec![HashMap::new()], deadline, registry)
}

/// Like [`execute_query`] but also returns predicate variable bindings.
///
/// Each result is a `(Bindings, PredBindings)` pair. `PredBindings` maps
/// predicate variable names to the predicate strings that were bound during
/// evaluation — see [`VarPattern::predicate_var`].
pub fn execute_query_full(
    query: &Query,
    snapshot: &Snapshot,
    deadline: Option<Instant>,
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<(Bindings, PredBindings)>, QueryError> {
    execute_query_seeded_full(query, snapshot, vec![HashMap::new()], deadline, registry)
}

/// Like [`execute_query`] but overlays `pending` (uncommitted write-buffer
/// triples from an open transaction) on top of every storage scan.
///
/// This implements write-your-own-reads: a query that shares a `tx_id` with
/// prior `Insert` calls will see those uncommitted triples in its results
/// alongside the committed storage state at the transaction's `read_ts`.
pub fn execute_query_with_pending(
    query: &Query,
    snapshot: &Snapshot,
    pending: &[Triple],
    deadline: Option<Instant>,
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<Bindings>, QueryError> {
    Ok(
        execute_query_with_pending_full(query, snapshot, pending, deadline, registry)?
            .into_iter()
            .map(|(b, _pb)| b)
            .collect(),
    )
}

/// Like [`execute_query_with_pending`] but also returns predicate variable
/// bindings — see [`execute_query_full`].
pub fn execute_query_with_pending_full(
    query: &Query,
    snapshot: &Snapshot,
    pending: &[Triple],
    deadline: Option<Instant>,
    registry: Option<&EdgeTypeRegistry>,
) -> Result<Vec<(Bindings, PredBindings)>, QueryError> {
    let mut solutions: Vec<(Bindings, PredBindings)> = vec![(HashMap::new(), PredBindings::new())];

    for vp in &query.patterns {
        if deadline.map(|d| Instant::now() > d).unwrap_or(false) {
            return Err(QueryError::Timeout);
        }

        let mut next = Vec::new();

        for (bindings, pred_bindings) in solutions {
            let pattern = substitute_with_preds(vp, &bindings, &pred_bindings);
            let triples = evaluate_with_overlay(&pattern, snapshot, pending, registry)?;

            for triple in triples {
                if let Some(pair) = extend_full(&triple, vp, &bindings, &pred_bindings) {
                    next.push(pair);
                }
            }
        }

        solutions = next;

        if solutions.is_empty() {
            return Ok(vec![]);
        }
    }

    Ok(solutions)
}

// ── Recursive Datalog ─────────────────────────────────────────────────────────

/// In-memory derived facts: `predicate → set of (subject, object)` pairs.
///
/// Derived facts represent relations inferred by recursive rules. They are
/// never written back to the triple store — they exist only for the duration
/// of a `execute_recursive` call.
pub type DerivedFacts = HashMap<String, HashSet<(NodeId, NodeId)>>;

/// A Datalog rule with a derived-relation head.
///
/// ```text
/// // Transitive closure: reachable(X, Z) :- reachable(X, Y), edge(Y, Z).
/// Rule::new("reachable", "x", "z")
///     .with_body(vec![
///         VarPattern::new().subject(Term::var("x")).predicate("reachable").object(Term::var("y")),
///         VarPattern::new().subject(Term::var("y")).predicate("edge").object(Term::var("z")),
///     ])
/// ```
///
/// The head's subject and object slots are named variables that must appear
/// in the body. The body may reference the same derived predicate recursively;
/// those patterns are evaluated against `DerivedFacts`, not storage.
#[derive(Debug, Clone)]
pub struct Rule {
    /// Name of the derived predicate produced by this rule's head.
    pub head_predicate: String,
    /// Variable in the body that maps to the head's subject slot.
    pub head_subject_var: String,
    /// Variable in the body that maps to the head's object slot.
    pub head_object_var: String,
    /// Conjunctive body — all patterns must be satisfied simultaneously.
    pub body: Vec<VarPattern>,
}

impl Rule {
    /// Create a rule whose head is `head_predicate(head_subject_var, head_object_var)`.
    pub fn new(
        head_predicate: impl Into<String>,
        head_subject_var: impl Into<String>,
        head_object_var: impl Into<String>,
    ) -> Self {
        Self {
            head_predicate: head_predicate.into(),
            head_subject_var: head_subject_var.into(),
            head_object_var: head_object_var.into(),
            body: Vec::new(),
        }
    }

    /// Set the body patterns for this rule.
    pub fn with_body(mut self, body: Vec<VarPattern>) -> Self {
        self.body = body;
        self
    }
}

/// Execute a set of recursive Datalog rules until a fixed point is reached.
///
/// # Arguments
/// * `seed` – Initial (predicate, subject, object) tuples that prime the
///   derived relations before the first rule application.
/// * `rules` – Rules applied iteratively. A rule's body may reference any
///   predicate, including a derived one (for recursion).
/// * `snapshot` – Storage snapshot for base (EDB) fact lookups.
///
/// # Returns
/// The complete `DerivedFacts` map after no new facts can be derived.
///
/// # Cycle safety
/// The fixed-point loop terminates even when rules create cycles, because it
/// only continues while at least one *new* fact is added per iteration.
///
/// # Example — transitive closure
/// ```rust,ignore
/// // Build seed: alice→bob, alice→carol (direct "knows" edges)
/// let seed = vec![
///     ("tc".into(), alice, bob),
///     ("tc".into(), alice, carol),
/// ];
/// // Rule: tc(X, Z) :- tc(X, Y), knows(Y, Z)
/// let rules = vec![
///     Rule::new("tc", "x", "z").with_body(vec![
///         VarPattern::new().subject(Term::var("x")).predicate("tc").object(Term::var("y")),
///         VarPattern::new().subject(Term::var("y")).predicate("knows").object(Term::var("z")),
///     ])
/// ];
/// let derived = execute_recursive(&seed, &rules, &snapshot)?;
/// // derived["tc"] contains every (src, dst) reachable via "knows" from alice.
/// ```
pub fn execute_recursive(
    seed: &[(String, NodeId, NodeId)],
    rules: &[Rule],
    snapshot: &Snapshot,
    deadline: Option<Instant>,
) -> Result<DerivedFacts, QueryError> {
    // Initialise derived facts from the seed.
    let mut derived: DerivedFacts = HashMap::new();
    for (pred, s, o) in seed {
        derived.entry(pred.clone()).or_default().insert((*s, *o));
    }

    // Fixed-point iteration — keep going as long as any new fact is added.
    loop {
        if deadline.map(|d| Instant::now() > d).unwrap_or(false) {
            return Err(QueryError::Timeout);
        }

        let mut added_any = false;

        for rule in rules {
            // Evaluate the rule body conjunctively, consulting derived facts
            // for derived predicates and storage for base predicates.
            let body_query = Query {
                patterns: rule.body.clone(),
            };
            let solutions = execute_query_hybrid(&body_query, snapshot, &derived, deadline)?;

            // Extract head variable bindings to produce new derived facts.
            let entry = derived.entry(rule.head_predicate.clone()).or_default();
            for bindings in solutions {
                let s = bindings.get(&rule.head_subject_var).copied();
                let o = bindings.get(&rule.head_object_var).copied();
                if let (Some(s), Some(o)) = (s, o) {
                    if entry.insert((s, o)) {
                        added_any = true;
                    }
                }
            }
        }

        if !added_any {
            break;
        }
    }

    Ok(derived)
}

/// Compute the set of all `NodeId`s reachable from `start` by following
/// `predicate` edges transitively (one or more hops).
///
/// This is the most common recursive graph query. Internally it seeds the
/// `"reachable"` derived relation with direct neighbours and applies the
/// transitive-closure rule until a fixed point.
///
/// Returns an empty set when `start` has no outgoing `predicate` edges.
/// Self-loops are included (a node that has a `predicate` edge to itself
/// appears in the result set).
/// BFS-limited transitive closure: nodes reachable from `start` in at most
/// `max_hops` steps along `predicate` edges.
///
/// `max_hops = 0` returns an empty set (zero steps means no movement).
/// For unlimited depth use [`reachable_from`].
pub fn reachable_from_hops(
    start: NodeId,
    predicate: &str,
    snapshot: &Snapshot,
    max_hops: usize,
    deadline: Option<Instant>,
) -> Result<HashSet<NodeId>, QueryError> {
    if max_hops == 0 {
        return Ok(HashSet::new());
    }

    let mut visited: HashSet<NodeId> = HashSet::new();
    let mut frontier: Vec<NodeId> = vec![start];

    for _ in 0..max_hops {
        if deadline.map(|d| Instant::now() > d).unwrap_or(false) {
            return Err(QueryError::Timeout);
        }

        let mut next: Vec<NodeId> = Vec::new();
        for node in &frontier {
            let triples = snapshot.scan_by_subject_predicate(node, predicate)?;
            for triple in triples {
                if let Triple::Relation { object, .. } = triple {
                    if visited.insert(object) {
                        next.push(object);
                    }
                }
            }
        }
        frontier = next;
        if frontier.is_empty() {
            break;
        }
    }

    Ok(visited)
}

pub fn reachable_from(
    start: NodeId,
    predicate: &str,
    snapshot: &Snapshot,
    deadline: Option<Instant>,
) -> Result<HashSet<NodeId>, QueryError> {
    // Seed: direct neighbours of start via the given predicate.
    let direct = evaluate(
        &Pattern::new().with_subject(start).with_predicate(predicate),
        snapshot,
    )?;

    let seed: Vec<(String, NodeId, NodeId)> = direct
        .iter()
        .filter_map(|t| match t {
            Triple::Relation {
                subject, object, ..
            } => Some(("reachable".into(), *subject, *object)),
            Triple::Property { .. } | Triple::EdgeProperty { .. } | Triple::EdgeRelation { .. } => {
                None
            }
        })
        .collect();

    if seed.is_empty() {
        return Ok(HashSet::new());
    }

    // Rule: reachable(X, Z) :- reachable(X, Y), predicate(Y, Z)
    let rules = vec![Rule::new("reachable", "x", "z").with_body(vec![
        VarPattern::new()
            .subject(Term::var("x"))
            .predicate("reachable")
            .object(Term::var("y")),
        VarPattern::new()
            .subject(Term::var("y"))
            .predicate(predicate)
            .object(Term::var("z")),
    ])];

    let derived = execute_recursive(&seed, &rules, snapshot, deadline)?;

    // Collect all objects reachable from `start` (subject == start).
    let reachable = derived
        .get("reachable")
        .map(|pairs| {
            pairs
                .iter()
                .filter(|(s, _)| *s == start)
                .map(|(_, o)| *o)
                .collect()
        })
        .unwrap_or_default();

    Ok(reachable)
}

// ── Hybrid evaluator (base + derived) ─────────────────────────────────────────

/// Run a conjunctive query where patterns whose predicate is a key in `derived`
/// are evaluated against the in-memory derived set rather than storage.
pub fn execute_query_hybrid(
    query: &Query,
    snapshot: &Snapshot,
    derived: &DerivedFacts,
    deadline: Option<Instant>,
) -> Result<Vec<Bindings>, QueryError> {
    Ok(
        execute_query_hybrid_full(query, snapshot, derived, deadline)?
            .into_iter()
            .map(|(b, _pb)| b)
            .collect(),
    )
}

/// Like [`execute_query_hybrid`] but also returns predicate variable bindings
/// — see [`execute_query_full`].
pub fn execute_query_hybrid_full(
    query: &Query,
    snapshot: &Snapshot,
    derived: &DerivedFacts,
    deadline: Option<Instant>,
) -> Result<Vec<(Bindings, PredBindings)>, QueryError> {
    let mut solutions: Vec<(Bindings, PredBindings)> = vec![(HashMap::new(), PredBindings::new())];

    for vp in &query.patterns {
        if deadline.map(|d| Instant::now() > d).unwrap_or(false) {
            return Err(QueryError::Timeout);
        }

        let mut next = Vec::new();

        for (bindings, pred_bindings) in solutions {
            let pattern = substitute_with_preds(vp, &bindings, &pred_bindings);
            let triples = evaluate_hybrid(&pattern, snapshot, derived)?;

            for triple in triples {
                if let Some(pair) = extend_full(&triple, vp, &bindings, &pred_bindings) {
                    next.push(pair);
                }
            }
        }

        solutions = next;
        if solutions.is_empty() {
            return Ok(vec![]);
        }
    }

    Ok(solutions)
}

/// Evaluate a single concrete `Pattern` against storage **or** `derived` facts.
///
/// If the pattern's predicate is a key in `derived`, all results come from the
/// in-memory set (derived predicates are purely synthetic — they don't exist
/// in the triple store). Otherwise delegates to the normal `evaluate`.
fn evaluate_hybrid(
    pattern: &Pattern,
    snapshot: &Snapshot,
    derived: &DerivedFacts,
) -> Result<Vec<Triple>, StorageError> {
    if let Some(pred) = &pattern.predicate {
        if let Some(pairs) = derived.get(pred) {
            // Filter derived pairs by the bound subject/object slots.
            let triples = pairs
                .iter()
                .filter(|(s, o)| {
                    pattern.subject.map_or(true, |ps| ps == *s)
                        && pattern.object.map_or(true, |po| po == *o)
                })
                .map(|(s, o)| Triple::Relation {
                    subject: *s,
                    predicate: Predicate::new(pred),
                    object: *o,
                    edge_id: EdgeId::new(),
                    temporal: BiTemporalRange::assert_now(Timestamp::now()),
                })
                .collect();
            return Ok(triples);
        }
    }
    evaluate(pattern, snapshot)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use polargraph_core::{
        id::{EdgeId, NodeId},
        temporal::{BiTemporalRange, Timestamp},
        triple::{Predicate, Triple},
        value::Value,
    };
    use polargraph_storage::TripleStore;
    use tempfile::TempDir;

    // ── setup helpers ─────────────────────────────────────────────────────────

    fn open() -> (TripleStore, TempDir) {
        let dir = TempDir::new().unwrap();
        (TripleStore::open(dir.path()).unwrap(), dir)
    }

    fn rel(from: NodeId, pred: &str, to: NodeId) -> Triple {
        Triple::Relation {
            subject: from,
            predicate: Predicate::new(pred),
            object: to,
            edge_id: EdgeId::new(),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        }
    }

    fn prop(node: NodeId, pred: &str, val: impl Into<Value>) -> Triple {
        Triple::Property {
            subject: node,
            predicate: Predicate::new(pred),
            value: val.into(),
            temporal: BiTemporalRange::assert_now(Timestamp::now()),
        }
    }

    fn commit(store: &TripleStore, triples: Vec<Triple>) -> polargraph_storage::Snapshot {
        let mut tx = store.begin();
        for t in triples {
            tx.insert(t);
        }
        let ts = tx.commit().unwrap();
        store.snapshot(ts)
    }

    fn bound(id: NodeId) -> Term {
        Term::Bound(id)
    }
    fn var(name: &str) -> Term {
        Term::Var(name.into())
    }

    fn collect_var(results: &[Bindings], name: &str) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = results
            .iter()
            .filter_map(|b| b.get(name).copied())
            .collect();
        ids.sort();
        ids
    }

    // ── single-pattern (degenerate join) ──────────────────────────────────────

    #[test]
    fn single_pattern_bound_subject() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(alice, "knows", carol),
                rel(bob, "knows", carol),
            ],
        );

        let q = Query::new().pattern(
            VarPattern::new()
                .subject(bound(alice))
                .predicate("knows")
                .object(var("who")),
        );

        let results = execute_query(&q, &snap, None, None).unwrap();
        assert_eq!(results.len(), 2);
        let who = collect_var(&results, "who");
        assert!(who.contains(&bob));
        assert!(who.contains(&carol));
    }

    #[test]
    fn single_pattern_no_match_returns_empty() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let nobody = NodeId::new();

        let snap = commit(&store, vec![rel(alice, "knows", NodeId::new())]);

        let q = Query::new().pattern(
            VarPattern::new()
                .subject(bound(nobody))
                .predicate("knows")
                .object(var("x")),
        );

        assert!(execute_query(&q, &snap, None, None).unwrap().is_empty());
    }

    // ── two-pattern join ──────────────────────────────────────────────────────

    #[test]
    fn two_pattern_colleague_join() {
        // Who are Alice's colleagues? (share the same manager)
        // Pattern 1: (alice, reports-to, ?mgr)
        // Pattern 2: (?colleague, reports-to, ?mgr)
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();
        let dave = NodeId::new(); // different manager
        let mgr_a = NodeId::new();
        let mgr_b = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "reports-to", mgr_a),
                rel(bob, "reports-to", mgr_a),
                rel(carol, "reports-to", mgr_a),
                rel(dave, "reports-to", mgr_b), // different manager — should not appear
            ],
        );

        let q = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate("reports-to")
                    .object(var("mgr")),
            )
            .pattern(
                VarPattern::new()
                    .subject(var("colleague"))
                    .predicate("reports-to")
                    .object(var("mgr")),
            );

        let results = execute_query(&q, &snap, None, None).unwrap();

        // alice, bob, carol all report to mgr_a → 3 results (including alice herself)
        assert_eq!(results.len(), 3, "expected alice, bob, carol as colleagues");

        let colleagues = collect_var(&results, "colleague");
        assert!(colleagues.contains(&alice));
        assert!(colleagues.contains(&bob));
        assert!(colleagues.contains(&carol));
        assert!(!colleagues.contains(&dave));
    }

    #[test]
    fn two_pattern_join_manager_is_consistent_across_results() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let mgr = NodeId::new();

        let snap = commit(
            &store,
            vec![rel(alice, "reports-to", mgr), rel(bob, "reports-to", mgr)],
        );

        let q = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate("reports-to")
                    .object(var("m")),
            )
            .pattern(
                VarPattern::new()
                    .subject(var("c"))
                    .predicate("reports-to")
                    .object(var("m")),
            );

        let results = execute_query(&q, &snap, None, None).unwrap();
        // Every result must have ?m = mgr.
        for b in &results {
            assert_eq!(b["m"], mgr, "?m must always be the same manager");
        }
    }

    // ── three-pattern chain ───────────────────────────────────────────────────

    #[test]
    fn three_pattern_friend_of_friend() {
        // Who does Alice know transitively at depth 2?
        // (alice, knows, ?b) ∧ (?b, knows, ?c)
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();
        let dave = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(alice, "knows", carol),
                rel(bob, "knows", dave),
                rel(carol, "knows", dave),
            ],
        );

        let q = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate("knows")
                    .object(var("b")),
            )
            .pattern(
                VarPattern::new()
                    .subject(var("b"))
                    .predicate("knows")
                    .object(var("c")),
            );

        let results = execute_query(&q, &snap, None, None).unwrap();

        // (alice→bob→dave) and (alice→carol→dave) → 2 paths, both end at dave
        assert_eq!(results.len(), 2);
        let c_vals = collect_var(&results, "c");
        assert!(c_vals.iter().all(|&id| id == dave));
    }

    #[test]
    fn three_pattern_project_chain() {
        // (alice, owns-project, ?proj) ∧ (?proj, uses-tech, ?tech) ∧ (?tech, vendor, ?vendor)
        let (store, _dir) = open();
        let alice = NodeId::new();
        let proj1 = NodeId::new();
        let proj2 = NodeId::new();
        let rust = NodeId::new();
        let python = NodeId::new();
        let mozilla = NodeId::new();
        let psf = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "owns-project", proj1),
                rel(alice, "owns-project", proj2),
                rel(proj1, "uses-tech", rust),
                rel(proj2, "uses-tech", python),
                rel(rust, "vendor", mozilla),
                rel(python, "vendor", psf),
            ],
        );

        let q = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate("owns-project")
                    .object(var("proj")),
            )
            .pattern(
                VarPattern::new()
                    .subject(var("proj"))
                    .predicate("uses-tech")
                    .object(var("tech")),
            )
            .pattern(
                VarPattern::new()
                    .subject(var("tech"))
                    .predicate("vendor")
                    .object(var("vendor")),
            );

        let results = execute_query(&q, &snap, None, None).unwrap();
        assert_eq!(results.len(), 2);

        let vendors = collect_var(&results, "vendor");
        assert!(vendors.contains(&mozilla));
        assert!(vendors.contains(&psf));
    }

    // ── variable reuse and conflict ───────────────────────────────────────────

    #[test]
    fn same_variable_in_subject_and_object_acts_as_self_reference_filter() {
        // (? knows ?) where subject == object — "who knows themselves?"
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "knows", alice), // self-loop
                rel(alice, "knows", bob),   // not a self-loop
            ],
        );

        let q = Query::new().pattern(
            VarPattern::new()
                .subject(var("x"))
                .predicate("knows")
                .object(var("x")),
        );

        let results = execute_query(&q, &snap, None, None).unwrap();
        assert_eq!(results.len(), 1, "only the self-loop satisfies x == x");
        assert_eq!(results[0]["x"], alice);
    }

    #[test]
    fn conflicting_bindings_produce_no_result() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(alice, "knows", carol),
                rel(bob, "knows", bob), // bob has a self-loop; carol does not
            ],
        );

        // Patterns 1+2 bind ?x to bob or carol.
        let q = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate("knows")
                    .object(var("x")),
            )
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate("knows")
                    .object(var("x")),
            )
            // Third pattern: ?x must know itself — only bob satisfies this.
            .pattern(
                VarPattern::new()
                    .subject(var("x"))
                    .predicate("knows")
                    .object(var("x")),
            );

        // carol has no self-loop so its binding is eliminated; only ?x = bob survives.
        let results = execute_query(&q, &snap, None, None).unwrap();
        assert!(results.iter().all(|b| b.get("x") == Some(&bob)));
    }

    // ── property triples with variable subjects ───────────────────────────────

    #[test]
    fn subject_variable_matches_property_triples() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit(
            &store,
            vec![prop(alice, "name", "Alice"), prop(bob, "name", "Bob")],
        );

        let q = Query::new().pattern(
            VarPattern::new()
                .subject(var("who"))
                .predicate("name")
                .object(Term::Any),
        );

        let results = execute_query(&q, &snap, None, None).unwrap();
        assert_eq!(results.len(), 2);
        let who = collect_var(&results, "who");
        assert!(who.contains(&alice));
        assert!(who.contains(&bob));
    }

    #[test]
    fn object_variable_skips_property_triples() {
        // Object var can't bind to a Value — property triples are excluded.
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "knows", bob), // relation → ?obj binds to bob
                prop(alice, "name", "A"), // property → skipped
            ],
        );

        let q = Query::new().pattern(
            VarPattern::new()
                .subject(bound(alice))
                .predicate("name")
                .object(var("obj")),
        );

        // "name" is a property predicate — object is a Value, not a NodeId.
        let results = execute_query(&q, &snap, None, None).unwrap();
        assert!(
            results.is_empty(),
            "object var cannot bind to scalar property value"
        );
    }

    // ── empty query ───────────────────────────────────────────────────────────

    #[test]
    fn empty_query_returns_one_empty_binding() {
        let (store, _dir) = open();
        let snap = commit(&store, vec![]);
        let q = Query::new(); // no patterns
        let results = execute_query(&q, &snap, None, None).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_empty());
    }

    // ── no results from second pattern short-circuits ─────────────────────────

    #[test]
    fn unsatisfiable_second_pattern_returns_empty() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();

        let snap = commit(&store, vec![rel(alice, "knows", bob)]);

        let q = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate("knows")
                    .object(var("x")),
            )
            .pattern(
                VarPattern::new()
                    .subject(var("x"))
                    .predicate("owns-project")
                    .object(var("proj")),
            );

        // bob has no "owns-project" edges → second pattern fails → no results.
        let results = execute_query(&q, &snap, None, None).unwrap();
        assert!(results.is_empty());
    }

    // ── recursive Datalog: execute_recursive ──────────────────────────────────

    // Helper: build a seed vec from a list of (predicate, subject, object) triples.
    fn seed(triples: &[(&str, NodeId, NodeId)]) -> Vec<(String, NodeId, NodeId)> {
        triples
            .iter()
            .map(|(p, s, o)| (p.to_string(), *s, *o))
            .collect()
    }

    #[test]
    fn transitive_closure_linear_chain() {
        // A → B → C → D
        // tc(A, B), tc(A, C), tc(A, D) should all be derived.
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        let d = NodeId::new();

        let snap = commit(
            &store,
            vec![rel(a, "edge", b), rel(b, "edge", c), rel(c, "edge", d)],
        );

        // Seed: direct neighbours of a.
        let s = seed(&[("tc", a, b)]);

        // Rule: tc(X, Z) :- tc(X, Y), edge(Y, Z)
        let rules = vec![Rule::new("tc", "x", "z").with_body(vec![
            VarPattern::new()
                .subject(Term::var("x"))
                .predicate("tc")
                .object(Term::var("y")),
            VarPattern::new()
                .subject(Term::var("y"))
                .predicate("edge")
                .object(Term::var("z")),
        ])];

        let derived = execute_recursive(&s, &rules, &snap, None).unwrap();
        let tc = derived.get("tc").unwrap();

        assert!(tc.contains(&(a, b)), "a→b should be reachable");
        assert!(tc.contains(&(a, c)), "a→c should be reachable");
        assert!(tc.contains(&(a, d)), "a→d should be reachable");
        assert_eq!(tc.len(), 3);
    }

    #[test]
    fn transitive_closure_diamond() {
        // A → B, A → C, B → D, C → D
        // Reachable from A: B, C, D
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        let d = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(a, "edge", b),
                rel(a, "edge", c),
                rel(b, "edge", d),
                rel(c, "edge", d),
            ],
        );

        let s = seed(&[("tc", a, b), ("tc", a, c)]);
        let rules = vec![Rule::new("tc", "x", "z").with_body(vec![
            VarPattern::new()
                .subject(Term::var("x"))
                .predicate("tc")
                .object(Term::var("y")),
            VarPattern::new()
                .subject(Term::var("y"))
                .predicate("edge")
                .object(Term::var("z")),
        ])];

        let derived = execute_recursive(&s, &rules, &snap, None).unwrap();
        let tc = derived.get("tc").unwrap();

        assert!(tc.contains(&(a, b)));
        assert!(tc.contains(&(a, c)));
        assert!(tc.contains(&(a, d)));
        assert_eq!(
            tc.len(),
            3,
            "each target appears only once despite two paths to d"
        );
    }

    #[test]
    fn transitive_closure_terminates_on_cycle() {
        // A → B → C → A  (cycle)
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();

        let snap = commit(
            &store,
            vec![rel(a, "edge", b), rel(b, "edge", c), rel(c, "edge", a)],
        );

        let s = seed(&[("tc", a, b)]);
        let rules = vec![Rule::new("tc", "x", "z").with_body(vec![
            VarPattern::new()
                .subject(Term::var("x"))
                .predicate("tc")
                .object(Term::var("y")),
            VarPattern::new()
                .subject(Term::var("y"))
                .predicate("edge")
                .object(Term::var("z")),
        ])];

        // Must terminate and not loop forever.
        let derived = execute_recursive(&s, &rules, &snap, None).unwrap();
        let tc = derived.get("tc").unwrap();

        // A can reach B, C, and back to A through the cycle.
        assert!(tc.contains(&(a, b)));
        assert!(tc.contains(&(a, c)));
        assert!(tc.contains(&(a, a)), "cycle means a can reach itself");
    }

    #[test]
    fn no_edges_yields_only_seed() {
        // Seed primes tc(A, B) but there are no further edges.
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();

        let snap = commit(&store, vec![]); // empty store

        let s = seed(&[("tc", a, b)]);
        let rules = vec![Rule::new("tc", "x", "z").with_body(vec![
            VarPattern::new()
                .subject(Term::var("x"))
                .predicate("tc")
                .object(Term::var("y")),
            VarPattern::new()
                .subject(Term::var("y"))
                .predicate("edge")
                .object(Term::var("z")),
        ])];

        let derived = execute_recursive(&s, &rules, &snap, None).unwrap();
        let tc = derived.get("tc").unwrap();
        assert_eq!(tc.len(), 1);
        assert!(tc.contains(&(a, b)));
    }

    #[test]
    fn empty_seed_yields_no_derived_facts() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();

        let snap = commit(&store, vec![rel(a, "edge", b)]);

        let rules = vec![Rule::new("tc", "x", "z").with_body(vec![
            VarPattern::new()
                .subject(Term::var("x"))
                .predicate("tc")
                .object(Term::var("y")),
            VarPattern::new()
                .subject(Term::var("y"))
                .predicate("edge")
                .object(Term::var("z")),
        ])];

        let derived = execute_recursive(&[], &rules, &snap, None).unwrap();
        // No seed → no derived facts (the base case for tc has nothing to expand).
        assert!(derived.get("tc").map_or(true, |s| s.is_empty()));
    }

    // ── reachable_from ────────────────────────────────────────────────────────

    use super::reachable_from;

    #[test]
    fn reachable_from_linear_chain() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        let d = NodeId::new();

        let snap = commit(
            &store,
            vec![rel(a, "knows", b), rel(b, "knows", c), rel(c, "knows", d)],
        );

        let reachable = reachable_from(a, "knows", &snap, None).unwrap();
        assert!(reachable.contains(&b));
        assert!(reachable.contains(&c));
        assert!(reachable.contains(&d));
        assert_eq!(reachable.len(), 3);
    }

    #[test]
    fn reachable_from_diamond() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        let d = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(a, "knows", b),
                rel(a, "knows", c),
                rel(b, "knows", d),
                rel(c, "knows", d),
            ],
        );

        let reachable = reachable_from(a, "knows", &snap, None).unwrap();
        assert_eq!(reachable.len(), 3);
        assert!(reachable.contains(&b));
        assert!(reachable.contains(&c));
        assert!(reachable.contains(&d));
    }

    #[test]
    fn reachable_from_cycle_terminates() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();

        let snap = commit(
            &store,
            vec![rel(a, "knows", b), rel(b, "knows", c), rel(c, "knows", a)],
        );

        let reachable = reachable_from(a, "knows", &snap, None).unwrap();
        // b and c are reachable; a itself via cycle.
        assert!(reachable.contains(&b));
        assert!(reachable.contains(&c));
        assert!(
            reachable.contains(&a),
            "a can reach itself through the cycle"
        );
    }

    #[test]
    fn reachable_from_isolated_node_returns_empty() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();

        // b and c are connected but a has no outgoing "knows" edges.
        let snap = commit(&store, vec![rel(b, "knows", c)]);

        let reachable = reachable_from(a, "knows", &snap, None).unwrap();
        assert!(reachable.is_empty());
    }

    #[test]
    fn reachable_from_self_loop_included() {
        let (store, _dir) = open();
        let a = NodeId::new();

        let snap = commit(&store, vec![rel(a, "knows", a)]);

        let reachable = reachable_from(a, "knows", &snap, None).unwrap();
        assert!(reachable.contains(&a), "self-loop: a knows itself");
    }

    #[test]
    fn reachable_from_does_not_cross_predicate_boundary() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();

        // a→b via "knows", b→c via "manages" — reachability only follows "knows".
        let snap = commit(&store, vec![rel(a, "knows", b), rel(b, "manages", c)]);

        let reachable = reachable_from(a, "knows", &snap, None).unwrap();
        assert!(reachable.contains(&b));
        assert!(
            !reachable.contains(&c),
            "c only reachable via 'manages', not 'knows'"
        );
    }

    // ── deadline / timeout ────────────────────────────────────────────────────

    #[test]
    fn execute_recursive_expired_deadline_returns_timeout() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let snap = commit(&store, vec![rel(a, "edge", b)]);

        // A deadline that is already in the past fires on the first iteration.
        let expired = Some(Instant::now() - std::time::Duration::from_millis(1));
        let s = seed(&[("tc", a, b)]);
        let rules = vec![Rule::new("tc", "x", "z").with_body(vec![
            VarPattern::new()
                .subject(Term::var("x"))
                .predicate("tc")
                .object(Term::var("y")),
            VarPattern::new()
                .subject(Term::var("y"))
                .predicate("edge")
                .object(Term::var("z")),
        ])];

        let result = execute_recursive(&s, &rules, &snap, expired);
        assert!(
            matches!(result, Err(QueryError::Timeout)),
            "expected Timeout, got {result:?}"
        );
    }

    #[test]
    fn execute_query_expired_deadline_returns_timeout() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let snap = commit(&store, vec![rel(a, "knows", b)]);

        let expired = Some(Instant::now() - std::time::Duration::from_millis(1));
        let q = Query::new().pattern(
            VarPattern::new()
                .subject(Term::Bound(a))
                .predicate("knows")
                .object(Term::var("x")),
        );

        let result = execute_query(&q, &snap, expired, None);
        assert!(
            matches!(result, Err(QueryError::Timeout)),
            "expected Timeout, got {result:?}"
        );
    }

    #[test]
    fn execute_query_none_deadline_never_times_out() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let snap = commit(&store, vec![rel(a, "knows", b)]);

        let q = Query::new().pattern(
            VarPattern::new()
                .subject(Term::Bound(a))
                .predicate("knows")
                .object(Term::var("x")),
        );

        let result = execute_query(&q, &snap, None, None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    // ── bounded-hop VarPattern (max_hops) ─────────────────────────────────────

    #[test]
    fn bounded_hop_1_returns_direct_neighbours_only() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        // chain: a → b → c
        let snap = commit(&store, vec![rel(a, "hop", b), rel(b, "hop", c)]);

        let mut vp = VarPattern::new()
            .subject(Term::Bound(a))
            .predicate("hop")
            .object(Term::var("x"));
        vp.max_hops = Some(1);

        let q = Query::new().pattern(vp);
        let results = execute_query(&q, &snap, None, None).unwrap();
        let found: Vec<NodeId> = results.iter().filter_map(|b| b.get("x").copied()).collect();
        assert_eq!(found.len(), 1);
        assert!(
            found.contains(&b),
            "only direct neighbour should be reachable at depth 1"
        );
        assert!(!found.contains(&c), "c is 2 hops away, should not appear");
    }

    #[test]
    fn bounded_hop_3_reaches_across_chain() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();
        let d = NodeId::new();
        let e = NodeId::new();
        // chain: a → b → c → d → e
        let snap = commit(
            &store,
            vec![
                rel(a, "hop", b),
                rel(b, "hop", c),
                rel(c, "hop", d),
                rel(d, "hop", e),
            ],
        );

        let mut vp = VarPattern::new()
            .subject(Term::Bound(a))
            .predicate("hop")
            .object(Term::var("x"));
        vp.max_hops = Some(3);

        let q = Query::new().pattern(vp);
        let results = execute_query(&q, &snap, None, None).unwrap();
        let found: std::collections::HashSet<NodeId> =
            results.iter().filter_map(|b| b.get("x").copied()).collect();

        assert!(found.contains(&b), "b is 1 hop away");
        assert!(found.contains(&c), "c is 2 hops away");
        assert!(found.contains(&d), "d is 3 hops away");
        assert!(!found.contains(&e), "e is 4 hops away, beyond max_hops=3");
    }

    // ── predicate variable (predicate_var) ────────────────────────────────────

    #[test]
    fn predicate_var_bound_subject_returns_all_predicates() {
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let acme = NodeId::new();

        let snap = commit(
            &store,
            vec![rel(alice, "knows", bob), rel(alice, "manages", acme)],
        );

        // Pattern: (alice, ?p, ?o) — enumerate all outgoing edges from alice.
        let q = Query::new().pattern(
            VarPattern::new()
                .subject(bound(alice))
                .predicate_var("p")
                .object(var("o")),
        );

        let results = execute_query_full(&q, &snap, None, None).unwrap();
        assert_eq!(results.len(), 2, "alice has two outgoing edges");

        let preds: std::collections::HashSet<String> = results
            .iter()
            .map(|(_nb, pb)| pb.get("p").cloned().unwrap())
            .collect();
        assert!(preds.contains("knows"), "predicate 'knows' must be bound");
        assert!(
            preds.contains("manages"),
            "predicate 'manages' must be bound"
        );

        // Each binding should also have the object variable set.
        let objects: std::collections::HashSet<NodeId> = results
            .iter()
            .map(|(nb, _pb)| nb.get("o").copied().unwrap())
            .collect();
        assert!(objects.contains(&bob));
        assert!(objects.contains(&acme));
    }

    #[test]
    fn predicate_var_fully_unbound_returns_all_triples() {
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();
        let c = NodeId::new();

        let snap = commit(&store, vec![rel(a, "knows", b), rel(b, "manages", c)]);

        // Fully unbound: (?s, ?p, ?o)
        let q = Query::new().pattern(
            VarPattern::new()
                .subject(var("s"))
                .predicate_var("p")
                .object(var("o")),
        );

        let results = execute_query_full(&q, &snap, None, None).unwrap();
        assert_eq!(results.len(), 2, "two triples in the store");

        let preds: std::collections::HashSet<String> = results
            .iter()
            .map(|(_nb, pb)| pb.get("p").cloned().unwrap())
            .collect();
        assert!(preds.contains("knows"));
        assert!(preds.contains("manages"));
    }

    #[test]
    fn predicate_var_joins_correctly_with_fixed_predicate_pattern() {
        // Pattern 1: (alice, ?p, ?o)  — bind ?p and ?o for each outgoing edge
        // Pattern 2: (?o, "knows", ?x) — join on ?o with a fixed predicate
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();
        let dave = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(alice, "manages", carol),
                rel(bob, "knows", dave), // bob knows dave — joins on Pattern 2
                rel(carol, "reports", dave), // carol doesn't "know" anyone — no join
            ],
        );

        let q = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate_var("p")
                    .object(var("o")),
            )
            .pattern(
                VarPattern::new()
                    .subject(var("o"))
                    .predicate("knows")
                    .object(var("x")),
            );

        let results = execute_query_full(&q, &snap, None, None).unwrap();

        // Only alice→bob satisfies Pattern 2 (bob knows dave); alice→carol does not.
        assert_eq!(results.len(), 1, "only the knows→knows chain joins");
        let (nb, pb) = &results[0];
        assert_eq!(nb.get("o").copied(), Some(bob));
        assert_eq!(nb.get("x").copied(), Some(dave));
        assert_eq!(pb.get("p").map(String::as_str), Some("knows"));
    }

    #[test]
    fn predicate_var_respects_conflict_within_pattern() {
        // Self-join: (?x, ?p, ?x) — only triples where subject == object.
        let (store, _dir) = open();
        let a = NodeId::new();
        let b = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(a, "self", a), // self-loop — satisfies ?x == ?x
                rel(a, "edge", b), // a ≠ b — should not match
            ],
        );

        let q = Query::new().pattern(
            VarPattern::new()
                .subject(var("x"))
                .predicate_var("p")
                .object(var("x")), // same variable → self-loop only
        );

        let results = execute_query_full(&q, &snap, None, None).unwrap();
        assert_eq!(results.len(), 1, "only the self-loop satisfies x == x");
        let (nb, pb) = &results[0];
        assert_eq!(nb.get("x").copied(), Some(a));
        assert_eq!(pb.get("p").map(String::as_str), Some("self"));
    }

    #[test]
    fn predicate_var_substituted_as_predicate_in_later_pattern() {
        // Bind ?p in pattern 1, then use ?p as the predicate in pattern 2.
        // This exercises the substitute_with_preds path.
        let (store, _dir) = open();
        let alice = NodeId::new();
        let bob = NodeId::new();
        let carol = NodeId::new();

        let snap = commit(
            &store,
            vec![
                rel(alice, "knows", bob),
                rel(bob, "knows", carol),
                rel(alice, "manages", carol), // different predicate — should not appear
            ],
        );

        // Pattern 1: (alice, ?p, bob)   → binds p="knows"
        // Pattern 2: (bob, ?p, ?x)      → reuses p="knows" as the predicate
        let q = Query::new()
            .pattern(
                VarPattern::new()
                    .subject(bound(alice))
                    .predicate_var("p")
                    .object(bound(bob)),
            )
            .pattern(
                VarPattern::new()
                    .subject(bound(bob))
                    .predicate_var("p") // same variable — must reuse "knows"
                    .object(var("x")),
            );

        let results = execute_query_full(&q, &snap, None, None).unwrap();
        assert_eq!(
            results.len(),
            1,
            "only the knows chain satisfies both patterns"
        );
        let (nb, pb) = &results[0];
        assert_eq!(nb.get("x").copied(), Some(carol));
        assert_eq!(pb.get("p").map(String::as_str), Some("knows"));
    }
}
