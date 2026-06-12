//! Query layer: pattern evaluation and view projection.
//!
//! # Usage
//!
//! ```rust,ignore
//! use polargraph_query::{evaluate, apply_view, Pattern};
//!
//! // Evaluate a pattern against a snapshot.
//! let snap = store.snapshot(ts);
//! let triples = evaluate(&Pattern::new().with_subject(alice), &snap)?;
//!
//! // Project through a view for display.
//! let projected = apply_view(&org_chart_view, triples);
//! for pt in projected {
//!     println!("{} --[{}]--> ...", pt.triple.subject(), pt.display_label);
//! }
//! ```

pub mod aggregation;
pub mod cypher;
pub mod datalog;
pub mod eval;
pub mod explain;
pub mod planner;
pub mod projection;

pub use datalog::{
    execute_query, execute_query_hybrid, execute_query_seeded, execute_query_with_pending,
    execute_recursive, reachable_from, reachable_from_hops, Bindings, DerivedFacts, Query,
    QueryError, Rule, Term, VarPattern,
};
pub use eval::{evaluate, evaluate_annotations, evaluate_with_registry};
pub use explain::{explain_query, ExplainPlan, ExplainStep};
pub use planner::{
    choose_annotation_index, choose_index, AnnotationIndexChoice, AnnotationPattern,
    IndexChoice, Pattern, SchemaHints,
};
pub use projection::{apply_view, ProjectedTriple};
