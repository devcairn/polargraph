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

pub mod datalog;
pub mod eval;
pub mod planner;
pub mod projection;

pub use datalog::{
    execute_query, execute_recursive, reachable_from, Bindings, DerivedFacts, Query, Rule, Term,
    VarPattern,
};
pub use eval::evaluate;
pub use planner::{choose_index, IndexChoice, Pattern};
pub use projection::{apply_view, ProjectedTriple};
