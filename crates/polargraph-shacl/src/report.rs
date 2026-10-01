//! Validation results.

use polargraph_core::id::NodeId;
use serde::{Deserialize, Serialize};

use crate::{data::Obj, shapes::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Severity {
    Info,
    Warning,
    Violation,
}

impl Severity {
    /// The `sh:` local name.
    pub fn local_name(self) -> &'static str {
        match self {
            Severity::Info => "Info",
            Severity::Warning => "Warning",
            Severity::Violation => "Violation",
        }
    }
}

/// One `sh:ValidationResult`.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub focus_node: Obj,
    pub path: Option<Path>,
    pub value: Option<Obj>,
    pub source_shape: NodeId,
    /// Constraint component local name, e.g. `MinCountConstraintComponent`.
    pub component: &'static str,
    pub severity: Severity,
    pub message: String,
}

/// The outcome of a validation run.
#[derive(Debug, Clone, Default)]
pub struct ValidationReport {
    pub results: Vec<ValidationResult>,
}

impl ValidationReport {
    /// `sh:conforms`: no results at all (any severity, as in SHACL).
    pub fn conforms(&self) -> bool {
        self.results.is_empty()
    }

    /// Whether any result has severity `Violation` (callers that treat
    /// warnings as advisory gate on this).
    pub fn has_violations(&self) -> bool {
        self.results
            .iter()
            .any(|r| r.severity == Severity::Violation)
    }
}
