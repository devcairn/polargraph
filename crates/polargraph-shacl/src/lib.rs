//! SHACL Core validation for PolarGraph (plan step 8b,
//! `docs/design/proposals-shacl.md`).
//!
//! Shapes are read from shapes graphs in the store ([`Shapes::load`]) and
//! evaluated against a [`DataView`]: a dataset of graphs at a snapshot,
//! optionally with an uncommitted overlay of adds and retractions — so a
//! changeset can be validated before `ApplyChanges` commits it.
//!
//! Supported (v1): node and property shapes; `sh:targetClass` (with
//! `rdfs:subClassOf`), `sh:targetNode`, `sh:targetSubjectsOf`,
//! `sh:targetObjectsOf`; predicate, inverse and sequence paths;
//! `sh:minCount` / `sh:maxCount`; `sh:datatype`, `sh:class`, `sh:nodeKind`;
//! `sh:minInclusive` / `sh:maxInclusive` / `sh:minExclusive` /
//! `sh:maxExclusive`; `sh:pattern` (+ `sh:flags`), `sh:minLength` /
//! `sh:maxLength`; `sh:in`; `sh:node`; `sh:closed` + `sh:ignoredProperties`;
//! `sh:severity`, `sh:message`, `sh:deactivated`.

pub mod data;
pub mod report;
pub mod shapes;
pub mod validate;
pub mod vocab;

pub use data::{DataView, Obj, Overlay};
pub use report::{Severity, ValidationReport, ValidationResult};
pub use shapes::{Shape, ShapeError, Shapes};
pub use validate::validate;
