//! SPARQL 1.1 translation layer for PolarGraph.
//!
//! Translates parsed SPARQL queries into PolarGraph native query types
//! (`VarPattern`, `Rule`, etc.) for execution against the graph store.

pub mod protocol;
pub mod response;
pub mod translate;

pub use protocol::negotiate_format;
pub use response::{serialize_csv, serialize_json, ResponseFormat};
pub use translate::{translate_query, Branch, SparqlFilter, SparqlTranslation};

#[derive(Debug, thiserror::Error)]
pub enum SparqlError {
    #[error("SPARQL parse error: {0}")]
    ParseError(String),
    #[error("unsupported SPARQL feature: {0}")]
    Unsupported(String),
    #[error("translation error: {0}")]
    TranslationError(String),
}
