//! SPARQL 1.1 translation layer for PolarGraph.
//!
//! Translates parsed SPARQL queries into PolarGraph native query types
//! (`VarPattern`, `Rule`, etc.) for execution against the graph store.
//!
//! Also provides in-process execution helpers for SPARQL semantics that cannot
//! be expressed as simple pattern queries (left join, aggregation).

pub mod execute;
pub mod protocol;
pub mod response;
pub mod serialize;
pub mod translate;

pub use protocol::negotiate_format;
pub use response::{
    node_bindings_to_sparql, serialize_csv, serialize_json, ResponseFormat, SparqlBindings,
    SparqlValue,
};
pub use serialize::{
    node_id_to_iri, serialize_ntriples, serialize_turtle, value_to_nt_literal, RdfTriple,
};
pub use translate::{
    translate_construct, translate_pattern_pub, translate_query, Branch, ConstructTemplate,
    ConstructTranslation, EdgeAnnotationStep, SparqlAggFunc, SparqlAggregateSpec, SparqlFilter,
    SparqlLiteral, SparqlTranslation,
};

#[derive(Debug, thiserror::Error)]
pub enum SparqlError {
    #[error("SPARQL parse error: {0}")]
    ParseError(String),
    #[error("unsupported SPARQL feature: {0}")]
    Unsupported(String),
    #[error("translation error: {0}")]
    TranslationError(String),
}
