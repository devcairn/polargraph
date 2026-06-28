//! SPARQL 1.1 translation layer for PolarGraph.
//!
//! Translates parsed SPARQL queries into PolarGraph native query types
//! (`VarPattern`, `Rule`, etc.) for execution against the graph store.
//!
//! Also provides in-process execution helpers for SPARQL semantics that cannot
//! be expressed as simple pattern queries (left join, aggregation).
//!
//! The [`rdf_import`] module adds multi-format RDF parsing (N-Triples, Turtle,
//! JSON-LD) and the [`serialize`] module adds JSON-LD and schema OWL/RDFS
//! serialization.

pub mod execute;
pub mod protocol;
pub mod rdf_import;
pub mod response;
pub mod serialize;
pub mod translate;

pub use protocol::negotiate_format;
pub use rdf_import::{
    bnode_to_node_id, edge_id_for, parse_jsonld, parse_ntriples, parse_turtle, uri_to_node_id,
    ImportedObject, ImportedTriple,
};
pub use response::{
    node_bindings_to_sparql, serialize_csv, serialize_json, ResponseFormat, SparqlBindings,
    SparqlValue,
};
pub use serialize::{
    node_id_to_iri, parse_schema_rdf, serialize_jsonld, serialize_ntriples,
    serialize_ntriples_star, serialize_schema_rdf, serialize_turtle, serialize_turtle_star,
    strip_brackets, value_to_nt_literal, RdfStarSubject, RdfStarTriple, RdfTriple, SchemaEdgeType,
    SchemaField, SchemaNodeType,
};
pub use translate::{
    translate_construct, translate_pattern_pub, translate_query, Branch, ConstructTemplate,
    ConstructTranslation, EdgeAnnotationObjectStep, EdgeAnnotationStep, SparqlAggFunc,
    SparqlAggregateSpec, SparqlFilter, SparqlLiteral, SparqlTranslation,
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
