//! Vocabulary IRIs.

pub const SH: &str = "http://www.w3.org/ns/shacl#";
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
pub const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
pub const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
pub const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
pub const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
pub const RDFS_CLASS: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// A SHACL term IRI (`sh:` + `local`).
pub fn sh(local: &str) -> String {
    format!("{SH}{local}")
}
