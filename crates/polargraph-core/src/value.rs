//! Property values stored on nodes and edges.

use serde::{Deserialize, Serialize};

/// A typed property value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "v")]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
    /// Dense float embedding vector. Stored in binary (LE f32) rather than JSON.
    Vector(Vec<f32>),
    /// A language-tagged string (`rdf:langString`), e.g. `"Acme"@en`.
    /// `lang` is a BCP 47 tag as written (not normalised).
    LangText {
        text: String,
        lang: String,
    },
    /// An RDF literal whose datatype has no native variant, e.g.
    /// `"2026-09-29"^^xsd:date`. `datatype` is the full IRI; `lexical` is the
    /// literal's lexical form, unchanged.
    Typed {
        lexical: String,
        datatype: String,
    },
}

impl Value {
    /// The string content of textual values (`Text` and `LangText`), for
    /// text search and string predicates.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) | Value::LangText { text: s, .. } => Some(s),
            _ => None,
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}
impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(v)
    }
}
