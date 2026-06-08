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
}

impl From<bool> for Value {
    fn from(v: bool) -> Self { Value::Bool(v) }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self { Value::Int(v) }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self { Value::Float(v) }
}
impl From<String> for Value {
    fn from(v: String) -> Self { Value::Text(v) }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self { Value::Text(v.to_owned()) }
}
impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self { Value::Vector(v) }
}
