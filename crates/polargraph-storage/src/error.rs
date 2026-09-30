use crate::mvcc::ConflictError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("RocksDB error: {0}")]
    Rocks(#[from] rocksdb::Error),

    #[error("Serialisation error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Column family '{0}' not found")]
    MissingCf(String),

    #[error("Key decode error: {0}")]
    KeyDecode(String),

    #[error("Write conflict: {0}")]
    WriteConflict(ConflictError),

    #[error("{0}")]
    ReadOnly(String),

    #[error("Validation error: {0}")]
    Validation(String),

    /// The store is still in storage format v2; run `polargraphd migrate`.
    #[error("store uses storage format v2; stop the server and run `polargraphd migrate` (see docs/design/v3-key-layout.md)")]
    NeedsMigration,

    /// The store was written by a newer build.
    #[error("unsupported storage format {0}; this build reads format 3")]
    UnsupportedFormat(u32),

    /// Two different IRIs hash to the same `NodeId` (xxHash3-128 collision).
    #[error("IRI collision on node {node}: {existing:?} already stored, {new:?} rejected")]
    IriCollision {
        node: polargraph_core::id::NodeId,
        existing: String,
        new: String,
    },
}
