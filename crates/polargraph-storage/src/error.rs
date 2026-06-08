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
}
