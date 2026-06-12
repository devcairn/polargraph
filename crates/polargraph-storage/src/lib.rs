//! Storage layer: RocksDB-backed triple store with MVCC.
//!
//! # Index layout
//!
//! Six column families provide O(log n) lookups for any (S,P,O) bind pattern:
//!
//! | CF  | Key order        | Use case                              |
//! |-----|------------------|---------------------------------------|
//! | SPO | subject→pred→obj | "all edges from S", "edge S→P→?"      |
//! | SOP | subject→obj→pred | "all edges between S and O"           |
//! | PSO | pred→subject→obj | "all edges with predicate P from S"   |
//! | POS | pred→obj→subject | "all edges with predicate P to O"     |
//! | OSP | obj→subject→pred | "all edges to O"                      |
//! | OPS | obj→pred→subject | "all edges to O with predicate P"     |

pub mod backup;
pub mod cf;
pub mod codec;
pub mod compaction;
pub mod error;
pub mod hnsw;
pub mod keys;
pub mod migrations;
pub mod mvcc;
pub mod registry;
pub mod sst_import;
pub mod store;
pub mod wal_stream;

pub use backup::BackupManager;
pub use compaction::{CompactionManager, RetentionStats};
pub use error::StorageError;
pub use migrations::{AppliedMigration, MigrationRunner, MigrationStats, MIGRATIONS};
pub use mvcc::{ConflictError, Snapshot, Transaction};
pub use polargraph_core::schema::{RetentionPolicy, VectorSpaceDef};
pub use registry::{EdgeTypeRegistry, NodeTypeRegistry, ValidationError, SCHEMA_REGISTRY_NODE};
pub use sst_import::{ImportStats, SstImporter};
pub use store::{EdgeAnnotation, EdgeAnnotationValue, StoreMode, TripleStore};
pub use wal_stream::{WalEntry, WalStreamer};
