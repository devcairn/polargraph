//! Schema migration runner.
//!
//! Migrations are versioned, ordered, and idempotent up/down functions. The
//! current schema version is stored in the META column family under
//! `__migrations__/version` (little-endian u32). Each applied migration is
//! also recorded under `__migrations__/applied/<version>` as a JSON blob.
//!
//! # Usage
//!
//! ```ignore
//! let runner = MigrationRunner::new(store.clone());
//! let stats = runner.run_pending()?;
//! println!("applied: {:?}", stats.applied);
//! ```
//!
//! # Adding a migration
//!
//! Append a new [`Migration`] to the [`MIGRATIONS`] static list. The version
//! must be strictly greater than the previous entry (gaps are allowed).

use crate::{cf, error::StorageError, registry::NodeTypeRegistry, store::TripleStore};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;

// ── META key constants ────────────────────────────────────────────────────────

const META_VERSION_KEY: &[u8] = b"__migrations__/version";
const META_APPLIED_PREFIX: &str = "__migrations__/applied/";

// ── Public types ──────────────────────────────────────────────────────────────

/// A single versioned schema migration with reversible up/down operations.
pub struct Migration {
    pub version: u32,
    pub description: &'static str,
    /// Apply the migration. Called once per `run_pending()` when the stored
    /// version is less than `version`.
    pub up: fn(&TripleStore) -> Result<(), StorageError>,
    /// Revert the migration. Called by `rollback()` when unwinding past
    /// `version`.
    pub down: fn(&TripleStore) -> Result<(), StorageError>,
}

/// Summary of migrations applied or skipped by a single `run_pending()` call.
#[derive(Debug, Clone, Default)]
pub struct MigrationStats {
    pub applied: Vec<u32>,
    pub skipped: Vec<u32>,
}

/// Persisted record of a migration that has been applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedMigration {
    pub version: u32,
    pub description: String,
    /// Wall-clock microseconds since Unix epoch when the migration ran.
    pub applied_at_tx_time: i64,
}

// ── Built-in migrations ───────────────────────────────────────────────────────

fn migration_v1_up(store: &TripleStore) -> Result<(), StorageError> {
    let meta_cf = store.cf_handle(cf::META)?;
    let mut batch = WriteBatch::default();
    batch.put_cf(&meta_cf, b"__schema__/version", b"1");
    store.db_write(batch)
}

fn migration_v1_down(store: &TripleStore) -> Result<(), StorageError> {
    let meta_cf = store.cf_handle(cf::META)?;
    let mut batch = WriteBatch::default();
    batch.delete_cf(&meta_cf, b"__schema__/version");
    store.db_write(batch)
}

fn migration_v2_up(store: &TripleStore) -> Result<(), StorageError> {
    // Re-serialize all registered node type schemas through the current
    // NodeSchemaJson format. This picks up any new serde-defaulted fields
    // (e.g. storage_mode on VectorSpaceDef) in existing records that were
    // written by an older version of the server.
    let registry = NodeTypeRegistry::new(store.clone())?;
    for def in registry.list_types() {
        registry.register_type(def)?;
    }
    Ok(())
}

fn migration_v2_down(_store: &TripleStore) -> Result<(), StorageError> {
    // The re-serialization is backward-compatible — old readers can still
    // deserialize the records via serde defaults, so no undo is needed.
    Ok(())
}

/// All built-in migrations, in ascending version order.
///
/// Every entry must have a strictly greater `version` than the previous one.
/// Gaps are allowed (skipped versions are simply never applied).
pub static MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        description: "initial schema",
        up: migration_v1_up,
        down: migration_v1_down,
    },
    Migration {
        version: 2,
        description: "normalize node type schema records",
        up: migration_v2_up,
        down: migration_v2_down,
    },
];

// ── Runner ────────────────────────────────────────────────────────────────────

/// Applies and tracks schema migrations against a live [`TripleStore`].
pub struct MigrationRunner {
    store: TripleStore,
}

impl MigrationRunner {
    pub fn new(store: TripleStore) -> Self {
        Self { store }
    }

    /// Return the highest migration version that has been applied to this
    /// database. Returns `0` on a fresh (never-migrated) database.
    pub fn current_version(&self) -> Result<u32, StorageError> {
        let meta_cf = self.store.cf_handle(cf::META)?;
        match self.store.db_ref().get_cf(&meta_cf, META_VERSION_KEY)? {
            Some(bytes) if bytes.len() >= 4 => Ok(u32::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
            ])),
            _ => Ok(0),
        }
    }

    /// The highest version in the [`MIGRATIONS`] list.
    pub fn latest_version() -> u32 {
        MIGRATIONS.last().map(|m| m.version).unwrap_or(0)
    }

    /// Apply all migrations whose `version` is greater than
    /// [`current_version`](Self::current_version). Returns the set of versions
    /// applied and skipped.
    ///
    /// Each migration is applied in order; on failure the run stops and the
    /// error is returned. The version counter advances only after each
    /// successful migration.
    pub fn run_pending(&self) -> Result<MigrationStats, StorageError> {
        let current = self.current_version()?;
        let mut stats = MigrationStats::default();

        for migration in MIGRATIONS {
            if migration.version <= current {
                stats.skipped.push(migration.version);
                continue;
            }
            info!(
                version = migration.version,
                description = migration.description,
                "applying migration"
            );
            (migration.up)(&self.store)?;
            self.record_applied(migration)?;
            stats.applied.push(migration.version);
            info!(version = migration.version, "migration applied");
        }

        Ok(stats)
    }

    /// Roll back all migrations with `version > to_version`, in reverse order.
    ///
    /// After rollback the stored version reflects the highest still-applied
    /// version (or `0` if all were rolled back).
    pub fn rollback(&self, to_version: u32) -> Result<MigrationStats, StorageError> {
        let current = self.current_version()?;
        let mut stats = MigrationStats::default();

        let to_rollback: Vec<&Migration> = MIGRATIONS
            .iter()
            .filter(|m| m.version > to_version && m.version <= current)
            .collect();

        for migration in to_rollback.into_iter().rev() {
            info!(
                version = migration.version,
                description = migration.description,
                "rolling back migration"
            );
            (migration.down)(&self.store)?;
            self.remove_applied(migration.version)?;
            stats.applied.push(migration.version);
            info!(version = migration.version, "migration rolled back");
        }

        Ok(stats)
    }

    /// Return all applied migrations in ascending version order.
    pub fn list_applied(&self) -> Result<Vec<AppliedMigration>, StorageError> {
        let meta_cf = self.store.cf_handle(cf::META)?;
        let prefix = META_APPLIED_PREFIX.as_bytes();
        let iter = self.store.db_ref().iterator_cf(
            &meta_cf,
            rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );
        let mut results = Vec::new();
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            let applied: AppliedMigration = serde_json::from_slice(&v)?;
            results.push(applied);
        }
        results.sort_by_key(|m| m.version);
        Ok(results)
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn record_applied(&self, migration: &Migration) -> Result<(), StorageError> {
        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;
        let applied = AppliedMigration {
            version: migration.version,
            description: migration.description.to_string(),
            applied_at_tx_time: now_micros,
        };
        let json = serde_json::to_vec(&applied)?;
        let key = format!("{META_APPLIED_PREFIX}{:010}", migration.version);
        let meta_cf = self.store.cf_handle(cf::META)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key.as_bytes(), &json);
        batch.put_cf(
            &meta_cf,
            META_VERSION_KEY,
            migration.version.to_le_bytes(),
        );
        self.store.db_write(batch)
    }

    fn remove_applied(&self, version: u32) -> Result<(), StorageError> {
        let key = format!("{META_APPLIED_PREFIX}{:010}", version);
        let meta_cf = self.store.cf_handle(cf::META)?;

        // Compute the new current version: highest still-applied version
        // after removing `version`.
        let applied = self.list_applied()?;
        let new_version = applied
            .iter()
            .filter(|m| m.version != version)
            .map(|m| m.version)
            .max()
            .unwrap_or(0);

        let mut batch = WriteBatch::default();
        batch.delete_cf(&meta_cf, key.as_bytes());
        batch.put_cf(&meta_cf, META_VERSION_KEY, new_version.to_le_bytes());
        self.store.db_write(batch)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_store() -> (TripleStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = TripleStore::open(dir.path()).unwrap();
        (store, dir)
    }

    #[test]
    fn fresh_db_applies_all_migrations() {
        let (store, _dir) = open_store();
        let runner = MigrationRunner::new(store);
        let stats = runner.run_pending().unwrap();

        assert_eq!(stats.applied, vec![1, 2]);
        assert!(stats.skipped.is_empty());
        assert_eq!(runner.current_version().unwrap(), 2);
    }

    #[test]
    fn already_at_latest_run_pending_is_noop() {
        let (store, _dir) = open_store();
        let runner = MigrationRunner::new(store);
        runner.run_pending().unwrap();

        // Second run must be a no-op.
        let stats = runner.run_pending().unwrap();
        assert!(stats.applied.is_empty());
        assert_eq!(stats.skipped, vec![1, 2]);
        assert_eq!(runner.current_version().unwrap(), 2);
    }

    #[test]
    fn list_applied_returns_all_applied_migrations() {
        let (store, _dir) = open_store();
        let runner = MigrationRunner::new(store);
        runner.run_pending().unwrap();

        let applied = runner.list_applied().unwrap();
        assert_eq!(applied.len(), 2);
        assert_eq!(applied[0].version, 1);
        assert_eq!(applied[0].description, "initial schema");
        assert_eq!(applied[1].version, 2);
        assert!(applied[1].applied_at_tx_time > 0);
    }
}
