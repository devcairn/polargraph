//! RocksDB backup engine wrapper.
//!
//! Uses RocksDB's built-in `BackupEngine` to create incremental backups.
//! SST files that haven't changed since the previous backup are hard-linked
//! in the backup directory, so successive backups are space-efficient.
//!
//! # Restore
//!
//! Restore is an **offline** operation. The database must not be running when
//! a restore is performed. See `restore_from_backup` docs and
//! `docs/architecture.md` for the step-by-step runbook.

use crate::{error::StorageError, store::TripleStore};
use rocksdb::{
    backup::{BackupEngine, BackupEngineInfo, BackupEngineOptions, RestoreOptions},
    Env,
};
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Metadata for a single backup.
#[derive(Debug, Clone)]
pub struct BackupInfo {
    pub backup_id: u32,
    /// Unix timestamp (seconds since epoch) when the backup was created.
    pub timestamp: i64,
    pub size_bytes: u64,
    pub num_files: u32,
}

impl From<BackupEngineInfo> for BackupInfo {
    fn from(i: BackupEngineInfo) -> Self {
        Self {
            backup_id: i.backup_id,
            timestamp: i.timestamp,
            size_bytes: i.size,
            num_files: i.num_files,
        }
    }
}

/// Newtype that lets `BackupEngine` cross thread boundaries.
///
/// `BackupEngine` holds a raw C pointer and does not implement `Send`.
/// Every access is serialized through the `Mutex` in `BackupManager`.
struct SendEngine(BackupEngine);

// SAFETY: All mutable access to the inner BackupEngine is serialized via the
// Mutex<SendEngine> in BackupManager. The underlying rocksdb_backup_engine_t
// is safe to use from any thread when externally synchronized.
unsafe impl Send for SendEngine {}

/// Manages incremental RocksDB backups for a [`TripleStore`].
///
/// Cloning `PolarGraphServer` shares the same `Arc<BackupManager>`, so all
/// clones serialize backup operations through a single internal lock.
pub struct BackupManager {
    engine: Mutex<SendEngine>,
    store: TripleStore,
    #[allow(dead_code)]
    backup_dir: PathBuf,
}

impl BackupManager {
    /// Open (or create) a backup engine rooted at `backup_dir`.
    ///
    /// `backup_dir` is created if it does not already exist.
    pub fn open(backup_dir: &Path, store: &TripleStore) -> Result<Self, StorageError> {
        std::fs::create_dir_all(backup_dir)?;
        let opts = BackupEngineOptions::new(backup_dir)?;
        let env = Env::new()?;
        let engine = BackupEngine::open(&opts, &env)?;
        Ok(Self {
            engine: Mutex::new(SendEngine(engine)),
            store: store.clone(),
            backup_dir: backup_dir.to_path_buf(),
        })
    }

    /// Create a new incremental backup of the live store.
    ///
    /// SST files already present in the backup directory are hard-linked rather
    /// than copied, so only changed files consume additional space. Returns
    /// metadata for the newly created backup.
    pub fn create_backup(&self) -> Result<BackupInfo, StorageError> {
        let mut guard = self.engine.lock().unwrap();
        // flush=true so in-flight memtable writes are captured.
        self.store
            .with_db(|db| guard.0.create_new_backup_flush(db, true))?;
        guard
            .0
            .get_backup_info()
            .into_iter()
            .max_by_key(|i| i.backup_id)
            .map(BackupInfo::from)
            .ok_or_else(|| StorageError::KeyDecode("no backup found after create".into()))
    }

    /// List all backups with their ID, timestamp, size, and file count.
    pub fn list_backups(&self) -> Result<Vec<BackupInfo>, StorageError> {
        let guard = self.engine.lock().unwrap();
        Ok(guard
            .0
            .get_backup_info()
            .into_iter()
            .map(BackupInfo::from)
            .collect())
    }

    /// Restore backup `backup_id` into `restore_dir`.
    ///
    /// **This is an offline operation.** The server must be stopped before
    /// calling this function. After restore completes, restart `polargraphd`
    /// with `--data-dir <restore_dir>`.
    ///
    /// `restore_dir` is created if it does not already exist.
    pub fn restore_from_backup(
        &self,
        backup_id: u32,
        restore_dir: &Path,
    ) -> Result<(), StorageError> {
        std::fs::create_dir_all(restore_dir)?;
        let mut guard = self.engine.lock().unwrap();
        let opts = RestoreOptions::default();
        // db_dir == wal_dir: RocksDB default places WAL in the data directory.
        guard
            .0
            .restore_from_backup(restore_dir, restore_dir, &opts, backup_id)?;
        Ok(())
    }

    /// Delete all but the `keep_n` most recent backups.
    ///
    /// Returns the number of backups that were deleted.
    pub fn purge_old_backups(&self, keep_n: u32) -> Result<u32, StorageError> {
        let mut guard = self.engine.lock().unwrap();
        let before = guard.0.get_backup_info().len();
        guard.0.purge_old_backups(keep_n as usize)?;
        let after = guard.0.get_backup_info().len();
        Ok(before.saturating_sub(after) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TripleStore, TempDir, TempDir) {
        let data_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();
        let store = TripleStore::open(data_dir.path()).unwrap();
        (store, data_dir, backup_dir)
    }

    #[test]
    fn create_and_list() {
        let (store, _data, backup_dir) = setup();
        let mgr = BackupManager::open(backup_dir.path(), &store).unwrap();

        let info = mgr.create_backup().unwrap();
        assert_eq!(info.backup_id, 1);
        assert!(info.size_bytes > 0);

        let list = mgr.list_backups().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].backup_id, 1);
    }

    #[test]
    fn multiple_backups_incremental() {
        let (store, _data, backup_dir) = setup();
        let mgr = BackupManager::open(backup_dir.path(), &store).unwrap();

        mgr.create_backup().unwrap();
        mgr.create_backup().unwrap();
        let info = mgr.create_backup().unwrap();
        assert_eq!(info.backup_id, 3);

        let list = mgr.list_backups().unwrap();
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn purge_keeps_n_most_recent() {
        let (store, _data, backup_dir) = setup();
        let mgr = BackupManager::open(backup_dir.path(), &store).unwrap();

        mgr.create_backup().unwrap();
        mgr.create_backup().unwrap();
        mgr.create_backup().unwrap();

        let deleted = mgr.purge_old_backups(1).unwrap();
        assert_eq!(deleted, 2);

        let list = mgr.list_backups().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].backup_id, 3);
    }

    #[test]
    fn restore_produces_valid_db_dir() {
        let (store, _data, backup_dir) = setup();
        let mgr = BackupManager::open(backup_dir.path(), &store).unwrap();

        let info = mgr.create_backup().unwrap();

        let restore_dir = TempDir::new().unwrap();
        mgr.restore_from_backup(info.backup_id, restore_dir.path())
            .unwrap();

        // The restored directory must be non-empty and openable as a TripleStore.
        let entries: Vec<_> = std::fs::read_dir(restore_dir.path()).unwrap().collect();
        assert!(
            !entries.is_empty(),
            "restore dir should contain RocksDB files"
        );

        let restored = TripleStore::open(restore_dir.path()).unwrap();
        // A fresh store has no triples — just check the store opens without error.
        let snap = restored.snapshot(restored.begin().read_ts);
        let all = snap.scan_all().unwrap();
        assert!(all.is_empty());
    }
}
