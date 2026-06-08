//! Primary-side WAL streaming.
//!
//! `WalStreamer` tails the RocksDB write-ahead log from a given sequence
//! number and delivers entries into a tokio mpsc channel via `blocking_send`.
//! Intended to be executed inside `tokio::task::spawn_blocking`.

use crate::store::TripleStore;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// A single WAL entry ready for shipping to a replica.
pub struct WalEntry {
    pub sequence_number: u64,
    /// Raw serialised `rocksdb::WriteBatch` bytes.
    pub write_batch: Vec<u8>,
}

/// Streams WAL entries from a `TripleStore` into a channel.
pub struct WalStreamer {
    store: TripleStore,
}

impl WalStreamer {
    pub fn new(store: TripleStore) -> Self {
        Self { store }
    }

    /// Tail the WAL from `since_seq`, sending every entry into `tx`.
    ///
    /// After draining all existing entries the loop polls every 50 ms for new
    /// ones. Returns when `tx` is closed (client disconnected). Uses
    /// `blocking_send` — call this from `tokio::task::spawn_blocking`.
    pub fn run(self, since_seq: u64, tx: mpsc::Sender<WalEntry>) {
        let store = self.store;
        let mut current_seq = since_seq;

        loop {
            if tx.is_closed() {
                return;
            }

            let db = store.db_ref();
            match db.get_updates_since(current_seq) {
                Ok(wal_iter) => {
                    let mut sent = 0u64;
                    for item in wal_iter {
                        if tx.is_closed() {
                            return;
                        }
                        match item {
                            Ok((seq, batch)) => {
                                debug!(seq, "streaming WAL entry");
                                let entry = WalEntry {
                                    sequence_number: seq,
                                    write_batch: batch.data().to_vec(),
                                };
                                if tx.blocking_send(entry).is_err() {
                                    return;
                                }
                                current_seq = seq + 1;
                                sent += 1;
                            }
                            Err(e) => {
                                warn!("WAL iterator error at seq {current_seq}: {e}");
                                break;
                            }
                        }
                    }
                    if sent == 0 {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
                Err(e) => {
                    warn!("get_updates_since({current_seq}) failed: {e}");
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
}
