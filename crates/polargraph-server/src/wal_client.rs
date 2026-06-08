//! Replica-side WAL replication client.
//!
//! `WalReplicationClient` connects to the primary's gRPC endpoint, calls
//! `StreamWal`, and applies each incoming write batch to the replica's local
//! RocksDB via `TripleStore::apply_replicated_batch`. The `last_applied_seq`
//! is persisted to the META CF so the stream can resume after a restart.
//!
//! On any connection error the client reconnects with exponential backoff
//! (starting at 1 s, capped at 30 s).

use crate::{
    proto::{polar_graph_service_client::PolarGraphServiceClient, StreamWalRequest},
    service::ReplicaState,
};
use polargraph_storage::TripleStore;
use std::{sync::Arc, time::Duration};
use tonic::transport::Channel;
use tracing::{info, warn};

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Runs the WAL replication loop for a replica.
///
/// Spawns a tokio task that never returns normally. Call from `tokio::spawn`.
pub async fn run_replication(store: TripleStore, state: Arc<ReplicaState>) {
    let primary_address = match store.primary_address() {
        Some(addr) => addr.to_owned(),
        None => {
            warn!("run_replication called on a non-replica store; exiting");
            return;
        }
    };

    let mut backoff = BACKOFF_INITIAL;

    loop {
        info!(primary = %primary_address, "WAL replication: connecting to primary");

        match connect_and_stream(&store, &primary_address, &state).await {
            Ok(()) => {
                // Stream ended cleanly (primary shutdown?). Reconnect after backoff.
                warn!("WAL stream ended cleanly; will reconnect");
            }
            Err(e) => {
                warn!("WAL replication error: {e}; reconnecting in {:.1}s", backoff.as_secs_f32());
            }
        }

        tokio::time::sleep(backoff).await;
        // Exponential backoff, capped at BACKOFF_MAX.
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn connect_and_stream(
    store: &TripleStore,
    primary_address: &str,
    state: &ReplicaState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = Channel::from_shared(primary_address.to_owned())?
        .connect()
        .await?;

    let mut client = PolarGraphServiceClient::new(channel);

    let since_seq = store.last_applied_seq();
    info!(since_seq, "WAL replication: requesting stream");

    let mut stream = client
        .stream_wal(StreamWalRequest { since_seq })
        .await?
        .into_inner();

    // Reset backoff on successful connection.
    info!("WAL replication: stream established");

    while let Some(entry) = stream.message().await? {
        store.apply_replicated_batch(entry.sequence_number, &entry.write_batch)?;
        state.record_catchup(entry.sequence_number);
    }

    Ok(())
}
