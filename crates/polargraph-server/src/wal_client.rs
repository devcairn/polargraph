//! Replica-side WAL replication client.
//!
//! `WalReplicationClient` connects to the primary's gRPC endpoint, calls
//! `StreamWal`, and applies each incoming write batch to the replica's local
//! RocksDB via `TripleStore::apply_replicated_batch`. The `last_applied_seq`
//! is persisted to the META CF so the stream can resume after a restart.
//!
//! On any connection error the client reconnects with exponential backoff
//! (starting at 1 s, capped at 30 s).  The loop exits cleanly when the
//! supplied `CancellationToken` is cancelled.

use crate::{
    proto::{polar_graph_service_client::PolarGraphServiceClient, StreamWalRequest},
    service::ReplicaState,
};
use polargraph_storage::TripleStore;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{info, warn};

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Runs the WAL replication loop for a replica.
///
/// Returns when the `token` is cancelled.  Call from `tokio::spawn`.
pub async fn run_replication(
    store: TripleStore,
    state: Arc<ReplicaState>,
    token: CancellationToken,
) {
    let primary_address = match store.primary_address() {
        Some(addr) => addr.to_owned(),
        None => {
            warn!("run_replication called on a non-replica store; exiting");
            return;
        }
    };

    let mut backoff = BACKOFF_INITIAL;

    loop {
        if token.is_cancelled() {
            return;
        }

        info!(primary = %primary_address, "WAL replication: connecting to primary");

        match connect_and_stream(&store, &primary_address, &state, &token).await {
            Ok(()) => {
                if token.is_cancelled() {
                    return;
                }
                // Stream ended cleanly (primary shutdown?). Reconnect after backoff.
                warn!("WAL stream ended cleanly; will reconnect");
            }
            Err(e) => {
                if token.is_cancelled() {
                    return;
                }
                warn!("WAL replication error: {e}; reconnecting in {:.1}s", backoff.as_secs_f32());
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = token.cancelled() => { return; }
        }

        // Exponential backoff, capped at BACKOFF_MAX.
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn connect_and_stream(
    store: &TripleStore,
    primary_address: &str,
    state: &ReplicaState,
    token: &CancellationToken,
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

    loop {
        let maybe_entry = tokio::select! {
            msg = stream.message() => msg?,
            _ = token.cancelled() => { return Ok(()); }
        };

        match maybe_entry {
            Some(entry) => {
                store.apply_replicated_batch(entry.sequence_number, &entry.write_batch)?;
                state.record_catchup(entry.sequence_number);
                metrics::gauge!("polargraph_wal_applied_seq").set(entry.sequence_number as f64);
            }
            None => return Ok(()),
        }
    }
}
