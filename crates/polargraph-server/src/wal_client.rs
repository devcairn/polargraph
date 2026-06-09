//! Replica-side WAL replication client.
//!
//! Connects to the primary's gRPC endpoint, calls `StreamWal`, and applies
//! each incoming write batch via `TripleStore::apply_replicated_batch`.
//! The `last_applied_seq` is persisted to the META CF so the stream can
//! resume after a restart.
//!
//! Reconnects with exponential backoff (1 s → 30 s) on any error.
//! Exits cleanly when the supplied `CancellationToken` is cancelled.

use crate::{
    proto::{polar_graph_service_client::PolarGraphServiceClient, StreamWalRequest},
    service::ReplicaState,
};
use polargraph_storage::TripleStore;
use std::{sync::{atomic::Ordering, Arc}, time::Duration};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic_health::{server::HealthReporter, ServingStatus};
use tracing::{info, warn};

const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const HEALTH_SVC: &str = "polargraph.v1.PolarGraphService";

/// Runs the WAL replication loop for a replica.
///
/// - `health` — optional health reporter; toggles the gRPC health status
///   between `NotServing` (reconnecting) and `Serving` (stream active).
/// - `tls_ca_pem` — when `Some`, the bytes are used as the PEM CA certificate
///   for verifying the primary's TLS certificate. When `None`, no TLS.
///
/// Returns when `token` is cancelled.  Call from `tokio::spawn`.
pub async fn run_replication(
    store: TripleStore,
    state: Arc<ReplicaState>,
    token: CancellationToken,
    mut health: Option<HealthReporter>,
    tls_ca_pem: Option<Vec<u8>>,
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

        // Mark disconnected while attempting to (re)connect.
        state.connected.store(false, Ordering::Relaxed);
        if let Some(ref mut h) = health {
            h.set_service_status(HEALTH_SVC, ServingStatus::NotServing).await;
        }

        info!(primary = %primary_address, "WAL replication: connecting to primary");

        match connect_and_stream(&store, &primary_address, &state, &token, &tls_ca_pem, &mut health).await {
            Ok(()) => {
                if token.is_cancelled() {
                    return;
                }
                warn!("WAL stream ended cleanly; will reconnect");
            }
            Err(e) => {
                if token.is_cancelled() {
                    return;
                }
                warn!("WAL replication error: {e}; reconnecting in {:.1}s", backoff.as_secs_f32());
            }
        }

        state.connected.store(false, Ordering::Relaxed);

        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = token.cancelled() => { return; }
        }

        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

async fn connect_and_stream(
    store: &TripleStore,
    primary_address: &str,
    state: &ReplicaState,
    token: &CancellationToken,
    tls_ca_pem: &Option<Vec<u8>>,
    health: &mut Option<HealthReporter>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = Channel::from_shared(primary_address.to_owned())?;

    let channel = if let Some(ca_pem) = tls_ca_pem {
        let ca_cert = tonic::transport::Certificate::from_pem(ca_pem);
        let tls = tonic::transport::ClientTlsConfig::new().ca_certificate(ca_cert);
        endpoint.tls_config(tls)?.connect().await?
    } else {
        endpoint.connect().await?
    };

    let mut client = PolarGraphServiceClient::new(channel);

    let since_seq = store.last_applied_seq();
    info!(since_seq, "WAL replication: requesting stream");

    let mut stream = client
        .stream_wal(StreamWalRequest { since_seq })
        .await?
        .into_inner();

    // Stream established — mark connected and report Serving.
    state.connected.store(true, Ordering::Relaxed);
    if let Some(ref mut h) = health {
        h.set_service_status(HEALTH_SVC, ServingStatus::Serving).await;
    }
    info!("WAL replication: stream established");

    // Reset backoff happens implicitly because next connect attempt starts fresh.

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
