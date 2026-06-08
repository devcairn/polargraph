//! Per-RPC tracing and metrics middleware.
//!
//! [`TelemetryLayer`] is a `tower::Layer` that wraps every gRPC handler. For
//! each request it:
//!
//! - opens an `info_span!` carrying `method` and `peer` fields so that child
//!   log events are automatically annotated,
//! - emits a log event on completion with `status` and `duration_ms`,
//! - increments `polargraph_rpc_requests_total{method, status}`,
//! - records `polargraph_rpc_duration_seconds{method}`.
//!
//! The layer is applied at the transport level via
//! `Server::builder().layer(TelemetryLayer)`.

use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use http::{Request, Response};
use tower::{Layer, Service};
use tracing::Instrument;

// ── Layer ─────────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
pub struct TelemetryLayer;

impl<S> Layer<S> for TelemetryLayer {
    type Service = TelemetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TelemetryService { inner }
    }
}

// ── Service ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct TelemetryService<S> {
    inner: S,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for TelemetryService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display + Send,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        // Use the already-ready instance; replace self with a fresh clone for
        // the next call (standard tower clone-and-swap pattern).
        let clone = self.inner.clone();
        let mut inner = mem::replace(&mut self.inner, clone);

        // Extract gRPC method name from the URI path (last '/' segment).
        let method = req
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap_or("unknown")
            .to_owned();

        // Best-effort peer address from tonic's TCP connect info extension.
        let peer = req
            .extensions()
            .get::<tonic::transport::server::TcpConnectInfo>()
            .and_then(|ci| ci.remote_addr())
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".to_owned());

        let span = tracing::info_span!("rpc", method = %method, peer = %peer);
        let start = Instant::now();

        Box::pin(
            async move {
                let result = inner.call(req).await;
                let elapsed = start.elapsed();
                let duration_ms = elapsed.as_millis() as u64;
                let duration_secs = elapsed.as_secs_f64();

                match &result {
                    Ok(resp) => {
                        // For gRPC trailer-only error responses the grpc-status
                        // header is present in the initial HEADERS frame. For
                        // successful streaming responses it arrives in trailers
                        // (not accessible here), so we default to "ok".
                        let grpc_code = resp
                            .headers()
                            .get("grpc-status")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u32>().ok())
                            .unwrap_or(0);

                        let status = grpc_status_label(grpc_code);

                        metrics::counter!(
                            "polargraph_rpc_requests_total",
                            "method" => method.clone(),
                            "status" => status.to_owned()
                        )
                        .increment(1);
                        metrics::histogram!(
                            "polargraph_rpc_duration_seconds",
                            "method" => method.clone()
                        )
                        .record(duration_secs);

                        if grpc_code == 0 {
                            tracing::info!(status, duration_ms, "completed");
                        } else {
                            tracing::warn!(status, duration_ms, "completed");
                        }
                    }
                    Err(e) => {
                        metrics::counter!(
                            "polargraph_rpc_requests_total",
                            "method" => method.clone(),
                            "status" => "transport_error"
                        )
                        .increment(1);
                        metrics::histogram!(
                            "polargraph_rpc_duration_seconds",
                            "method" => method.clone()
                        )
                        .record(duration_secs);

                        tracing::error!(error = %e, duration_ms, "transport error");
                    }
                }

                result
            }
            .instrument(span),
        )
    }
}

// ── gRPC status code → label ──────────────────────────────────────────────────

fn grpc_status_label(code: u32) -> &'static str {
    match code {
        0 => "ok",
        1 => "cancelled",
        2 => "unknown",
        3 => "invalid_argument",
        4 => "deadline_exceeded",
        5 => "not_found",
        6 => "already_exists",
        7 => "permission_denied",
        8 => "resource_exhausted",
        9 => "failed_precondition",
        10 => "aborted",
        11 => "out_of_range",
        12 => "unimplemented",
        13 => "internal",
        14 => "unavailable",
        15 => "data_loss",
        16 => "unauthenticated",
        _ => "unknown",
    }
}
