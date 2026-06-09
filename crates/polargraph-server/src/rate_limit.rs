//! Per-client token-bucket rate limiting middleware for the PolarGraph gRPC server.
//!
//! `RateLimitLayer` is a tower `Layer` applied at the transport level. It tracks
//! a token bucket per client IP address and rejects requests with
//! `ResourceExhausted` when the bucket runs dry.
//!
//! # Client IP resolution
//!
//! 1. `x-forwarded-for` header (leftmost IP — original client behind a proxy)
//! 2. Tonic's `TcpConnectInfo` extension (direct TCP peer address)
//! 3. `0.0.0.0` sentinel (fallback; all such clients share one bucket)
//!
//! # Exemptions
//!
//! `/polargraph.v1.PolarGraphService/ReplicaStatus` is always allowed through
//! so load-balancers can health-probe the instance without consuming quota.
//!
//! # Disabled mode
//!
//! When `max_rps == 0`, the layer is a no-op pass-through.
//!
//! # Stale-entry cleanup
//!
//! Every 1 000 requests the layer scans the bucket map and removes entries that
//! have not received a request in the last 60 seconds, bounding memory growth
//! for transient clients.

use dashmap::DashMap;
use http::{Request, Response};
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tonic::{body::BoxBody, Status};
use tower::{Layer, Service};

/// gRPC method paths that bypass rate limiting.
const EXEMPT_PATHS: &[&str] = &["/polargraph.v1.PolarGraphService/ReplicaStatus"];

/// Remove buckets idle for longer than this.
const STALE_SECS: u64 = 60;

/// Run cleanup once every this many requests.
const CLEANUP_INTERVAL: u64 = 1_000;

// ── Layer ─────────────────────────────────────────────────────────────────────

/// Tower layer that enforces per-client-IP token-bucket rate limiting.
#[derive(Clone)]
pub struct RateLimitLayer {
    max_rps: u32,
    state: Arc<RateLimitState>,
}

struct RateLimitState {
    buckets: DashMap<IpAddr, TokenBucket>,
    request_count: AtomicU64,
}

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimitLayer {
    /// Create a layer that allows at most `max_rps` requests per second per client.
    pub fn new(max_rps: u32) -> Self {
        Self {
            max_rps,
            state: Arc::new(RateLimitState {
                buckets: DashMap::new(),
                request_count: AtomicU64::new(0),
            }),
        }
    }

    /// Create a disabled (pass-through) layer.
    pub fn disabled() -> Self {
        Self::new(0)
    }

    /// Returns `true` when rate limiting is active.
    pub fn is_enabled(&self) -> bool {
        self.max_rps > 0
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitService { inner, max_rps: self.max_rps, state: Arc::clone(&self.state) }
    }
}

// ── Service ───────────────────────────────────────────────────────────────────

/// Tower service produced by [`RateLimitLayer`].
#[derive(Clone)]
pub struct RateLimitService<S> {
    inner: S,
    max_rps: u32,
    state: Arc<RateLimitState>,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

impl<S, B> Service<Request<B>> for RateLimitService<S>
where
    S: Service<Request<B>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let max_rps = self.max_rps;
        let state = Arc::clone(&self.state);

        Box::pin(async move {
            if max_rps == 0 {
                return inner.call(req).await;
            }

            if EXEMPT_PATHS.iter().any(|p| req.uri().path() == *p) {
                return inner.call(req).await;
            }

            let client_ip = extract_client_ip(&req);

            // Lazy cleanup every CLEANUP_INTERVAL requests.
            let count = state.request_count.fetch_add(1, Ordering::Relaxed);
            if count % CLEANUP_INTERVAL == 0 && count > 0 {
                let cutoff = Instant::now() - Duration::from_secs(STALE_SECS);
                state.buckets.retain(|_, b| b.last_refill > cutoff);
            }

            let allowed = {
                let mut bucket = state.buckets.entry(client_ip).or_insert_with(|| TokenBucket {
                    tokens: max_rps as f64,
                    last_refill: Instant::now(),
                });

                let now = Instant::now();
                let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
                bucket.tokens =
                    (bucket.tokens + elapsed * max_rps as f64).min(max_rps as f64);
                bucket.last_refill = now;

                if bucket.tokens >= 1.0 {
                    bucket.tokens -= 1.0;
                    true
                } else {
                    false
                }
            };

            if !allowed {
                return Ok(Status::resource_exhausted("rate limit exceeded").to_http());
            }

            inner.call(req).await
        })
    }
}

// ── IP extraction ─────────────────────────────────────────────────────────────

fn extract_client_ip<B>(req: &Request<B>) -> IpAddr {
    // x-forwarded-for: leftmost address is the original client.
    if let Some(fwd) = req.headers().get("x-forwarded-for") {
        if let Ok(val) = fwd.to_str() {
            if let Some(first) = val.split(',').next() {
                if let Ok(ip) = first.trim().parse::<IpAddr>() {
                    return ip;
                }
            }
        }
    }

    // Direct TCP peer from tonic's connection extension.
    if let Some(info) = req.extensions().get::<tonic::transport::server::TcpConnectInfo>() {
        if let Some(addr) = info.remote_addr() {
            return addr.ip();
        }
    }

    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state(_max_rps: u32) -> Arc<RateLimitState> {
        Arc::new(RateLimitState {
            buckets: DashMap::new(),
            request_count: AtomicU64::new(0),
        })
    }

    fn consume_bucket(state: &Arc<RateLimitState>, ip: IpAddr, max_rps: u32) -> bool {
        let mut bucket = state.buckets.entry(ip).or_insert_with(|| TokenBucket {
            tokens: max_rps as f64,
            last_refill: Instant::now(),
        });
        let now = Instant::now();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * max_rps as f64).min(max_rps as f64);
        bucket.last_refill = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    #[test]
    fn bucket_allows_up_to_max_rps_then_rejects() {
        let max_rps = 5u32;
        let state = make_state(max_rps);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        for _ in 0..max_rps {
            assert!(consume_bucket(&state, ip, max_rps), "should be allowed");
        }
        assert!(!consume_bucket(&state, ip, max_rps), "should be rejected after quota exhausted");
    }

    #[test]
    fn two_ips_have_independent_buckets() {
        let max_rps = 2u32;
        let state = make_state(max_rps);
        let ip_a: IpAddr = "10.0.0.1".parse().unwrap();
        let ip_b: IpAddr = "10.0.0.2".parse().unwrap();

        // Exhaust A's bucket.
        for _ in 0..max_rps {
            assert!(consume_bucket(&state, ip_a, max_rps));
        }
        assert!(!consume_bucket(&state, ip_a, max_rps), "A should be exhausted");

        // B is independent — should still have full quota.
        for _ in 0..max_rps {
            assert!(consume_bucket(&state, ip_b, max_rps), "B should still be allowed");
        }
    }

    #[test]
    fn layer_is_enabled_only_when_max_rps_nonzero() {
        assert!(RateLimitLayer::new(10).is_enabled());
        assert!(!RateLimitLayer::disabled().is_enabled());
        assert!(!RateLimitLayer::new(0).is_enabled());
    }

    #[test]
    fn tokens_refill_over_time() {
        let max_rps = 1u32;
        let state = make_state(max_rps);
        let ip: IpAddr = "10.0.0.3".parse().unwrap();

        // Exhaust the single token.
        assert!(consume_bucket(&state, ip, max_rps));
        assert!(!consume_bucket(&state, ip, max_rps), "should be throttled");

        // Simulate 1.1s of elapsed time by backdating last_refill.
        {
            let mut bucket = state.buckets.get_mut(&ip).unwrap();
            bucket.last_refill = Instant::now() - Duration::from_millis(1_100);
        }

        // After simulated refill, one token should be available again.
        assert!(consume_bucket(&state, ip, max_rps), "should be refilled after 1s");
    }
}
