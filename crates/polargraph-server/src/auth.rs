//! API key authentication middleware for the PolarGraph gRPC server.
//!
//! `ApiKeyLayer` is a tower `Layer` applied at the transport level. It inspects
//! the `Authorization` metadata header on every inbound request and uses
//! constant-time comparison (`subtle::ConstantTimeEq`) to prevent timing attacks.
//!
//! Accepted formats: `Bearer <key>` or `ApiKey <key>`.
//!
//! # Exemptions
//!
//! `/polargraph.v1.PolarGraphService/ReplicaStatus` is always allowed through so
//! load-balancers can health-probe the instance without possessing a key.
//!
//! # Disabled mode
//!
//! When no keys are configured (`ApiKeyLayer::disabled()` / empty list), every
//! request passes through and a warning is logged once at startup.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use http::{Request, Response};
use subtle::ConstantTimeEq;
use tonic::{body::BoxBody, Status};
use tower::{Layer, Service};

/// gRPC method paths that bypass authentication.
const EXEMPT_PATHS: &[&str] = &["/polargraph.v1.PolarGraphService/ReplicaStatus"];

/// Shared, mutable API key list. Cloned into every request handler and the
/// management service so keys added/revoked take effect on the next request.
pub type KeyStore = Arc<RwLock<Vec<String>>>;

// ── Layer ─────────────────────────────────────────────────────────────────────

/// Tower layer that enforces API-key authentication on inbound gRPC requests.
#[derive(Clone)]
pub struct ApiKeyLayer {
    keys: KeyStore,
}

impl ApiKeyLayer {
    /// Create a layer from `keys`. Returns both the layer and a shared
    /// `KeyStore` handle so callers can add/revoke keys at runtime.
    pub fn new(keys: Vec<String>) -> (Self, KeyStore) {
        let store: KeyStore = Arc::new(RwLock::new(keys));
        (
            Self {
                keys: Arc::clone(&store),
            },
            store,
        )
    }

    /// Create a disabled layer — all requests pass through. No `KeyStore` is
    /// returned because auth is off; use [`Self::new`] with an empty vec when
    /// you want a handle to later enable auth.
    pub fn disabled() -> Self {
        Self {
            keys: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Returns `true` when at least one key is configured.
    pub fn is_enabled(&self) -> bool {
        !self.keys.read().unwrap().is_empty()
    }
}

impl<S> Layer<S> for ApiKeyLayer {
    type Service = ApiKeyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ApiKeyService {
            inner,
            keys: Arc::clone(&self.keys),
        }
    }
}

// ── Service ───────────────────────────────────────────────────────────────────

/// Tower service that enforces API-key auth; produced by [`ApiKeyLayer`].
#[derive(Clone)]
pub struct ApiKeyService<S> {
    inner: S,
    keys: KeyStore,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

impl<S, B> Service<Request<B>> for ApiKeyService<S>
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
        // Swap inner so the ready reservation stays on the clone that will be
        // driven to completion; self.inner becomes the not-yet-ready stand-in.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let path = req.uri().path().to_owned();
        let keys = Arc::clone(&self.keys);

        Box::pin(async move {
            // Snapshot the key list under a brief read lock, then drop the guard
            // before calling into the inner service.
            let key_snapshot: Vec<String> = keys.read().unwrap().clone();

            // Auth disabled — pass through.
            if key_snapshot.is_empty() {
                return inner.call(req).await;
            }

            // Exempt methods bypass auth.
            if EXEMPT_PATHS.iter().any(|p| path == *p) {
                return inner.call(req).await;
            }

            // Validate the Authorization header.
            if !check_bearer_auth(req.headers(), &key_snapshot) {
                return Ok(Status::unauthenticated("missing or invalid api key").to_http());
            }

            inner.call(req).await
        })
    }
}

// ── Key comparison ────────────────────────────────────────────────────────────

/// Returns `true` when the `Authorization` header carries a recognized key.
pub(crate) fn check_bearer_auth(headers: &http::HeaderMap, keys: &[String]) -> bool {
    let Some(auth_header) = headers.get("authorization") else {
        return false;
    };
    let Ok(value) = auth_header.to_str() else {
        return false;
    };

    let key = if let Some(k) = value.strip_prefix("Bearer ") {
        k
    } else if let Some(k) = value.strip_prefix("ApiKey ") {
        k
    } else {
        return false;
    };

    keys.iter().any(|valid| key_matches_ct(key, valid))
}

/// Constant-time string equality to prevent timing-oracle attacks.
fn key_matches_ct(provided: &str, valid: &str) -> bool {
    if provided.len() != valid.len() {
        return false;
    }
    bool::from(provided.as_bytes().ct_eq(valid.as_bytes()))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    fn headers_with_auth(value: &str) -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert("authorization", value.parse().unwrap());
        map
    }

    #[test]
    fn bearer_prefix_accepted() {
        let keys = vec!["secret".to_string()];
        assert!(check_bearer_auth(
            &headers_with_auth("Bearer secret"),
            &keys
        ));
    }

    #[test]
    fn apikey_prefix_accepted() {
        let keys = vec!["secret".to_string()];
        assert!(check_bearer_auth(
            &headers_with_auth("ApiKey secret"),
            &keys
        ));
    }

    #[test]
    fn wrong_key_rejected() {
        let keys = vec!["secret".to_string()];
        assert!(!check_bearer_auth(
            &headers_with_auth("Bearer wrong"),
            &keys
        ));
    }

    #[test]
    fn missing_header_rejected() {
        let keys = vec!["secret".to_string()];
        assert!(!check_bearer_auth(&HeaderMap::new(), &keys));
    }

    #[test]
    fn any_key_in_list_accepted() {
        let keys = vec!["key-a".to_string(), "key-b".to_string()];
        assert!(check_bearer_auth(&headers_with_auth("Bearer key-a"), &keys));
        assert!(check_bearer_auth(&headers_with_auth("Bearer key-b"), &keys));
        assert!(!check_bearer_auth(
            &headers_with_auth("Bearer key-c"),
            &keys
        ));
    }

    #[test]
    fn constant_time_different_lengths_rejected() {
        assert!(!key_matches_ct("short", "longer-key"));
        assert!(!key_matches_ct("", "x"));
    }

    #[test]
    fn layer_is_enabled_reflects_key_list() {
        assert!(ApiKeyLayer::new(vec!["k".into()]).0.is_enabled());
        assert!(!ApiKeyLayer::disabled().is_enabled());
    }
}
