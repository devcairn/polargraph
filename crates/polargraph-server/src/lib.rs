//! Library target — exposes service, proto types, and conversions for tests
//! and future embedding use cases.

pub mod auth;
pub mod config;
pub mod convert;
pub mod rate_limit;
pub mod retention_scheduler;
pub mod service;
pub mod telemetry;
pub mod ui_api;
pub mod wal_client;

#[allow(clippy::enum_variant_names)]
pub mod proto {
    tonic::include_proto!("polargraph.v1");
}
