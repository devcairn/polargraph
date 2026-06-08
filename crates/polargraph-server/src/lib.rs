//! Library target — exposes service, proto types, and conversions for tests
//! and future embedding use cases.

pub mod auth;
pub mod convert;
pub mod service;
pub mod telemetry;
pub mod ui_api;
pub mod wal_client;

pub mod proto {
    tonic::include_proto!("polargraph.v1");
}
