//! Library target — exposes service, proto types, and conversions for tests
//! and future embedding use cases.

pub mod convert;
pub mod service;

pub mod proto {
    tonic::include_proto!("polargraph.v1");
}
