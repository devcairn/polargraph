//! Bitemporal timestamps.
//!
//! Every fact carries two independent time axes:
//!
//! - **Valid time** (`vt`): when the fact was true in the real world.
//! - **Transaction time** (`tt`): when we recorded it in the database.
//!
//! This lets you ask:
//! - "What did we know about Alice's role *as of last Tuesday*?"  (rewind tx time)
//! - "What *was* Alice's role during Q3 2024?"                   (query valid time)
//! - "What did we *believe* about Q3 2024 when we ran that audit in January?" (both)

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Microseconds since Unix epoch. Enough resolution for org-scale writes
/// without the overhead of nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Current wall-clock time.
    pub fn now() -> Self {
        let us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock went backwards")
            .as_micros() as i64;
        Self(us)
    }

    /// Sentinel meaning "this fact has no end — it is still current."
    pub const END_OF_TIME: Self = Self(i64::MAX);

    /// Fixed-width 8-byte big-endian representation for sortable index keys.
    #[inline]
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    #[inline]
    pub fn from_be_bytes(b: [u8; 8]) -> Self {
        Self(i64::from_be_bytes(b))
    }
}

/// Bitemporal envelope attached to every asserted fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BiTemporalRange {
    /// Start of the period during which this fact was true in the world.
    pub vt_start: Timestamp,
    /// End of the valid period (`END_OF_TIME` if still current).
    pub vt_end: Timestamp,
    /// Transaction time at which this version was recorded.
    pub tt: Timestamp,
}

impl BiTemporalRange {
    /// Construct an open-ended (still-current) fact recorded right now.
    pub fn assert_now(valid_from: Timestamp) -> Self {
        Self {
            vt_start: valid_from,
            vt_end: Timestamp::END_OF_TIME,
            tt: Timestamp::now(),
        }
    }
}
