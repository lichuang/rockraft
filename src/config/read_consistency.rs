//! Read consistency modes for the KV read path.
//!
//! The four levels trade cost against staleness. The system default is
//! [`ReadConsistency::LeaderApplied`], which preserves the historical
//! behavior; stronger levels are opt-in per node.

use serde::{Deserialize, Serialize};
use std::fmt;

/// How reads are served. Weaker levels are cheaper; stronger levels wait for
/// leadership confirmation and state-machine catch-up.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
  /// Forward the read to the leader, then read the leader's applied state.
  ///
  /// Reads serialize through one node and only ever see committed data, but
  /// a partitioned leader that does not know it was deposed can serve stale
  /// values, and a just-committed write can be missed until it is applied.
  /// This is the default and the cheapest forwarded mode.
  #[default]
  LeaderApplied,

  /// Answer from this node's own applied state, never forwarding.
  ///
  /// Same staleness semantics as `LeaderApplied`, plus the local view can
  /// lag the leader by any number of committed entries. Useful for
  /// read-heavy workloads that tolerate staleness.
  LocalApplied,

  /// Leader confirms its lease (election-timeout window) instead of
  /// contacting a quorum, then reads the applied state.
  ///
  /// Linearizable *assuming clocks drift less than the lease bound*. Much
  /// cheaper than `ReadIndex` since no round-trip to followers happens.
  LeaseRead,

  /// Leader confirms leadership with a majority via the Raft ReadIndex
  /// protocol, waits for the state machine to apply up to that position,
  /// then reads.
  ///
  /// Unconditionally linearizable. Each read pays one quorum round-trip;
  /// concurrent reads can share one confirmation.
  ReadIndex,
}

impl fmt::Display for ReadConsistency {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ReadConsistency::LeaderApplied => write!(f, "leader_applied"),
      ReadConsistency::LocalApplied => write!(f, "local_applied"),
      ReadConsistency::LeaseRead => write!(f, "lease_read"),
      ReadConsistency::ReadIndex => write!(f, "read_index"),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_serde_roundtrip_via_postcard_codec() {
    use crate::raft::types::{decode, encode};

    for value in [
      ReadConsistency::LeaderApplied,
      ReadConsistency::LocalApplied,
      ReadConsistency::LeaseRead,
      ReadConsistency::ReadIndex,
    ] {
      let decoded: ReadConsistency = decode(&encode(&value).unwrap()).unwrap();
      assert_eq!(decoded, value);
    }
  }

  #[test]
  fn test_default_is_leader_applied() {
    assert_eq!(ReadConsistency::default(), ReadConsistency::LeaderApplied);
  }
}
