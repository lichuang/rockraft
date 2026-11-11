use openraft::alias::VoteOf;
use serde::Deserialize;
use serde::Serialize;

use crate::raft::types::SnapshotMeta;
use crate::raft::types::TypeConfig;

/// One chunk of a snapshot transfer.
///
/// OpenRaft 0.10 removed the chunked `InstallSnapshotRequest` (it moved to
/// `openraft-legacy`) and its `SnapshotMeta` no longer carries a `snapshot_id`.
/// The gRPC transport therefore frames a transfer itself: the sender picks a
/// `transfer_id`, and the receiver keys the assembled file by it. The id must
/// stay a plain file-name component (ASCII alphanumerics, `-`, `_`, `.`).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SnapshotChunk {
  /// The leader's vote for this transfer.
  pub vote: VoteOf<TypeConfig>,

  /// Metadata of the snapshot being transferred.
  pub meta: SnapshotMeta,

  /// Identifier of this transfer, chosen by the sender.
  pub transfer_id: String,

  /// Offset of `data` in the assembled snapshot file.
  pub offset: u64,

  /// Chunk payload.
  pub data: Vec<u8>,

  /// Whether this is the final chunk.
  pub done: bool,
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::raft::types::StoredMembership;
  use crate::raft::types::decode;
  use crate::raft::types::encode;
  use crate::utils::test::create_log_id;
  use openraft::Vote;

  /// The chunk is what crosses the wire; postcard must round-trip the vote,
  /// the (id-less) snapshot metadata and the payload together.
  #[test]
  fn test_snapshot_chunk_roundtrips_through_postcard() {
    let chunk = SnapshotChunk {
      vote: Vote::new_committed(2, 1),
      meta: SnapshotMeta {
        last_log_id: Some(create_log_id(2, 1, 9)),
        last_membership: StoredMembership::default(),
      },
      transfer_id: "T2-N1-0".to_string(),
      offset: 7,
      data: vec![1, 2, 3, 4],
      done: true,
    };

    let encoded = encode(&chunk).unwrap();
    let decoded: SnapshotChunk = decode(&encoded).unwrap();

    assert_eq!(decoded, chunk);
  }
}
