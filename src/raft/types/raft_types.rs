use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;

use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::MembershipOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;

use super::AppliedState;
use super::log_entry::LogEntry;
use crate::config::Endpoint;

pub type SnapshotData = tokio::fs::File;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
  pub node_id: NodeId,
  pub endpoint: Endpoint,
}

impl Display for Node {
  fn fmt(&self, f: &mut Formatter) -> FmtResult {
    write!(f, "{}={}", self.node_id, self.endpoint)
  }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct KeyValue {
  pub key: Bytes,
  pub value: Bytes,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = LogEntry,
        R = AppliedState,
        NodeId = NodeId,
        Node = Node
);

pub type Entry = EntryOf<TypeConfig>;
pub type LogState = openraft::storage::LogState<TypeConfig>;
pub type LogId = LogIdOf<TypeConfig>;
pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;
pub type Membership = MembershipOf<TypeConfig>;

pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
pub type StoredMembership = StoredMembershipOf<TypeConfig>;
/// Snapshot as exposed by this crate: the openraft metadata plus the data file.
pub type Snapshot = SnapshotOf<TypeConfig, SnapshotData>;
pub type SnapshotMeta = SnapshotMetaOf<TypeConfig>;
