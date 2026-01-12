use core::fmt;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

use super::cmd::Cmd;
use super::endpoint::Endpoint;

pub type SnapshotData = tokio::fs::File;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppResponseData {
  pub value: Option<Bytes>,
}

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
  pub node_id: u64,
  pub endpoint: Endpoint,
}

impl std::fmt::Display for Node {
  fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
        D = Cmd,
        R = AppResponseData,
        NodeId = NodeId,
        Node = Node,
        SnapshotData = SnapshotData
);

pub type Entry = openraft::Entry<TypeConfig>;
pub type LogState = openraft::storage::LogState<TypeConfig>;
pub type LogId = openraft::LogId<TypeConfig>;
pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;

pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
pub type StoredMembership = openraft::StoredMembership<TypeConfig>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
