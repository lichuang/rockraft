use core::fmt;

use bytes::Bytes;
use serde::Deserialize;
use serde::Serialize;

use super::cmd::Cmd;

pub type SnapshotData = tokio::fs::File;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppResponseData {
  pub value: Option<Bytes>,
}

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RaftNode {
  pub node_id: u64,
  pub rpc_addr: String,
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
        Node = RaftNode,
        SnapshotData = SnapshotData
);

pub type Entry = openraft::Entry<TypeConfig>;
pub type LogState = openraft::storage::LogState<TypeConfig>;
pub type LogId = openraft::LogId<TypeConfig>;
pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;
pub type Node = <TypeConfig as openraft::RaftTypeConfig>::Node;
pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
pub type StoredMembership = openraft::StoredMembership<TypeConfig>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
