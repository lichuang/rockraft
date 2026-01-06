use serde::{Deserialize, Serialize};

use crate::raft::types::{NodeId, node::Node};

use super::upsert_kv::UpsertKV;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
  /// Add node if absent
  AddNode {
    node: Node,
    /// Whether to override existing record.
    #[serde(default)]
    overriding: bool,
  },

  /// Remove node
  RemoveNode { node_id: NodeId },

  /// Update or insert a general purpose kv store
  UpsertKV(UpsertKV),
}
