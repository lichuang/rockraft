use std::fmt;

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

impl fmt::Display for Cmd {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Cmd::AddNode { node, overriding } => {
        if *overriding {
          write!(f, "add_node(override):{}", node)
        } else {
          write!(f, "add_node(no-override):{}", node)
        }
      }
      Cmd::RemoveNode { node_id } => {
        write!(f, "remove_node:{}", node_id)
      }

      Cmd::UpsertKV(upsert_kv) => {
        write!(f, "upsert_kv:{}", upsert_kv)
      }
    }
  }
}
