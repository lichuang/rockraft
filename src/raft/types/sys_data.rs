use std::collections::BTreeMap;

use super::StoredMembership;
use super::{LogId, Node, NodeId};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SysData {
  pub last_applied: Option<LogId>,

  pub nodes: BTreeMap<NodeId, Node>,

  /// The last applied membership entry, as applied from `EntryPayload::Membership`.
  /// `None` when the node has only been initialized through `Cmd::AddNode`
  /// (legacy single-node bootstrap path) and has not applied a membership
  /// entry yet.
  pub last_membership: Option<StoredMembership>,
}
