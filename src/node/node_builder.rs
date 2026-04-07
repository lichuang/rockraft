use crate::config::Config;
use crate::error::Result;
use crate::node::RaftNode;
use std::sync::Arc;

pub struct RaftNodeBuilder;

impl RaftNodeBuilder {
  /// Build a new RaftNode with the given configuration
  ///
  /// Note: Config is already validated and parsed during deserialization.
  /// All endpoint strings have been converted to typed Endpoints.
  pub async fn build(config: &Config) -> Result<Arc<RaftNode>> {
    let raft_node = RaftNode::create(config).await?;
    RaftNode::start(raft_node.clone()).await?;
    Ok(raft_node)
  }
}
