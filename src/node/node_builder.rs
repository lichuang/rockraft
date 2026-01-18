use crate::config::Config;
use crate::error::Result;
use crate::node::RaftNode;
use std::sync::Arc;

pub struct RaftNodeBuilder;

impl RaftNodeBuilder {
  /// Build a new RaftNode with the given configuration
  pub async fn build(config: &Config) -> Result<Arc<RaftNode>> {
    config.validate()?;

    let raft_node = RaftNode::create(config).await?;
    RaftNode::start(raft_node.clone(), config).await?;
    Ok(raft_node)
  }
}
