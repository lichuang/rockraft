use crate::error::OpenRaft;
use crate::error::Result;
use crate::error::RockRaftError;
use crate::node::RaftNode;
use crate::raft::types::AppliedState;
use crate::raft::types::Cmd;
use crate::raft::types::ForwardRequestBody;
use crate::raft::types::ForwardResponse;
use crate::raft::types::GetKVReq;
use crate::raft::types::JoinRequest;
use crate::raft::types::LogEntry;
use crate::raft::types::Node;
use crate::raft::types::TypeConfig;
use openraft::Raft;
use std::collections::BTreeSet;
use std::sync::Arc;
use tonic::Status;
use tracing::debug;
use tracing::error;

/// LeaderHandler provides methods that can only be called on a leader node
pub struct LeaderHandler<'a> {
  node: &'a RaftNode,
}

impl<'a> LeaderHandler<'a> {
  /// Create a new LeaderHandler from a RaftNode reference
  pub fn new(node: &'a RaftNode) -> Self {
    Self { node }
  }

  /// Get a reference to the underlying RaftNode
  pub fn node(&self) -> &RaftNode {
    self.node
  }

  /// Get a reference to the underlying Raft instance
  pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
    self.node.raft()
  }

  /// Handle a forward request body
  ///
  /// This function handles different types of forward requests based on the body type.
  /// It can only be called on the leader node.
  ///
  /// # Arguments
  /// * `body` - The ForwardRequestBody to handle
  ///
  /// # Returns
  /// * `Ok(ForwardResponse)` - The response to the request
  /// * `Err(Status)` - If the operation failed
  pub async fn handle(
    &self,
    body: ForwardRequestBody,
  ) -> std::result::Result<ForwardResponse, Status> {
    match body {
      ForwardRequestBody::Join(req) => self.handle_join(req).await,
      ForwardRequestBody::Write(entry) => self.handle_write(entry).await,
      ForwardRequestBody::GetKV(req) => self.handle_get_kv(req).await,
    }
  }

  /// Handle join request
  ///
  /// This function adds a node to the raft cluster. It first writes a log entry
  /// to add the node, then changes the membership to include the new node as a voter.
  async fn handle_join(&self, req: JoinRequest) -> std::result::Result<ForwardResponse, Status> {
    let node_id = req.node_id;

    // Get current membership and check if node already exists
    let metrics = self.raft().metrics().borrow().clone();
    let membership = metrics.membership_config.membership();

    let voters: BTreeSet<u64> = membership.voter_ids().collect();
    if voters.contains(&node_id) {
      return Ok(ForwardResponse::Join(()));
    }

    let node = Node {
      node_id,
      endpoint: req.endpoint.clone(),
    };

    // Write a log entry to add the node
    let entry = LogEntry::new(Cmd::AddNode {
      node: node.clone(),
      overriding: false,
    });
    
    if let Err(e) = self.write(entry).await {
      error!("Failed to join node: {:?}", e);
      return Err(Status::internal(format!("Failed to join node: {}", e)));
    }

    // Change membership to add the new node as voter (retain removed voters as learners)
    let mut add_voters: BTreeSet<u64> = BTreeSet::new();
    add_voters.insert(node_id);

    if let Err(e) = self
      .raft()
      .change_membership(add_voters, false)
      .await
    {
      error!("Failed to join node: {:?}", e);
      return Err(Status::internal(format!("Failed to join node: {}", e)));
    }

    Ok(ForwardResponse::Join(()))
  }

  /// Handle write request
  async fn handle_write(&self, entry: LogEntry) -> std::result::Result<ForwardResponse, Status> {
    match self.write(entry).await {
      Ok(applied_state) => Ok(ForwardResponse::Write(applied_state)),
      Err(e) => {
        error!("Failed to write log entry: {:?}", e);
        Err(Status::internal(format!(
          "Failed to write log entry: {}",
          e
        )))
      }
    }
  }

  /// Handle get_kv request
  async fn handle_get_kv(&self, req: GetKVReq) -> std::result::Result<ForwardResponse, Status> {
    match self.node.state_machine().get_kv(&req.key) {
      Ok(value) => Ok(ForwardResponse::GetKV(value)),
      Err(e) => {
        error!("Failed to get kv: {:?}", e);
        Err(Status::internal(format!("Failed to get kv: {}", e)))
      }
    }
  }

  /// Write a log entry to the raft cluster
  ///
  /// This function writes a LogEntry to the raft log and waits for it to be applied.
  /// It can only be called on the leader node.
  ///
  /// The `time_ms` field of the entry will be set to the current timestamp before writing.
  ///
  /// # Arguments
  /// * `entry` - The LogEntry to write
  ///
  /// # Returns
  /// * `Ok(AppliedState)` - The result of applying the log entry
  /// * `Err(RockRaftError)` - If the operation failed or this node is not the leader
  pub async fn write(&self, mut entry: LogEntry) -> Result<AppliedState> {
    // Set the current timestamp in milliseconds, safe to unwrap
    let now = std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .unwrap_or_default()
      .as_millis() as u64;
    entry.time_ms = Some(now);

    let node_id = self.raft().node_id();

    match self.raft().client_write(entry).await {
      Ok(response) => {
        debug!(
          node_id = %node_id,
          log_id = %response.log_id,
          "Successfully wrote log entry"
        );
        Ok(response.data)
      }
      Err(e) => {
        error!(
          node_id = %node_id,
          error = %e,
          "Failed to write log entry"
        );
        match e {
          openraft::error::RaftError::APIError(api_err) => {
            Err(RockRaftError::OpenRaft(OpenRaft::ClientWrite(api_err)))
          }
          openraft::error::RaftError::Fatal(fatal_err) => {
            Err(RockRaftError::OpenRaft(OpenRaft::Fatal(fatal_err)))
          }
        }
      }
    }
  }
}
