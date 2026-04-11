//! Forwarding infrastructure for RaftNode.
//!
//! Handles leader detection, request forwarding to remote leaders,
//! and retry logic for leader-redirected operations.

use std::result::Result as StdResult;
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use tokio::time::{sleep, timeout};
use tracing::debug;

use crate::error::{Error, Result};
use crate::grpc::JoinConnectionFactory;
use crate::raft::protobuf::raft_service_client::RaftServiceClient;
use crate::raft::types::{
  ForwardRequest, ForwardResponse, ForwardToLeader, NodeId, RequestPayload, decode,
};

use super::LeaderHandler;
use super::node::RaftNode;

/// Retry configuration for forward operations
const MAX_RETRIES: u32 = 20;
const RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(200);
const RETRY_MAX_INTERVAL: Duration = Duration::from_secs(3);

impl RaftNode {
  /// Get the current leader node ID, waiting up to a deadline.
  ///
  /// Returns `Ok(Some(leader_id))` if a leader is found within the deadline,
  /// `Ok(None)` if no leader is found before timeout.
  pub(crate) async fn get_leader(&self) -> Result<Option<NodeId>> {
    let deadline = Duration::from_millis(2000);
    let mut metrics_rx = self.raft().metrics();

    let result = timeout(deadline, async {
      loop {
        if let Some(leader) = metrics_rx.borrow_watched().current_leader {
          return Ok(Some(leader));
        }
        if let Err(e) = WatchReceiver::changed(&mut metrics_rx).await {
          let error_msg = format!("Metrics watch error: {:?}", e);
          tracing::debug!("{}", error_msg);
          return Err(Error::internal(error_msg));
        }
      }
    })
    .await;

    match result {
      Ok(inner_result) => inner_result,
      Err(_) => Ok(None),
    }
  }

  /// Assume this node is the leader.
  ///
  /// Returns `Ok(LeaderHandler)` if this node is the current leader,
  /// or `Err(ForwardToLeader)` with leader information if not.
  pub(crate) async fn assume_leader(&self) -> StdResult<LeaderHandler<'_>, ForwardToLeader> {
    let current_node_id = *self.raft().node_id();

    match self.get_leader().await {
      Ok(Some(leader_id)) => {
        if leader_id == current_node_id {
          Ok(LeaderHandler::new(self))
        } else {
          Err(ForwardToLeader {
            leader_id: Some(leader_id),
            leader_node: None,
          })
        }
      }
      Ok(None) => Err(ForwardToLeader {
        leader_id: None,
        leader_node: None,
      }),
      Err(_) => Err(ForwardToLeader {
        leader_id: None,
        leader_node: None,
      }),
    }
  }

  /// Execute a request locally as leader, or forward to the current leader.
  ///
  /// This method unifies the assume_leader/forward pattern used by all
  /// client-facing operations.
  pub(crate) async fn execute_or_forward(
    &self,
    payload: RequestPayload,
  ) -> Result<ForwardResponse> {
    match self.assume_leader().await {
      Ok(leader) => Self::dispatch_leader_handler(leader, payload).await,
      Err(forward_err) => {
        let request = ForwardRequest { body: payload };
        self
          .forward_with_leader_id(forward_err.leader_id, request)
          .await
      }
    }
  }

  /// Handle a forwarded request (gRPC entry point).
  ///
  /// This is the entry point for requests coming from other nodes via gRPC.
  /// It handles the request using LeaderHandler if this node is leader,
  /// or forwards to the actual leader if not.
  pub async fn handle_forward_request(&self, request: ForwardRequest) -> Result<ForwardResponse> {
    debug!("recv forward req: {:?}", request);

    for attempt in 0..MAX_RETRIES {
      match self.assume_leader().await {
        Ok(leader) => {
          return Self::dispatch_leader_handler(leader, request.body).await;
        }
        Err(forward_err) => {
          let retry_reason = match forward_err.leader_id {
            Some(leader_id) => {
              match self
                .forward_request_to_leader(leader_id, request.clone())
                .await
              {
                Ok(response) => return Ok(response),
                Err(e) => {
                  if Self::is_retriable_error(&e) {
                    Some(format!("Failed to forward request ({e})"))
                  } else {
                    return Err(e);
                  }
                }
              }
            }
            None => Some("No leader available to forward request".to_string()),
          };

          if let Some(reason) = retry_reason
            && attempt < MAX_RETRIES - 1
          {
            let delay = RETRY_INITIAL_INTERVAL * 2u32.saturating_pow(attempt);
            let delay = delay.min(RETRY_MAX_INTERVAL);
            debug!(
              "{}, retrying {}/{}, waiting {:?}",
              reason,
              attempt + 1,
              MAX_RETRIES,
              delay
            );
            sleep(delay).await;
            continue;
          }

          return Err(Error::internal(
            "No leader available to forward request after max retries",
          ));
        }
      }
    }

    Err(Error::internal(
      "No leader available to forward request after max retries",
    ))
  }

  // -- Private helpers --

  /// Send a forward request to a gRPC endpoint.
  pub(crate) async fn send_forward_request(
    &self,
    addr: &String,
    request: ForwardRequest,
  ) -> Result<crate::raft::protobuf::RaftReply> {
    let timeout = Some(Duration::from_millis(10_000));
    let channel = JoinConnectionFactory::create_rpc_channel(addr, timeout, None)
      .await
      .map_err(|e| {
        tracing::error!("Failed to connect to {}: {:?}", addr, e);
        e
      })?;

    let mut raft_client = RaftServiceClient::new(channel);

    let response = raft_client
      .forward(request)
      .await
      .map_err(|e| Error::internal(format!("Failed to forward request: {}", e)))?;

    Ok(response.into_inner())
  }

  /// Forward a request to a specific leader by node ID.
  async fn forward_request_to_leader(
    &self,
    leader_id: NodeId,
    request: ForwardRequest,
  ) -> Result<ForwardResponse> {
    let membership = self
      .state_machine()
      .get_last_membership()
      .map_err(|e| Error::internal(format!("Failed to get membership: {}", e)))?;

    let leader_node = membership
      .membership()
      .get_node(&leader_id)
      .ok_or_else(|| Error::internal("Leader id not found in membership"))?;

    let leader_addr = leader_node.endpoint.to_string();
    let reply = self.send_forward_request(&leader_addr, request).await?;

    if reply.error.is_empty() {
      let forward_response: ForwardResponse = decode(&reply.data)
        .map_err(|e| Error::internal(format!("Failed to deserialize response: {}", e)))?;
      Ok(forward_response)
    } else {
      Err(Error::internal(format!(
        "Leader returned error: {:?}",
        String::from_utf8_lossy(&reply.error)
      )))
    }
  }

  /// Forward a request to a leader, resolving the leader_id first.
  async fn forward_with_leader_id(
    &self,
    leader_id: Option<NodeId>,
    request: ForwardRequest,
  ) -> Result<ForwardResponse> {
    let leader_id = leader_id.ok_or_else(|| {
      Error::retryable(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "No leader available",
      ))
    })?;
    self.forward_request_to_leader(leader_id, request).await
  }

  /// Dispatch a request body to the appropriate LeaderHandler method.
  pub(crate) async fn dispatch_leader_handler(
    leader: LeaderHandler<'_>,
    body: RequestPayload,
  ) -> Result<ForwardResponse> {
    match body {
      RequestPayload::Write(entry) => {
        let result = leader.write(entry).await?;
        Ok(ForwardResponse::Write(result))
      }
      RequestPayload::BatchWrite(req) => {
        let result = leader.batch_write(req).await?;
        Ok(ForwardResponse::BatchWrite(result))
      }
      RequestPayload::Txn(req) => {
        let result = leader.txn(req).await?;
        Ok(ForwardResponse::Txn(result))
      }
      RequestPayload::GetKV(req) => {
        let result = leader.read(req).await?;
        Ok(ForwardResponse::GetKV(result))
      }
      RequestPayload::ScanPrefix(req) => {
        let result = leader.scan_prefix(req).await?;
        Ok(ForwardResponse::ScanPrefix(result))
      }
      RequestPayload::Join(req) => {
        leader.add_node(req).await?;
        Ok(ForwardResponse::Join(()))
      }
      RequestPayload::Leave(req) => {
        leader.remove_node(req).await?;
        Ok(ForwardResponse::Leave(()))
      }
      RequestPayload::GetMembers(req) => {
        let result = leader.get_members(req).await?;
        Ok(ForwardResponse::GetMembers(result))
      }
    }
  }

  /// Check if the error is retriable (network/connection related)
  fn is_retriable_error(error: &Error) -> bool {
    error.is_retryable()
  }
}
