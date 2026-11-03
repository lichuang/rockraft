//! Forwarding infrastructure for RaftNode.
//!
//! Handles leader detection, request forwarding to remote leaders,
//! and retry logic for leader-redirected operations.

use std::result::Result as StdResult;
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use tokio::time::{sleep, timeout};
use tracing::debug;

use crate::error::{ApiError, Error, Result, RetryReason};
use crate::raft::protobuf::RaftReply;
use crate::raft::types::{
  ForwardRequest, ForwardResponse, ForwardToLeader, NodeId, RequestPayload, decode,
};

use super::LeaderHandler;
use super::node::RaftNode;

/// Retry configuration for forward operations
const MAX_RETRIES: u32 = 20;
const RETRY_INITIAL_INTERVAL: Duration = Duration::from_millis(200);
const RETRY_MAX_INTERVAL: Duration = Duration::from_secs(1);
/// Total wall-clock budget for one request's retry loop. Without a cap the
/// worst case was 20 attempts x 2s leader probe + backoff (~90s), which held
/// client connections and amplified election-period pile-ups.
const RETRY_TOTAL_BUDGET: Duration = Duration::from_secs(5);
/// Maximum number of nodes a forwarded request may pass through.
///
/// Forwarding is only a shortcut to the leader: after a couple of hops the
/// node the request originated from is better off re-resolving leadership than
/// letting two nodes that disagree bounce the request forever (every hop
/// starts a fresh retry budget, so nothing else bounds it).
const MAX_FORWARD_HOPS: u32 = 3;

/// Whether a forwarded request has used up the hops it is allowed to take.
fn hop_budget_exhausted(hops: u32) -> bool {
  hops >= MAX_FORWARD_HOPS
}

/// When no leader is known, wait this long for one before reporting
/// `NoLeader` and letting the retry loop back off. Previously every probe
/// waited up to 2s, making each failed attempt cost 2s on top of the backoff.
const LEADER_PROBE_TIMEOUT: Duration = Duration::from_millis(200);

/// Cheap per-call pseudo-random value for backoff jitter. Uses nanosecond
/// clock bits; not cryptographically random, but enough to de-synchronize
/// concurrent retries across requests and tasks.
fn jitter_millis() -> u64 {
  let nanos = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .map(|d| d.subsec_nanos() as u64 ^ (d.as_secs() & 0xFFFF))
    .unwrap_or(0);
  nanos % RETRY_INITIAL_INTERVAL.as_millis() as u64
}

impl RaftNode {
  /// Get the current leader node ID, waiting up to a deadline.
  ///
  /// Returns `Ok(Some(leader_id))` if a leader is found within the deadline,
  /// `Ok(None)` if no leader is found before timeout.
  pub(crate) async fn get_leader(&self) -> Result<Option<NodeId>> {
    let deadline = LEADER_PROBE_TIMEOUT;
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
  /// Handles three transient situations with retries (exponential backoff):
  /// - no leader yet (election in progress)
  /// - this node stepped down between `assume_leader()` and the actual
  ///   write, which then returns a forward-to-leader redirect
  /// - the forwarding target is unreachable or itself stepped down
  pub(crate) async fn execute_or_forward(
    &self,
    payload: RequestPayload,
    hops: u32,
  ) -> Result<ForwardResponse> {
    let deadline = tokio::time::Instant::now() + RETRY_TOTAL_BUDGET;
    for attempt in 0..MAX_RETRIES {
      if tokio::time::Instant::now() >= deadline {
        return Err(Error::retryable(std::io::Error::new(
          std::io::ErrorKind::TimedOut,
          "forward retry budget exhausted",
        )));
      }
      let result = match self.assume_leader().await {
        Ok(leader) => Self::dispatch_leader_handler(leader, payload.clone()).await,
        Err(forward_err) => match forward_err.leader_id {
          Some(leader_id) => {
            self
              .forward_request_to_leader(
                leader_id,
                ForwardRequest {
                  body: payload.clone(),
                },
                hops + 1,
              )
              .await
          }
          None => Err(Error::retryable_with_reason(RetryReason::NoLeader)),
        },
      };

      let err = match result {
        Ok(response) => return Ok(response),
        Err(err) => err,
      };

      // A write can race with a leadership change: this node was leader in
      // its cached metrics when assume_leader() ran but had stepped down
      // by the time client_write() executed. Redirect directly to the
      // known leader instead of failing the request.
      let result = match err.forward_leader_id() {
        Some(leader_id) => {
          self
            .forward_request_to_leader(
              leader_id,
              ForwardRequest {
                body: payload.clone(),
              },
              hops + 1,
            )
            .await
        }
        None => Err(err),
      };

      match result {
        Ok(response) => return Ok(response),
        Err(err) => {
          if err.is_retryable() && attempt < MAX_RETRIES - 1 {
            // Exponential backoff with jitter: synchronized retries after an
            // election pile up on the new leader; a random factor spreads
            // them out. 200ms..(min(base*2^attempt, cap) * 1.5]
            let base = RETRY_INITIAL_INTERVAL * 2u32.saturating_pow(attempt);
            let jitter = jitter_millis();
            let delay = (base + Duration::from_millis(jitter)).min(RETRY_MAX_INTERVAL * 3 / 2);
            debug!(
              "execute_or_forward: retry {}/{} after {:?}: {}",
              attempt + 1,
              MAX_RETRIES,
              delay,
              err
            );
            sleep(delay).await;
            continue;
          }
          return Err(err);
        }
      }
    }

    Err(Error::internal(
      "No leader available to forward request after max retries",
    ))
  }

  /// Handle a forwarded request (gRPC entry point).
  ///
  /// This is the entry point for requests coming from other nodes via gRPC.
  /// `hops` arrives in the `RaftRequest` envelope, not in the postcard body, so
  /// a node running an older version can still decode the request.
  ///
  /// It runs the same execute-or-forward logic as local requests, so multi-hop
  /// redirects resolve on this node.
  pub async fn handle_forward_request(
    &self,
    request: ForwardRequest,
    hops: u32,
  ) -> Result<ForwardResponse> {
    debug!("recv forward req: {:?} hops={}", request, hops);

    if hop_budget_exhausted(hops) {
      // Another hop would only extend a loop: report a retryable error so the
      // origin re-resolves leadership instead.
      debug!(
        "forwarded request exceeded {} hops, returning it to the caller",
        MAX_FORWARD_HOPS
      );
      return Err(Error::retryable_with_reason(RetryReason::LeaderTransition));
    }

    self.execute_or_forward(request.body, hops).await
  }

  // -- Private helpers --

  /// Send a forward request to a gRPC endpoint via the shared connection pool.
  ///
  /// `hops` is placed in the `RaftRequest` protobuf envelope rather than the
  /// postcard body, so an older peer that ignores unknown proto fields can
  /// still decode `data`; it will not propagate the count, which only means the
  /// bound is enforced among nodes that understand the field.
  ///
  /// Reuses pooled connections instead of dialing per request: forwarding is
  /// the hot entry path for non-leader nodes, and per-request channel setup
  /// (TCP + HTTP/2 handshake + DNS) dominated cluster write latency.
  pub(crate) async fn send_forward_request(
    &self,
    addr: &str,
    request: ForwardRequest,
    hops: u32,
  ) -> Result<RaftReply> {
    let mut raft_client = self.client_pool.raft_service_client(addr).await?;

    let response = raft_client
      .forward(request.into_grpc_request(hops)?)
      .await
      .map_err(|e| Error::internal(format!("Failed to forward request: {}", e)))?;

    Ok(response.into_inner())
  }

  /// Forward a request to a specific leader by node ID.
  async fn forward_request_to_leader(
    &self,
    leader_id: NodeId,
    request: ForwardRequest,
    hops: u32,
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
    let reply = self
      .send_forward_request(&leader_addr, request, hops)
      .await?;

    if reply.error.is_empty() {
      let forward_response: ForwardResponse = decode(&reply.data)
        .map_err(|e| Error::internal(format!("Failed to deserialize response: {}", e)))?;
      Ok(forward_response)
    } else {
      // Errors are postcard-encoded `ApiError` (see
      // `RaftServiceImpl::result_to_raft_reply`), so redirects keep their
      // leader id and stay retryable across the wire.
      let api_error: ApiError = decode(&reply.error)
        .map_err(|e| Error::internal(format!("Failed to deserialize error response: {}", e)))?;
      Err(Error::from(api_error))
    }
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
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::raft::protobuf as pb;
  use crate::raft::types::GetKVReq;
  use crate::raft::types::decode;
  use prost::Message;

  fn a_request() -> ForwardRequest {
    ForwardRequest {
      body: RequestPayload::GetKV(GetKVReq {
        key: "k".to_string(),
      }),
    }
  }

  #[test]
  fn test_hop_budget_is_finite() {
    assert!(!hop_budget_exhausted(0));
    assert!(!hop_budget_exhausted(MAX_FORWARD_HOPS - 1));
    assert!(
      hop_budget_exhausted(MAX_FORWARD_HOPS),
      "a request that used up its budget must not be forwarded again"
    );
    assert!(hop_budget_exhausted(MAX_FORWARD_HOPS + 1));
  }

  /// The hop count travels in the protobuf envelope, so the postcard body is
  /// unchanged and an older peer can still decode it.
  #[test]
  fn test_hop_count_roundtrips_in_the_envelope() {
    let request = a_request().into_grpc_request(2).unwrap().into_inner();
    assert_eq!(request.hops, 2);
    assert!(
      decode::<ForwardRequest>(&request.data).is_ok(),
      "the postcard body must stay decodable without the hop count"
    );

    let bytes = request.encode_to_vec();
    let decoded = pb::RaftRequest::decode(&bytes[..]).unwrap();
    assert_eq!(decoded.data, request.data);
    assert_eq!(decoded.hops, 2);
  }

  /// An envelope written by a version that predates the field has no field 2;
  /// prost decodes it with `hops = 0`, so old and new nodes keep communicating.
  #[test]
  fn test_old_envelope_decodes_with_zero_hops() {
    // field 1 (`data`), wire type 2: tag 0x0A, length 1, value 0x01
    let old_envelope = [0x0A, 0x01, 0x01];
    let decoded = pb::RaftRequest::decode(&old_envelope[..]).unwrap();
    assert_eq!(decoded.data, vec![1]);
    assert_eq!(decoded.hops, 0);
  }
}
