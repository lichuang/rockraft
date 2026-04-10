//! Cluster lifecycle management for RaftNode.
//!
//! Handles node startup, gRPC service initialization, cluster initialization
//! (single-node), and joining an existing cluster.

use std::collections::BTreeMap;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InitializeError, RaftError};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tonic::transport::Server;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::error::{Error, Result};
use crate::raft::protobuf::raft_service_server::RaftServiceServer;
use crate::raft::types::{ForwardRequest, JoinRequest, Node, RequestPayload};
use crate::service::RaftServiceImpl;

use super::node::RaftNode;

impl RaftNode {
  /// Start the Raft gRPC service (for builder use)
  ///
  /// This function spawns the gRPC server in a background task and waits for
  /// the service to successfully bind to the endpoint before returning.
  pub(crate) async fn start_raft_service(raft_node: Arc<Self>) -> Result<()> {
    let raft_endpoint = raft_node.config.raft.endpoint.clone();

    let mut shutdown_rx = raft_node.shutdown_tx.subscribe();
    let raft_node_for_service = raft_node.clone();
    let (startup_tx, startup_rx) = oneshot::channel::<StdResult<(), String>>();

    let handle = tokio::task::spawn(async move {
      tracing::info!("Starting Raft gRPC service on {}", raft_endpoint);

      let raft_service = RaftServiceImpl::new(raft_node_for_service);

      let listener = match TcpListener::bind(&raft_endpoint.to_string()).await {
        Ok(l) => l,
        Err(e) => {
          let err_msg = format!("Failed to bind gRPC server to {}: {}", raft_endpoint, e);
          tracing::error!("{}", err_msg);
          let _ = startup_tx.send(Err(err_msg));
          return;
        }
      };

      if startup_tx.send(Ok(())).is_err() {
        error!("Failed to signal startup completion");
        return;
      }

      info!("Raft gRPC service listening on {}", raft_endpoint);

      let server_future = Server::builder()
        .add_service(RaftServiceServer::new(raft_service))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

      tokio::select! {
        _ = shutdown_rx.recv() => {
          info!("Raft gRPC service received shutdown signal, shutting down...");
        }
        result = server_future => {
          match result {
            Ok(_) => info!("Raft gRPC service finished normally"),
            Err(e) => error!("Raft gRPC service error: {}", e),
          }
        }
      }

      info!("Raft gRPC service stopped");
    });

    match startup_rx.await {
      Ok(Ok(())) => {
        *raft_node
          .service_handle
          .lock()
          .map_err(|e| Error::internal(format!("Failed to lock service handle: {}", e)))? =
          Some(handle);
        info!("Raft gRPC service started successfully");
        Ok(())
      }
      Ok(Err(err_msg)) => {
        let _ = handle.await;
        Err(Error::internal(err_msg))
      }
      Err(_) => {
        let _ = handle.await;
        Err(Error::internal(
          "gRPC service startup task failed unexpectedly",
        ))
      }
    }
  }

  /// Check if this node is in the cluster membership
  pub(crate) fn is_in_cluster(&self) -> Result<bool> {
    let last_membership = self
      .state_machine()
      .get_last_membership()
      .map_err(|e| Error::internal(format!("get_last_membership error: {}", e)))?;
    let node_id = *self.raft().node_id();

    let is_voter = last_membership
      .membership()
      .voter_ids()
      .any(|id| id == node_id);

    Ok(is_voter)
  }

  /// Initialize the Raft cluster with a single node (for builder use)
  pub(crate) async fn init_cluster(&self, node: Node) -> Result<()> {
    if node.node_id != *self.raft().node_id() {
      return Err(Error::config(format!(
        "Node ID {} does not match current node ID {}",
        node.node_id,
        self.raft().node_id()
      )));
    }

    if node.endpoint.addr().is_empty() {
      return Err(Error::config("Node endpoint address cannot be empty"));
    }

    info!("Adding node {} to state machine", node.node_id);
    self.state_machine().add_node(node.clone()).map_err(|e| {
      error!("Failed to add node: {}", e);
      Error::internal(format!("Failed to add node: {}", e))
    })?;
    info!("Node {} added to state machine successfully", node.node_id);

    let node_id = node.node_id;
    let mut nodes = BTreeMap::new();
    nodes.insert(node_id, node);

    if let Err(e) = self.raft().initialize(nodes).await {
      match e {
        RaftError::APIError(e) => match e {
          InitializeError::NotAllowed(e) => {
            info!("Already initialized: {}", e);
          }
          InitializeError::NotInMembers(e) => {
            return Err(Error::config(e.to_string()));
          }
        },
        RaftError::Fatal(e) => {
          return Err(Error::internal(e.to_string()));
        }
      }
    }

    Ok(())
  }

  /// Join the raft cluster using the configured join addresses
  pub async fn join_cluster(&self) -> Result<()> {
    let config = &self.config;
    if config.raft.join.is_empty() {
      info!("'--join' is empty, do not need joining cluster");
      return Ok(());
    }

    if self.is_in_cluster()? {
      info!("node has already in cluster, do not need joining cluster");
      return Ok(());
    }

    self.do_join_cluster().await?;
    Ok(())
  }

  async fn do_join_cluster(&self) -> Result<()> {
    let config = &self.config;
    let addrs = &config.raft.join;
    let mut errors = vec![];
    let raft_address = config.raft.endpoint.to_string();
    let raft_advertise_address = config.raft.advertise_endpoint.to_string();

    for addr in addrs {
      if addr == &raft_address || addr == &raft_advertise_address {
        debug!("ignore join cluster via self node address {}", addr);
        continue;
      }
      for _i in 0..3 {
        let result = self.join_via(addr).await;
        info!("join cluster via {} result: {:?}", addr, result);

        match result {
          Ok(x) => return Ok(x),
          Err(api_error) => {
            let can_retry = api_error.is_retryable();

            if can_retry {
              debug!("try to connect to addr {} again", addr);
              sleep(Duration::from_millis(1_000)).await;
              continue;
            } else {
              errors.push(api_error);
              break;
            }
          }
        }
      }
    }

    Err(Error::internal(format!(
      "fail to join node-{} to cluster via {:?}, errors: {}",
      self.raft().node_id(),
      addrs,
      errors
        .into_iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(", ")
    )))
  }

  async fn join_via(&self, addr: &String) -> Result<()> {
    let config = &self.config;

    let join_req = JoinRequest {
      node_id: config.node_id,
      endpoint: config.raft.endpoint.clone(),
    };

    let req = ForwardRequest {
      body: RequestPayload::Join(join_req),
    };

    let reply = self.send_forward_request(addr, req).await?;

    if reply.error.is_empty() {
      Ok(())
    } else {
      Err(Error::internal(format!(
        "Join failed: {:?}",
        String::from_utf8_lossy(&reply.error)
      )))
    }
  }
}
