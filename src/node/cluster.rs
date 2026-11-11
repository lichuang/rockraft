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

use crate::error::{ApiError, Error, Result};
use crate::raft::protobuf::raft_service_server::RaftServiceServer;
use crate::raft::types::{ForwardRequest, JoinRequest, Node, RequestPayload, decode};
use crate::service::RaftServiceImpl;

use super::node::RaftNode;

impl RaftNode {
  /// Start the Raft gRPC service (for builder use)
  ///
  /// This function spawns the gRPC server in a background task and waits for
  /// the service to successfully bind to the endpoint before returning.
  pub(crate) async fn start_raft_service(raft_node: Arc<Self>) -> Result<()> {
    let raft_endpoint = raft_node.config.raft.endpoint.clone();
    let max_message_size = raft_node.config.raft.grpc_max_message_size();

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

      // Raise the tonic default 4MB message limit: AppendEntries can
      // legitimately carry far larger payloads (up to
      // `max_payload_entries` entries per RPC).
      let raft_service_server = RaftServiceServer::new(raft_service)
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);

      let server_future = Server::builder()
        .add_service(raft_service_server)
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
        *raft_node.service_handle.lock().await = Some(handle);
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

    // `initialize` writes the membership entry that carries this node, and the
    // state machine rebuilds its node table from that entry. Writing the table
    // here as well would leave a node behind if `initialize` failed, and
    // `is_in_cluster()` -- which falls back to the node table -- would then
    // report the node as a member of a cluster it never joined.
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
          InitializeError::InvalidMembership(e) => {
            return Err(Error::config(format!("invalid membership: {}", e)));
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

  async fn join_via(&self, addr: &str) -> Result<()> {
    let config = &self.config;

    let join_req = JoinRequest {
      node_id: config.node_id,
      endpoint: config.raft.advertise_endpoint.clone(),
    };

    let req = ForwardRequest {
      body: RequestPayload::Join(join_req),
    };

    // A join request originates here; it has not been forwarded yet.
    let reply = self.send_forward_request(addr, req, 0).await?;

    if reply.error.is_empty() {
      Ok(())
    } else {
      // Errors are postcard-encoded `ApiError`; decoding preserves
      // retryability so do_join_cluster can retry transient failures.
      let api_error: ApiError = decode(&reply.error).map_err(|e| {
        Error::internal(format!("Failed to deserialize join error response: {}", e))
      })?;
      Err(Error::from(api_error))
    }
  }
}

#[cfg(test)]
mod tests {
  use std::time::Duration;

  use tempfile::tempdir;

  use super::*;
  use crate::config::Config;
  use crate::config::RaftConfig;
  use crate::config::ReadConsistency;
  use crate::config::RocksdbConfig;
  use crate::node::node::CreateOptions;
  use crate::raft::types::Endpoint;

  fn create_test_config(data_dir: &str, node_id: u64, addr: &str) -> Config {
    let endpoint = Endpoint::parse(addr).expect("Invalid test address");
    Config {
      node_id,
      raft: RaftConfig {
        endpoint: endpoint.clone(),
        advertise_endpoint: endpoint,
        join: vec![],
        heartbeat_interval: None,
        election_timeout_min: None,
        election_timeout_max: None,
        grpc_max_message_size: None,
        read_consistency: ReadConsistency::default(),
      },
      rocksdb: RocksdbConfig {
        data_path: data_dir.to_string(),
        max_open_files: 1024,
        block_cache_size: None,
        write_buffer_size: None,
        max_write_buffer_number: None,
      },
    }
  }

  async fn create_node(data_dir: &str, node_id: u64, addr: &str) -> Result<Arc<RaftNode>> {
    let config = create_test_config(data_dir, node_id, addr);
    RaftNode::create(&config, &CreateOptions::default()).await
  }

  fn test_node(node_id: u64, addr: &str) -> Node {
    Node {
      node_id,
      endpoint: Endpoint::parse(addr).expect("Invalid test address"),
    }
  }

  /// A node table entry written before `Raft::initialize()` succeeds would
  /// outlive a failed init: `is_in_cluster()` falls back to the table when no
  /// membership entry has been applied, so the node would then report itself as
  /// a member of a cluster it never joined and skip initialization next start.
  #[tokio::test]
  async fn test_init_cluster_leaves_the_node_table_untouched_when_initialize_fails() -> Result<()> {
    let temp_dir = tempdir().unwrap().keep();
    let data_path = temp_dir.into_os_string().into_string().unwrap();
    let raft_node = create_node(&data_path, 1, "127.0.0.1:5001").await?;

    // A stopped raft core makes `initialize` fail with a fatal error, the way a
    // storage failure would; `NotAllowed` cannot be provoked directly because
    // `init_cluster` always lists the local node.
    raft_node.shutdown().await?;

    let result = raft_node.init_cluster(test_node(1, "127.0.0.1:5001")).await;
    assert!(
      result.is_err(),
      "initialize must fail once the raft core is gone"
    );

    assert!(
      raft_node.state_machine().get_nodes()?.is_empty(),
      "a failed init must not leave a node behind: the table is derived from the membership entry"
    );
    assert!(
      !raft_node.is_in_cluster()?,
      "a failed init must not make the node claim a membership it never got"
    );

    Ok(())
  }

  /// The table `init_cluster` used to write by hand is rebuilt from the
  /// membership entry `initialize` writes, so dropping that write loses nothing.
  #[tokio::test]
  async fn test_init_cluster_derives_the_node_table_from_the_membership() -> Result<()> {
    let temp_dir = tempdir().unwrap().keep();
    let data_path = temp_dir.into_os_string().into_string().unwrap();
    let raft_node = create_node(&data_path, 1, "127.0.0.1:5002").await?;

    raft_node
      .init_cluster(test_node(1, "127.0.0.1:5002"))
      .await?;

    // `initialize` waits for the entry to be flushed, not applied.
    raft_node
      .raft()
      .wait(Some(Duration::from_secs(5)))
      .applied_index_at_least(Some(0), "initial membership applied")
      .await
      .expect("the membership entry must be applied");

    let nodes = raft_node.state_machine().get_nodes()?;
    assert_eq!(nodes.len(), 1, "the membership entry carries the only node");
    assert_eq!(
      nodes.get(&1).map(|n| n.endpoint.to_string()),
      Some("127.0.0.1:5002".to_string())
    );
    assert!(
      raft_node.is_in_cluster()?,
      "the initialized node is a voter of its own single-node cluster"
    );

    Ok(())
  }
}
