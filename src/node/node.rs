//! RaftNode — the primary public type for interacting with a Raft KV cluster.
//!
//! This module contains the [`RaftNode`] struct definition, construction,
//! lifecycle, and all client-facing API methods (read/write/txn/cluster ops).
//!
//! Internal implementation is split into focused sibling modules:
//! - [`forward`](super::forward) — leader detection, request forwarding, retry logic
//! - [`cluster`](super::cluster) — cluster startup, initialization, joining

use std::path::PathBuf;
use std::sync::Arc;

use openraft::Raft;
use openraft::async_runtime::watch::WatchReceiver;
use tokio::sync::{Mutex, broadcast};
use tracing::debug;

use crate::config::Config;
use crate::engine::RocksDBEngine;
use crate::error::{Error, Result};
use crate::network::ClientPool;
use crate::raft::network::NetworkFactory;
use crate::raft::store;
use crate::raft::store::RocksStateMachine;
use crate::raft::types::LogId;
use crate::raft::types::{
  AppliedState, BatchWriteReply, BatchWriteReq, Cmd, GetKVReply, GetKVReq, GetMembersReply,
  GetMembersReq, JoinRequest, LeaveRequest, LogEntry, RequestPayload, ScanPrefixReply,
  ScanPrefixReq, TxnReply, TxnReq, TypeConfig, UpsertKV,
};

use super::LeaderHandler;

pub struct RaftNode {
  #[allow(dead_code)]
  pub(crate) engine: Arc<RocksDBEngine>,
  pub(crate) raft: Arc<Raft<TypeConfig>>,
  pub(crate) config: Config,
  #[allow(dead_code)]
  pub(crate) factory: NetworkFactory,
  pub(crate) state_machine: Arc<RocksStateMachine>,
  pub(crate) shutdown_tx: broadcast::Sender<()>,
  pub(crate) service_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RaftNode {
  /// Get a reference to the underlying Raft instance
  pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
    &self.raft
  }

  /// Get a reference to the state machine
  pub fn state_machine(&self) -> &Arc<RocksStateMachine> {
    &self.state_machine
  }

  /// Shutdown raft thread
  pub async fn shutdown(&self) -> Result<()> {
    let _ = self.shutdown_tx.send(());

    let handle = self.service_handle.lock().await.take();

    if let Some(h) = handle {
      h.await.ok();
    }

    Ok(())
  }

  pub(crate) async fn create(config: &Config) -> Result<Arc<Self>> {
    let engine = Arc::new(store::create_storage_engine(
      &config.rocksdb.data_path,
      config.rocksdb.max_open_files,
    )?);

    let data_dir = PathBuf::from(&config.rocksdb.data_path);
    let (log_store, state_machine) = store::create_stores(&engine, data_dir).await?;

    let client_pool = Arc::new(ClientPool::new(10));
    let factory = NetworkFactory::new(client_pool);
    let raft_config = config.raft.to_openraft_config();

    let raft = Arc::new(
      Raft::new(
        config.node_id,
        Arc::new(raft_config),
        factory.clone(),
        log_store,
        state_machine.clone(),
      )
      .await
      .map_err(|e| Error::internal(format!("Failed to create raft: {}", e)))?,
    );

    let (shutdown_tx, _) = broadcast::channel(1);

    Ok(Arc::new(Self {
      engine,
      raft,
      config: config.clone(),
      factory,
      state_machine: Arc::new(state_machine),
      shutdown_tx,
      service_handle: Mutex::new(None),
    }))
  }

  // ---- Client API: KV operations ----

  /// Write a log entry to the raft cluster
  ///
  /// The entry is appended to the raft log and replicated to a majority of nodes
  /// before returning. This provides strong consistency - once the call returns,
  /// the entry is guaranteed to be durable and will not be lost even if the
  /// current leader fails.
  pub async fn write(&self, entry: LogEntry) -> Result<AppliedState> {
    debug!("write log entry: {:?}", entry);
    self
      .execute_or_forward(RequestPayload::Write(entry))
      .await?
      .into_write()
  }

  /// Delete a key from the KV store
  ///
  /// Returns `AppliedState` with `value: None` confirming the deletion.
  /// This is a convenience method equivalent to writing a
  /// `Cmd::UpsertKV(UpsertKV::delete(key))` log entry.
  pub async fn delete(&self, key: impl ToString) -> Result<AppliedState> {
    debug!("delete key: {}", key.to_string());
    let entry = LogEntry::new(Cmd::UpsertKV(UpsertKV::delete(key)));
    self.write(entry).await
  }

  /// Delete multiple keys atomically in a single batch
  ///
  /// All deletions are applied as a single atomic unit - either
  /// all succeed or all fail.
  pub async fn batch_delete(&self, keys: Vec<String>) -> Result<BatchWriteReply> {
    debug!("batch delete: {} keys", keys.len());
    let entries: Vec<UpsertKV> = keys.into_iter().map(UpsertKV::delete).collect();
    let req = BatchWriteReq { entries };
    self.batch_write(req).await
  }

  /// Batch write multiple entries atomically
  ///
  /// All entries in the batch are applied as a single atomic unit - either
  /// all succeed or all fail.
  pub async fn batch_write(&self, req: BatchWriteReq) -> Result<BatchWriteReply> {
    debug!("batch write: {:?}", req);
    self
      .execute_or_forward(RequestPayload::BatchWrite(req))
      .await?
      .into_batch_write()
  }

  /// Execute an atomic compare-and-swap transaction
  ///
  /// Checks all conditions against the current state, then atomically executes
  /// either `if_then` (conditions met) or `else_then` (conditions not met).
  ///
  /// # Example
  /// ```rust,no_run
  /// use rockraft::raft::types::{TxnReq, TxnCondition, UpsertKV};
  ///
  /// async fn example(node: &rockraft::node::RaftNode) -> Result<(), Box<dyn std::error::Error>> {
  ///     let req = TxnReq::new(vec![TxnCondition::eq("key", b"expected_value")])
  ///       .if_then(UpsertKV::insert("key", b"new_value"));
  ///     let reply = node.txn(req).await?;
  ///     Ok(())
  /// }
  /// ```
  pub async fn txn(&self, req: TxnReq) -> Result<TxnReply> {
    debug!("transaction: {:?}", req);
    self
      .execute_or_forward(RequestPayload::Txn(req))
      .await?
      .into_txn()
  }

  /// Atomically swap a value, returning the previous value
  ///
  /// # Example
  /// ```rust,no_run
  /// # use rockraft::node::RaftNode;
  /// # async fn example(node: &RaftNode) -> Result<(), Box<dyn std::error::Error>> {
  /// let old_value = node.getset("my_key", b"new_value").await?;
  /// # Ok(())
  /// # }
  /// ```
  pub async fn getset(
    &self,
    key: impl ToString,
    value: impl AsRef<[u8]>,
  ) -> Result<Option<Vec<u8>>> {
    use crate::raft::types::UpsertKV;

    let req = TxnReq::new(vec![])
      .if_then(UpsertKV::insert(key, value.as_ref()))
      .with_return_previous();

    match self.txn(req).await? {
      TxnReply::Success { prev_values, .. } => Ok(prev_values.into_iter().next().flatten()),
    }
  }

  /// Read a value from the KV store
  pub async fn read(&self, req: GetKVReq) -> Result<GetKVReply> {
    debug!("read kv: {:?}", req);
    self
      .execute_or_forward(RequestPayload::GetKV(req))
      .await?
      .into_get_kv()
  }

  /// Scan keys with a given prefix
  pub async fn scan_prefix(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    debug!("scan_prefix: {:?}", req);
    self
      .execute_or_forward(RequestPayload::ScanPrefix(req))
      .await?
      .into_scan_prefix()
  }

  // ---- Client API: Cluster operations ----

  /// Add a new node to the raft cluster
  pub async fn add_node(&self, req: JoinRequest) -> Result<()> {
    debug!("join node: {:?}", req);
    self
      .execute_or_forward(RequestPayload::Join(req))
      .await?
      .into_join()
  }

  /// Remove a node from the raft cluster
  pub async fn remove_node(&self, req: LeaveRequest) -> Result<()> {
    debug!("leave node: {:?}", req);
    self
      .execute_or_forward(RequestPayload::Leave(req))
      .await?
      .into_leave()
  }

  /// Get the current cluster membership
  pub async fn get_members(&self, req: GetMembersReq) -> Result<GetMembersReply> {
    debug!("get members: {:?}", req);
    let leader_handler = LeaderHandler::new(self);
    leader_handler.get_members(req).await
  }

  // ---- Client API: Snapshot operations ----

  /// Get the `LogId` of the last snapshot on this node.
  ///
  /// Returns `None` if no snapshot has been built yet.
  /// Useful for integration tests to verify that a snapshot was actually
  /// triggered — compare before and after `trigger_snapshot()`.
  pub fn snapshot_log_id(&self) -> Option<LogId> {
    self.raft.metrics().borrow_watched().snapshot
  }

  /// Trigger a snapshot build on this node and wait for it to complete.
  ///
  /// Returns the `LogId` of the newly built snapshot on success.
  pub async fn trigger_snapshot(&self) -> Result<LogId> {
    self
      .raft
      .trigger()
      .snapshot()
      .await
      .map_err(|e| Error::internal(format!("Failed to trigger snapshot: {}", e)))?;

    self
      .snapshot_log_id()
      .ok_or_else(|| Error::internal("Snapshot log id not found after trigger"))
  }

  /// Purge Raft logs up to (and including) the given log index.
  ///
  /// Logs that have been captured by a snapshot can be safely purged.
  /// Typically called after `trigger_snapshot()` to reclaim disk space.
  pub async fn purge_log(&self, upto_index: u64) -> Result<()> {
    self
      .raft
      .trigger()
      .purge_log(upto_index)
      .await
      .map_err(|e| Error::internal(format!("Failed to purge log: {}", e)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::config::RaftConfig;
  use crate::config::RocksdbConfig;
  use crate::raft::types::{Endpoint, Node};
  use tempfile::tempdir;

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
      },
      rocksdb: RocksdbConfig {
        data_path: data_dir.to_string(),
        max_open_files: 1024,
      },
    }
  }

  async fn setup_nodes(raft_node: &RaftNode, node_ids: Vec<u64>) {
    let sm = raft_node.state_machine();

    for node_id in node_ids {
      let node = Node {
        node_id,
        endpoint: Endpoint::new("127.0.0.1", 1000 + node_id as u32),
      };
      sm.add_node(node).unwrap();
    }
  }

  #[tokio::test]
  async fn test_is_in_cluster_node_exists() -> Result<()> {
    let temp_dir = tempdir().unwrap().keep();
    let data_path = temp_dir.into_os_string().into_string().unwrap();

    let config = create_test_config(&data_path, 1, "127.0.0.1:5001");
    let raft_node = RaftNode::create(&config).await?;

    setup_nodes(&raft_node, vec![1, 2, 3]).await;

    let result = raft_node.is_in_cluster()?;
    assert!(result, "Node 1 should be in the cluster");

    Ok(())
  }

  #[tokio::test]
  async fn test_is_in_cluster_node_not_exists() -> Result<()> {
    let temp_dir = tempdir().unwrap().keep();
    let data_path = temp_dir.into_os_string().into_string().unwrap();

    let config = create_test_config(&data_path, 4, "127.0.0.1:5004");
    let raft_node = RaftNode::create(&config).await?;

    setup_nodes(&raft_node, vec![1, 2, 3]).await;

    let result = raft_node.is_in_cluster()?;
    assert!(!result, "Node 4 should not be in the cluster");

    Ok(())
  }
}
