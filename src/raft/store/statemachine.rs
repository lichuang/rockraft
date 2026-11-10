use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Error;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use bytes::Bytes;

use crate::raft::types::{decode, encode};
use futures::Stream;
use futures::TryStreamExt;
use openraft::EntryPayload;
use openraft::Membership;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::alias::SnapshotDataOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use rocksdb::BoundColumnFamily;
use tracing::debug;
use tracing::info;
use tracing::warn;

use rocksdb::DB;

use super::keys::LAST_APPLIED_LOG_KEY;
use super::keys::LAST_MEMBERSHIP_KEY;
use super::keys::NODES_KEY;
use super::keys::SM_DATA_FAMILY;
use super::keys::SM_META_FAMILY;
#[cfg(test)]
use super::snapshot::DEFAULT_SNAPSHOT_ENTRY_LIMIT;
use super::snapshot::build_snapshot;
use super::snapshot::get_current_snapshot;
use crate::raft::store::snapshot::recover_snapshot;
use crate::raft::types::AppliedState;
use crate::raft::types::Cmd;
use crate::raft::types::LogId;
use crate::raft::types::Node;
use crate::raft::types::NodeId;
use crate::raft::types::Operation;
use crate::raft::types::Snapshot;
use crate::raft::types::SnapshotMeta;
use crate::raft::types::StoredMembership;
use crate::raft::types::SysData;
use crate::raft::types::TxnOp;
use crate::raft::types::TxnReply;
use crate::raft::types::TxnReq;
use crate::raft::types::TypeConfig;
use crate::raft::types::UpsertKV;
use crate::raft::types::read_logs_err;

#[derive(Debug)]
pub struct RocksStateMachine {
  db: Arc<DB>,
  snapshot_dir: PathBuf,
  // In-memory cache of small state-machine metadata (nodes, last_applied,
  // last_membership), also persisted to the SM_META column family.
  //
  // Why not read from RocksDB directly?
  // - apply() processes a stream of log entries; the metadata must be visible
  //   to later entries within the same batch before the WriteBatch is
  //   committed.
  // - Avoids repeated RocksDB meta-CF lookups on every applied entry.
  //
  // apply() stages metadata changes on a working copy and writes them into the
  // same WriteBatch as the KV data; this cache is replaced only after that
  // write succeeds, so neither the cache nor RocksDB can run ahead of the data
  // they describe. `add_node` is the one deliberate exception: it writes the
  // table immediately, for callers that seed it outside the raft log.
  sys_data: Arc<Mutex<SysData>>,
  /// Largest key or value a snapshot entry may claim, derived from the node's
  /// gRPC message cap: an entry this node accepted for replication must also
  /// be installable from a snapshot.
  max_snapshot_entry_bytes: usize,
}

/// Writes buffered in the current apply batch but not yet committed.
/// `None` value means a delete is pending for that key.
type PendingWrites = BTreeMap<String, Option<Bytes>>;

/// Evaluate a transaction condition against the actual value
fn evaluate_condition(expected: &TxnOp, actual: Option<&[u8]>) -> bool {
  match expected {
    TxnOp::Exists => actual.is_some(),
    TxnOp::NotExists => actual.is_none(),
    TxnOp::Equal(expected_value) => actual
      .map(|v| v == expected_value.as_slice())
      .unwrap_or(false),
    TxnOp::NotEqual(expected_value) => actual
      .map(|v| v != expected_value.as_slice())
      .unwrap_or(true),
    TxnOp::Greater(expected_value) => actual
      .map(|v| v > expected_value.as_slice())
      .unwrap_or(false),
    TxnOp::Less(expected_value) => actual
      .map(|v| v < expected_value.as_slice())
      .unwrap_or(false),
    TxnOp::GreaterEqual(expected_value) => actual
      .map(|v| v >= expected_value.as_slice())
      .unwrap_or(false),
    TxnOp::LessEqual(expected_value) => actual
      .map(|v| v <= expected_value.as_slice())
      .unwrap_or(false),
  }
}

/// Remove `*_incomplete` files left behind by a previous process.
///
/// The receive path tracks in-flight transfers only in memory, so a file that
/// is still on disk at startup belongs to a transfer that can never resume.
/// Sweeping it here keeps restarts from leaking snapshot-sized files forever
/// (stale eviction only knows about transfers of this process).
async fn remove_leftover_incomplete_files(snapshot_dir: &Path) -> io::Result<()> {
  let mut entries = tokio::fs::read_dir(snapshot_dir).await?;

  while let Some(entry) = entries.next_entry().await? {
    if !entry.file_type().await?.is_file() {
      continue;
    }

    let name = entry.file_name();
    if !name.to_string_lossy().ends_with("_incomplete") {
      continue;
    }

    let path = entry.path();
    match tokio::fs::remove_file(&path).await {
      Ok(()) => info!(
        "Removed leftover snapshot file '{}': a streaming transfer from a previous process can never resume after restart",
        path.display()
      ),
      Err(e) => warn!(
        "Failed to remove leftover incomplete snapshot file '{}': {}",
        path.display(),
        e
      ),
    }
  }

  Ok(())
}

impl Clone for RocksStateMachine {
  fn clone(&self) -> Self {
    Self {
      db: self.db.clone(),
      snapshot_dir: self.snapshot_dir.clone(),
      sys_data: self.sys_data.clone(),
      max_snapshot_entry_bytes: self.max_snapshot_entry_bytes,
    }
  }
}

/// Reject a snapshot id that cannot be used as a file name component.
///
/// The id arrives from a remote peer inside an `InstallSnapshot` request and
/// ends up in a path, so `..`, separators or an absolute path would place the
/// assembled file outside the snapshot directory. Ids produced by
/// `build_snapshot` are `{leader_id}-{index}-{timestamp}-{seq}`, i.e. ASCII
/// alphanumerics, `-`, `_` and `.`.
fn check_snapshot_id(snapshot_id: &str) -> Result<(), io::Error> {
  let valid = !snapshot_id.is_empty()
    && snapshot_id.len() <= 255
    && snapshot_id != "."
    && snapshot_id != ".."
    && snapshot_id
      .chars()
      .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));

  if valid {
    Ok(())
  } else {
    Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      format!("invalid snapshot id: {:?}", snapshot_id),
    ))
  }
}

/// Name for a snapshot file created before its snapshot id is known.
///
/// Unique per call, so an abandoned transfer can never be mistaken for the
/// next one, and `_incomplete`-suffixed, so the startup sweep reclaims it.
fn receiving_snapshot_id() -> String {
  let nanos = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .map(|elapsed| elapsed.as_nanos())
    .unwrap_or_default();

  format!("receiving-{}", nanos)
}

impl RocksStateMachine {
  /// Path of the file a streamed-in snapshot is assembled into.
  ///
  /// Kept separate from [`Self::create_snapshot_temp_file`] so the receiving
  /// service can delete the file once the snapshot has been installed.
  ///
  /// Crate-private: the on-disk naming of an in-flight transfer is an
  /// implementation detail of the snapshot receive path, not public API.
  pub(crate) fn snapshot_temp_file_path(&self, snapshot_id: &str) -> PathBuf {
    self
      .snapshot_dir
      .join(format!("{}_incomplete", snapshot_id))
  }

  /// Create the file a streamed-in snapshot is assembled into.
  ///
  /// Opened **read+write**: the receiving service writes the chunks through
  /// this handle and then hands the very same handle to `install_snapshot`,
  /// which reads it back. A write-only handle makes that read fail with
  /// `EBADF`.
  pub fn create_snapshot_temp_file(&self, snapshot_id: &str) -> Result<File, io::Error> {
    check_snapshot_id(snapshot_id)?;
    OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .truncate(true)
      .open(self.snapshot_temp_file_path(snapshot_id))
  }

  fn lock_sys_data(&self) -> Result<std::sync::MutexGuard<'_, SysData>, io::Error> {
    self
      .sys_data
      .lock()
      .map_err(|e| Error::other(format!("Mutex lock failed: {}", e)))
  }

  #[cfg(test)]
  pub(crate) async fn new(db: Arc<DB>, data_dir: PathBuf) -> Result<RocksStateMachine, Error> {
    Self::new_with_snapshot_entry_limit(db, data_dir, DEFAULT_SNAPSHOT_ENTRY_LIMIT).await
  }

  /// Create a state machine that accepts snapshot entries up to
  /// `max_snapshot_entry_bytes` per key or value.
  ///
  /// The test-only `new` constructor uses the crate default; a node derives
  /// the limit from its own `grpc_max_message_size`, because an entry larger
  /// than this bound can be replicated but never installed.
  pub(crate) async fn new_with_snapshot_entry_limit(
    db: Arc<DB>,
    data_dir: PathBuf,
    max_snapshot_entry_bytes: usize,
  ) -> Result<RocksStateMachine, Error> {
    db.cf_handle(SM_META_FAMILY)
      .ok_or_else(|| Error::other("column family `_sm_meta` not found"))?;
    db.cf_handle(SM_DATA_FAMILY)
      .ok_or_else(|| Error::other("column family `_sm_data` not found"))?;

    let snapshot_dir = data_dir.join("snapshot");
    // A node that has never built a snapshot still receives one, and the
    // receive path assembles the transfer into this directory.
    tokio::fs::create_dir_all(&snapshot_dir).await?;
    // In-flight transfers live only in the gRPC service's in-memory map, so
    // any `_incomplete` file still here belongs to a transfer that can never
    // resume after this restart.
    remove_leftover_incomplete_files(&snapshot_dir).await?;
    let sys_data = Self::recover_sys_data(&db)?;

    Ok(Self {
      db,
      snapshot_dir,
      sys_data: Arc::new(Mutex::new(sys_data)),
      max_snapshot_entry_bytes,
    })
  }

  fn cf_sm_meta(&self) -> Arc<BoundColumnFamily<'_>> {
    self.db.cf_handle(SM_META_FAMILY).unwrap()
  }

  fn cf_sm_data(&self) -> Arc<BoundColumnFamily<'_>> {
    self.db.cf_handle(SM_DATA_FAMILY).unwrap()
  }

  fn recover_sys_data(db: &Arc<DB>) -> Result<SysData, io::Error> {
    let cf_meta = db
      .cf_handle(SM_META_FAMILY)
      .ok_or_else(|| io::Error::other("column family `_sm_meta` not found"))?;

    let last_applied = match db.get_cf(&cf_meta, LAST_APPLIED_LOG_KEY) {
      Ok(Some(v)) => {
        let log_id = decode(&v).map_err(read_logs_err)?;
        Some(log_id)
      }
      Ok(None) => None,
      Err(e) => return Err(read_logs_err(e)),
    };

    // Recover nodes
    let nodes = db
      .get_cf(&cf_meta, NODES_KEY)
      .map_err(read_logs_err)?
      .map(|bytes| decode(&bytes).map_err(read_logs_err))
      .transpose()?
      .unwrap_or_default();

    // Recover last applied membership entry (absent on nodes pre-dating
    // membership persistence, or bootstrapped only via Cmd::AddNode)
    let last_membership = db
      .get_cf(&cf_meta, LAST_MEMBERSHIP_KEY)
      .map_err(read_logs_err)?
      .map(|bytes| decode(&bytes).map_err(read_logs_err))
      .transpose()?;

    Ok(SysData {
      last_applied,
      nodes,
      last_membership,
    })
  }

  fn get_last_applied_log_id(&self) -> Result<Option<LogId>, io::Error> {
    Ok(self.lock_sys_data()?.last_applied)
  }

  /// Get the last applied membership.
  ///
  /// Prefers the membership entry applied from `EntryPayload::Membership`
  /// (the authoritative source, maintained jointly with the raft core).
  /// Falls back to synthesizing a membership from the nodes map for nodes
  /// that were bootstrapped via `Cmd::AddNode` before any membership entry
  /// has been applied.
  pub fn get_last_membership(&self) -> Result<StoredMembership, io::Error> {
    let sys_data = self.lock_sys_data()?;
    if let Some(stored) = &sys_data.last_membership {
      return Ok(stored.clone());
    }
    drop(sys_data);
    self.build_membership_from_nodes()
  }

  /// Build StoredMembership from current nodes
  fn build_membership_from_nodes(&self) -> Result<StoredMembership, io::Error> {
    let sys_data = self.lock_sys_data()?;
    let node_ids: BTreeSet<NodeId> = sys_data.nodes.keys().cloned().collect();
    let nodes = sys_data.nodes.clone();

    // If no nodes, return default membership
    if node_ids.is_empty() {
      return Ok(StoredMembership::default());
    }

    // Create membership with all nodes as voters
    let membership = Membership::new(vec![node_ids], nodes)
      .map_err(|e| io::Error::other(format!("Failed to create membership: {}", e)))?;

    Ok(StoredMembership::new(None, membership))
  }

  /// Get a value from the KV store by key
  pub fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>, io::Error> {
    self
      .db
      .get_cf(&self.cf_sm_data(), key.as_bytes())
      .map_err(read_logs_err)
  }

  /// Scan all key-value pairs with the given prefix from the KV store
  #[allow(clippy::type_complexity)]
  pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, io::Error> {
    let cf = self.cf_sm_data();
    let mut results = Vec::new();

    // Use iterator starting from the prefix
    // This works regardless of the prefix extractor configuration
    let iter = self.db.iterator_cf(
      &cf,
      rocksdb::IteratorMode::From(prefix, rocksdb::Direction::Forward),
    );

    for item in iter {
      let (key, value) = item.map_err(read_logs_err)?;

      // Check if the key has the prefix
      if !key.starts_with(prefix) {
        break;
      }

      results.push((key.to_vec(), value.to_vec()));
    }

    Ok(results)
  }

  /// Test helper: write the apply position on its own. Production code goes
  /// through `apply()`, which carries it in the batch with the data.
  #[cfg(test)]
  fn set_last_applied_log_id(&self, log_id: Option<LogId>) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;

    match log_id {
      Some(id) => {
        let data = encode(&id).map_err(read_logs_err)?;
        self
          .db
          .put_cf(&self.cf_sm_meta(), LAST_APPLIED_LOG_KEY, data)
          .map_err(read_logs_err)?;
        sys_data.last_applied = log_id;
      }
      None => {
        self
          .db
          .delete_cf(&self.cf_sm_meta(), LAST_APPLIED_LOG_KEY)
          .map_err(read_logs_err)?;
        sys_data.last_applied = None;
      }
    }
    Ok(())
  }

  pub fn get_nodes(&self) -> Result<BTreeMap<NodeId, Node>, io::Error> {
    Ok(self.lock_sys_data()?.nodes.clone())
  }

  pub fn contains_node(&self, node_id: NodeId) -> Result<bool, io::Error> {
    Ok(self.lock_sys_data()?.nodes.contains_key(&node_id))
  }

  /// Insert a node into the node table immediately.
  ///
  /// This is the only metadata write that bypasses `apply()` and its
  /// `WriteBatch`, so it is meant for seeding the table outside the raft log
  /// (tests, tools). Cluster bootstrap writes nothing here: `init_cluster`
  /// relies on the membership entry `Raft::initialize()` writes, which
  /// `apply_membership()` turns into this table. Membership changes that arrive
  /// through the raft log go through `Cmd::AddNode` /
  /// `EntryPayload::Membership` and are staged by `apply()`.
  pub fn add_node(&self, node: Node) -> Result<(), io::Error> {
    let mut sys_data = self.lock_sys_data()?;

    sys_data.nodes.insert(node.node_id, node);

    let data = encode(&sys_data.nodes).map_err(read_logs_err)?;
    self
      .db
      .put_cf(&self.cf_sm_meta(), NODES_KEY, data)
      .map_err(read_logs_err)?;

    Ok(())
  }

  fn stage_upsert_kv(
    &self,
    kv: UpsertKV,
    batch: &mut rocksdb::WriteBatch,
    pending: &mut PendingWrites,
  ) {
    let cf_data = &self.cf_sm_data();
    match kv.value {
      Operation::Update(value) => {
        batch.put_cf(cf_data, kv.key.as_bytes(), &value);
        // Bytes::from(Vec) reuses the allocation: no memcpy on the hot path.
        pending.insert(kv.key, Some(Bytes::from(value)));
      }
      Operation::Delete => {
        batch.delete_cf(cf_data, kv.key.as_bytes());
        pending.insert(kv.key, None);
      }
    }
  }

  /// Dispatch a command to its dedicated handler.
  ///
  /// KV writes are buffered in `batch`; metadata (nodes, membership) is
  /// buffered in `sys_data`, which `apply()` writes into that same batch.
  fn apply_cmd(
    &self,
    cmd: Cmd,
    batch: &mut rocksdb::WriteBatch,
    pending: &mut PendingWrites,
    sys_data: &mut SysData,
  ) -> Result<AppliedState, io::Error> {
    match cmd {
      Cmd::UpsertKV(kv) => self.apply_upsert_kv(kv, batch, pending),
      Cmd::BatchUpsertKV { entries } => self.apply_batch_upsert_kv(entries, batch, pending),
      Cmd::AddNode { node, overriding } => self.apply_add_node(&node, overriding, sys_data),
      Cmd::RemoveNode { node_id } => self.apply_remove_node(node_id, sys_data),
      Cmd::Txn { req } => self.apply_txn(req, batch, pending),
    }
  }

  fn apply_upsert_kv(
    &self,
    kv: UpsertKV,
    batch: &mut rocksdb::WriteBatch,
    pending: &mut PendingWrites,
  ) -> Result<AppliedState, io::Error> {
    self.stage_upsert_kv(kv, batch, pending);
    Ok(AppliedState::None)
  }

  fn apply_batch_upsert_kv(
    &self,
    entries: Vec<UpsertKV>,
    batch: &mut rocksdb::WriteBatch,
    pending: &mut PendingWrites,
  ) -> Result<AppliedState, io::Error> {
    for kv in entries {
      self.stage_upsert_kv(kv, batch, pending);
    }
    Ok(AppliedState::None)
  }

  fn apply_add_node(
    &self,
    node: &Node,
    overriding: bool,
    sys_data: &mut SysData,
  ) -> Result<AppliedState, io::Error> {
    let node_id = node.node_id;
    info!(
      "Applying AddNode command for node {} in state machine, overriding={}",
      node_id, overriding
    );

    // Checked against the staged table, not the committed one, so two commands
    // in the same batch see each other.
    if overriding || !sys_data.nodes.contains_key(&node_id) {
      sys_data.nodes.insert(node_id, node.clone());
    }

    Ok(AppliedState::None)
  }

  fn apply_remove_node(
    &self,
    node_id: NodeId,
    sys_data: &mut SysData,
  ) -> Result<AppliedState, io::Error> {
    sys_data.nodes.remove(&node_id);
    Ok(AppliedState::None)
  }

  /// Apply a membership entry from the raft log, making it the single
  /// source of truth for both the stored membership and the nodes table.
  ///
  /// The entry's own `LogId` is part of that record: openraft orders stored
  /// memberships by it (`StoredMembership`), uses it to decide which membership
  /// is committed, and carries it into every `SnapshotMeta`. Storing `None`
  /// makes the applied membership compare as older than any real one.
  ///
  /// The result is staged in `sys_data` and written by `apply()` with the
  /// batch, like every other metadata change.
  fn apply_membership(
    &self,
    log_id: LogId,
    membership: Membership<TypeConfig>,
    sys_data: &mut SysData,
  ) -> Result<(), io::Error> {
    let stored = StoredMembership::new(Some(log_id), membership);
    info!("applying membership: {:?}", stored);

    // The nodes table is derived from the membership's node map (voters and
    // learners alike) so that both views stay consistent with the membership
    // history applied through the raft log.
    sys_data.nodes = stored
      .membership()
      .nodes()
      .map(|(id, node)| (*id, node.clone()))
      .collect();
    sys_data.last_membership = Some(stored);

    Ok(())
  }

  fn apply_txn(
    &self,
    req: TxnReq,
    batch: &mut rocksdb::WriteBatch,
    pending: &mut PendingWrites,
  ) -> Result<AppliedState, io::Error> {
    // Conditions and prev_values must resolve against pending writes so results
    // do not depend on how the apply stream is chunked into batches.
    let mut all_conditions_met = true;
    for condition in &req.condition {
      let actual_value = self.get_kv_with_overlay(&condition.key, pending)?;
      if !evaluate_condition(&condition.expected, actual_value.as_deref()) {
        all_conditions_met = false;
        break;
      }
    }

    // Move the branch's operations out so their keys and values can be
    // consumed without cloning.
    let (if_len, else_len) = (req.if_then.len(), req.else_then.len());
    let ops_to_execute = if all_conditions_met {
      req.if_then
    } else {
      req.else_then
    };

    let prev_values = if req.return_previous {
      let mut values = Vec::with_capacity(ops_to_execute.len());
      for kv in &ops_to_execute {
        values.push(
          self
            .get_kv_with_overlay(&kv.key, pending)?
            .map(|b| b.to_vec()),
        );
      }
      values
    } else {
      Vec::new()
    };

    for kv in ops_to_execute {
      self.stage_upsert_kv(kv, batch, pending);
    }

    debug!(
      "Applied transaction: conditions_met={}, if_then_ops={}, else_then_ops={}",
      all_conditions_met, if_len, else_len
    );

    Ok(AppliedState::Txn(TxnReply::Success {
      branch: all_conditions_met,
      prev_values,
    }))
  }

  /// Read a value that may still be buffered in the current apply batch.
  ///
  /// Within `apply()`, all writes are accumulated into a single WriteBatch that
  /// is only committed after the whole batch is processed. Reads via RocksDB
  /// `get_cf` therefore cannot see writes from earlier entries in the same
  /// batch. This overlay resolves reads against the pending batch first
  /// (`None` entry = delete pending), falling back to the committed DB state.
  ///
  /// Without this, transaction condition checks could take a different branch
  /// depending on how the apply stream is chunked into batches — leader and
  /// followers may chunk differently (restart recovery, log catch-up) and
  /// would diverge.
  fn get_kv_with_overlay(
    &self,
    key: &str,
    pending: &PendingWrites,
  ) -> Result<Option<Bytes>, io::Error> {
    if let Some(pending_value) = pending.get(key) {
      return Ok(pending_value.clone());
    }
    Ok(self.get_kv(key)?.map(Bytes::from))
  }

  /// Publish the snapshot's apply progress and membership into the in-memory
  /// cache.
  ///
  /// The database copy is written by `recover_snapshot` in the same
  /// `WriteBatch` as the KV data, so a crash can never leave `applied_state()`
  /// describing a position the KV state has not reached (or vice versa). This
  /// only updates the cache, after that batch has been committed.
  fn publish_snapshot_meta(&self, meta: &SnapshotMeta) -> Result<(), io::Error> {
    // Rebuild the nodes table from the snapshot membership so both views agree
    // (same invariant as applying a Membership log entry).
    let nodes: BTreeMap<NodeId, Node> = meta
      .last_membership
      .membership()
      .nodes()
      .map(|(id, node)| (*id, node.clone()))
      .collect();

    // A poisoned lock means a previous holder panicked; the data is still
    // sound, and turning that into a storage error would stop the raft core.
    let mut sys_data = self
      .sys_data
      .lock()
      .unwrap_or_else(std::sync::PoisonError::into_inner);
    sys_data.last_applied = meta.last_log_id;
    sys_data.last_membership = Some(meta.last_membership.clone());
    sys_data.nodes = nodes;

    Ok(())
  }
}

impl RaftSnapshotBuilder<TypeConfig> for RocksStateMachine {
  async fn build_snapshot(&mut self) -> Result<Snapshot, io::Error> {
    let last_applied_log = self.get_last_applied_log_id()?;
    let last_membership = self.get_last_membership()?;

    build_snapshot(
      &self.db,
      &self.snapshot_dir,
      last_applied_log,
      last_membership,
    )
    .await
  }
}

impl RaftStateMachine<TypeConfig> for RocksStateMachine {
  type SnapshotBuilder = Self;

  async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
    let last_applied_log = self.get_last_applied_log_id()?;
    let last_membership = self.get_last_membership()?;

    Ok((last_applied_log, last_membership))
  }

  async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
  where
    Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
  {
    let mut batch = rocksdb::WriteBatch::default();
    let mut last_applied_log_id = None;
    let mut responses = Vec::new();
    let mut pending_writes = PendingWrites::new();
    // Metadata this batch changes, staged in memory: the rafts node table and
    // the applied membership. `apply()` writes them into the same WriteBatch as
    // the KV data and publishes them to `sys_data` only after that write
    // succeeds, so neither can ever run ahead of the data they describe.
    let mut sys_data = self.lock_sys_data()?.clone();

    while let Some((entry, responder)) = entries.try_next().await? {
      let log_id = entry.log_id;
      last_applied_log_id = Some(log_id);

      let response = match entry.payload {
        EntryPayload::Blank => AppliedState::None,
        EntryPayload::Normal(req) => {
          self.apply_cmd(req.cmd, &mut batch, &mut pending_writes, &mut sys_data)?
        }
        EntryPayload::Membership(membership) => {
          self.apply_membership(log_id, membership, &mut sys_data)?;
          AppliedState::None
        }
      };

      if let Some(responder) = responder {
        responses.push((responder, response));
      }
    }

    // Persist the apply progress into the SAME WriteBatch as the data, so a
    // crash can never leave data applied without the matching progress record
    // (which would re-apply committed entries on restart and replay Txn
    // responses with wrong prev_values). The metadata the batch changed goes
    // in with it, for the same reason.
    if let Some(last_applied_log_id) = last_applied_log_id {
      let data = encode(&last_applied_log_id).map_err(read_logs_err)?;
      batch.put_cf(&self.cf_sm_meta(), LAST_APPLIED_LOG_KEY, data);

      let committed = self.lock_sys_data()?.clone();
      if sys_data.nodes != committed.nodes {
        let data = encode(&sys_data.nodes).map_err(read_logs_err)?;
        batch.put_cf(&self.cf_sm_meta(), NODES_KEY, data);
      }
      if let Some(membership) = &sys_data.last_membership
        && sys_data.last_membership != committed.last_membership
      {
        // Stored on its own, without the `Option` wrapper `recover_sys_data()`
        // expects to find in the column family.
        let data = encode(membership).map_err(read_logs_err)?;
        batch.put_cf(&self.cf_sm_meta(), LAST_MEMBERSHIP_KEY, data);
      }

      sys_data.last_applied = Some(last_applied_log_id);
    }

    let applied = last_applied_log_id.is_some();

    // Atomic write of all data + apply progress + metadata
    self.db.write(batch).map_err(read_logs_err)?;

    // Publish to the in-memory cache only after a successful write; skip
    // entirely for an empty batch (last_applied_log_id is None) — DB state
    // unchanged.
    if applied {
      *self.lock_sys_data()? = sys_data;
    }

    // Only send responses after successful write
    for (responder, response) in responses {
      responder.send(response);
    }

    Ok(())
  }

  async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
    self.clone()
  }

  /// A blank, writable file for an incoming snapshot.
  ///
  /// The snapshot's own id only arrives with the data, so the file is named
  /// after a unique receiving id instead. Nothing has to delete it here:
  /// `install_snapshot` consumes the handle, and the startup sweep removes any
  /// `_incomplete` file a process left behind.
  async fn begin_receiving_snapshot(&mut self) -> Result<SnapshotDataOf<TypeConfig>, io::Error> {
    let std_file = self.create_snapshot_temp_file(&receiving_snapshot_id())?;
    Ok(tokio::fs::File::from_std(std_file))
  }

  async fn install_snapshot(
    &mut self,
    meta: &SnapshotMeta,
    snapshot: SnapshotDataOf<TypeConfig>,
  ) -> Result<(), io::Error> {
    recover_snapshot(
      &self.db,
      Snapshot {
        meta: meta.clone(),
        snapshot,
      },
      self.max_snapshot_entry_bytes,
    )
    .await?;

    // `recover_snapshot` committed the KV data and _sm_meta together; publish
    // the same values to the in-memory cache so live reads agree with the DB.
    self.publish_snapshot_meta(meta)
  }

  /// Return whichever snapshot the state machine holds.
  ///
  /// There is deliberately no `last_applied` check here: a snapshot the state
  /// machine has since moved past is exactly what a lagging follower needs --
  /// OpenRaft installs it, then replays the entries between the snapshot and
  /// the leader's log. OpenRaft judges whether a snapshot is new enough by
  /// comparing `meta.last_log_id` with the follower's progress.
  async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, io::Error> {
    get_current_snapshot(&self.snapshot_dir).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use std::collections::HashMap;

  use crate::engine::RocksDBEngine;
  use crate::raft::store::keys::LOG_DATA_FAMILY;
  use crate::raft::store::keys::LOG_META_FAMILY;
  use crate::raft::types::LogEntry;
  use crate::raft::types::TxnCondition;
  use crate::raft::types::TxnReq;
  use crate::raft::types::{Endpoint, Node};
  use crate::utils::test::{create_log_id, create_test_state_machine};
  use futures::stream;
  use openraft::Entry;
  use openraft::EntryPayload;
  use openraft::Membership;
  use openraft::storage::EntryResponder;
  use tokio::io::AsyncReadExt;
  use tokio::io::AsyncSeekExt;
  use tokio::io::AsyncWriteExt;

  use crate::raft::types::TypeConfig;

  /// Build an apply-stream item carrying the given command.
  fn create_apply_entry(index: u64, cmd: Cmd) -> EntryResponder<TypeConfig> {
    let entry = Entry {
      log_id: create_log_id(1, 1, index),
      payload: EntryPayload::Normal(LogEntry::new(cmd)),
    };
    (entry, None)
  }

  /// Apply entries as one batch.
  async fn apply_entries(sm: &mut RocksStateMachine, items: Vec<EntryResponder<TypeConfig>>) {
    sm.apply(stream::iter(items.into_iter().map(Ok)))
      .await
      .unwrap();
  }

  #[tokio::test]
  async fn test_set_and_get_last_applied() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    assert!(sm.get_last_applied_log_id()?.is_none());

    let log_id = create_log_id(1, 1, 100);
    sm.set_last_applied_log_id(Some(log_id))?;

    let retrieved = sm.get_last_applied_log_id()?.unwrap();
    assert_eq!(retrieved.leader_id.term, log_id.leader_id.term);
    assert_eq!(retrieved.leader_id.node_id, log_id.leader_id.node_id);
    assert_eq!(retrieved.index, log_id.index);

    let new_log_id = create_log_id(2, 2, 200);
    sm.set_last_applied_log_id(Some(new_log_id))?;

    let updated = sm.get_last_applied_log_id()?.unwrap();
    assert_eq!(updated.leader_id.term, 2);
    assert_eq!(updated.leader_id.node_id, 2);
    assert_eq!(updated.index, 200);

    sm.set_last_applied_log_id(None)?;
    assert!(sm.get_last_applied_log_id()?.is_none());

    Ok(())
  }

  #[tokio::test]
  async fn test_recover_sys_data_last_applied() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();

    // Create state machine and write data
    let sm1 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    let log_id = create_log_id(3, 5, 300);
    sm1.set_last_applied_log_id(Some(log_id))?;

    // Create a new state machine instance from the same database
    // This should recover the data from the database
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    let recovered = sm2.get_last_applied_log_id()?;
    assert!(recovered.is_some());

    let recovered_log_id = recovered.unwrap();
    assert_eq!(recovered_log_id.leader_id.term, log_id.leader_id.term);
    assert_eq!(recovered_log_id.leader_id.node_id, log_id.leader_id.node_id);
    assert_eq!(recovered_log_id.index, log_id.index);

    // Update the data again
    let new_log_id = create_log_id(4, 6, 400);
    sm2.set_last_applied_log_id(Some(new_log_id))?;

    // Create another instance and verify the updated data
    let sm3 = RocksStateMachine::new(engine.db().clone(), temp_data_dir)
      .await
      .unwrap();

    let updated = sm3.get_last_applied_log_id()?.unwrap();
    assert_eq!(updated.leader_id.term, 4);
    assert_eq!(updated.leader_id.node_id, 6);
    assert_eq!(updated.index, 400);

    Ok(())
  }

  #[tokio::test]
  async fn test_recover_sys_data_nodes() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();

    // Create state machine and add nodes
    let sm1 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    let node1 = Node {
      node_id: 1,
      endpoint: Endpoint::new("127.0.0.1", 8081),
    };
    sm1.add_node(node1.clone())?;

    let node2 = Node {
      node_id: 2,
      endpoint: Endpoint::new("127.0.0.1", 8082),
    };
    sm1.add_node(node2)?;

    // Create a new state machine instance from the same database
    let mut sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    // Verify nodes are recovered. The guard is scoped: holding it across the
    // `await`s below trips `clippy::await_holding_lock` and, worse, blocks any
    // other task that needs the metadata for the duration of the await.
    {
      let sys_data = sm2
        .sys_data
        .lock()
        .expect("Mutex lock should not be poisoned");
      assert_eq!(sys_data.nodes.len(), 2);
      assert!(sys_data.nodes.contains_key(&1));
      assert!(sys_data.nodes.contains_key(&2));

      let recovered_node1 = sys_data.nodes.get(&1).unwrap();
      assert_eq!(recovered_node1.node_id, 1);
      assert_eq!(recovered_node1.endpoint.addr(), "127.0.0.1");
      assert_eq!(recovered_node1.endpoint.port(), 8081);

      let recovered_node2 = sys_data.nodes.get(&2).unwrap();
      assert_eq!(recovered_node2.node_id, 2);
      assert_eq!(recovered_node2.endpoint.addr(), "127.0.0.1");
      assert_eq!(recovered_node2.endpoint.port(), 8082);
    }

    // Remove a node through the apply path, which is the only writer now
    apply_entries(
      &mut sm2,
      vec![create_apply_entry(50, Cmd::RemoveNode { node_id: 1 })],
    )
    .await;

    // Create another instance and verify
    let sm3 = RocksStateMachine::new(engine.db().clone(), temp_data_dir)
      .await
      .unwrap();

    let sys_data = sm3
      .sys_data
      .lock()
      .expect("Mutex lock should not be poisoned");
    assert_eq!(sys_data.nodes.len(), 1);
    assert!(!sys_data.nodes.contains_key(&1));
    assert!(sys_data.nodes.contains_key(&2));

    Ok(())
  }

  #[tokio::test]
  async fn test_apply_txn_condition_reads_pending_writes_in_same_batch() -> Result<(), io::Error> {
    let mut sm = create_test_state_machine().await;

    // Two Txn entries applied in one batch: the second txn's condition must
    // see the value written by the first txn, even though the WriteBatch is
    // not committed until the whole apply stream completes.
    // Each txn records its branch decision into a distinct marker key.
    let txn1 = Cmd::Txn {
      req: TxnReq::new(vec![TxnCondition::not_exists("txn_chain")])
        .if_then(UpsertKV::insert("txn_chain", b"first"))
        .if_then(UpsertKV::insert("txn1_branch", b"if")),
    };
    let txn2 = Cmd::Txn {
      req: TxnReq::new(vec![TxnCondition::eq("txn_chain", b"first")])
        .if_then(UpsertKV::insert("txn_chain", b"second"))
        .if_then(UpsertKV::insert("txn2_branch", b"if")),
    };

    apply_entries(
      &mut sm,
      vec![create_apply_entry(1, txn1), create_apply_entry(2, txn2)],
    )
    .await;

    // txn2's condition saw txn1's pending write -> both took the if_then branch
    assert_eq!(sm.get_kv("txn1_branch")?, Some(b"if".to_vec()));
    assert_eq!(sm.get_kv("txn2_branch")?, Some(b"if".to_vec()));
    // Final state reflects both applies in order
    assert_eq!(sm.get_kv("txn_chain")?, Some(b"second".to_vec()));

    Ok(())
  }

  #[tokio::test]
  async fn test_apply_txn_condition_sees_delete_in_same_batch() -> Result<(), io::Error> {
    let mut sm = create_test_state_machine().await;

    // Pre-populate committed state
    let seed = Cmd::UpsertKV(UpsertKV::insert("del_key", b"v"));
    apply_entries(&mut sm, vec![create_apply_entry(1, seed)]).await;

    // Batch: [UpsertKV del_key=delete, Txn not_exists(del_key) -> insert]
    // The txn condition must see the pending delete, not the committed value.
    let delete = Cmd::UpsertKV(UpsertKV::delete("del_key"));
    let txn = Cmd::Txn {
      req: TxnReq::new(vec![TxnCondition::not_exists("del_key")])
        .if_then(UpsertKV::insert("del_key", b"reborn")),
    };

    apply_entries(
      &mut sm,
      vec![create_apply_entry(2, delete), create_apply_entry(3, txn)],
    )
    .await;

    assert_eq!(sm.get_kv("del_key")?, Some(b"reborn".to_vec()));

    Ok(())
  }

  #[tokio::test]
  async fn test_apply_txn_without_overlay_would_diverge() -> Result<(), io::Error> {
    let mut sm = create_test_state_machine().await;

    // Committed state has counter=1. In one batch: [UpsertKV counter=2, Txn eq(counter,"2")].
    // The condition must observe the pending write and take the if_then branch.
    let seed = Cmd::UpsertKV(UpsertKV::insert("counter", b"1"));
    apply_entries(&mut sm, vec![create_apply_entry(1, seed)]).await;

    let upsert = Cmd::UpsertKV(UpsertKV::insert("counter", b"2"));
    let txn = Cmd::Txn {
      req: TxnReq::new(vec![TxnCondition::eq("counter", b"2")])
        .if_then(UpsertKV::insert("result_key", b"if"))
        .else_then(UpsertKV::insert("result_key", b"else")),
    };

    apply_entries(
      &mut sm,
      vec![create_apply_entry(2, upsert), create_apply_entry(3, txn)],
    )
    .await;

    // With the overlay the condition sees "2" -> if branch.
    // Without it, the condition would read the committed "1" -> else branch.
    assert_eq!(sm.get_kv("result_key")?, Some(b"if".to_vec()));

    Ok(())
  }

  #[tokio::test]
  async fn test_apply_membership_updates_nodes_and_survives_reopen() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();

    let mut sm = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    // Apply a membership entry with voters {1, 2} and a learner 3.
    let mut voters = BTreeSet::new();
    voters.insert(1);
    voters.insert(2);
    let mut nodes = BTreeMap::new();
    nodes.insert(
      1,
      Node {
        node_id: 1,
        endpoint: Endpoint::new("127.0.0.1", 7001),
      },
    );
    nodes.insert(
      2,
      Node {
        node_id: 2,
        endpoint: Endpoint::new("127.0.0.1", 7002),
      },
    );
    nodes.insert(
      3,
      Node {
        node_id: 3,
        endpoint: Endpoint::new("127.0.0.1", 7003),
      },
    );
    let membership = Membership::new(vec![voters], nodes).unwrap();

    let entries = vec![(
      Entry {
        log_id: create_log_id(1, 1, 1),
        payload: EntryPayload::Membership(membership),
      },
      None,
    )];
    sm.apply(stream::iter(entries.into_iter().map(Ok)))
      .await
      .unwrap();

    // get_last_membership now comes from the stored entry: learners are kept
    // as learners, not promoted to voters, and the entry keeps its own log id
    // so openraft can order it against the memberships it tracks.
    let stored = sm.get_last_membership()?;
    let voter_ids: BTreeSet<_> = stored.membership().voter_ids().collect();
    assert_eq!(voter_ids, BTreeSet::from([1, 2]));
    assert_eq!(
      stored.log_id(),
      &Some(create_log_id(1, 1, 1)),
      "the applied membership must record the log id it came from"
    );

    // nodes table was rebuilt from the membership (learners included)
    let nodes_now = sm.get_nodes()?;
    assert_eq!(nodes_now.len(), 3);
    assert!(nodes_now.contains_key(&3));
    assert!(sm.contains_node(3)?);

    // Reopen: membership is recovered from SM_META, not re-synthesized
    drop(sm);
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir).await?;

    let recovered = sm2.get_last_membership()?;
    let recovered_voters: BTreeSet<_> = recovered.membership().voter_ids().collect();
    assert_eq!(recovered_voters, BTreeSet::from([1, 2]));
    assert_eq!(
      recovered.log_id(),
      &Some(create_log_id(1, 1, 1)),
      "the membership log id must survive a reopen"
    );

    let recovered_nodes = sm2.get_nodes()?;
    assert_eq!(recovered_nodes.len(), 3);

    Ok(())
  }

  /// Re-applying the same membership at a new log id must still be persisted:
  /// `StoredMembership`'s equality includes its log id, so the staged
  /// `LAST_MEMBERSHIP_KEY` write is not skipped as a no-op.
  #[tokio::test]
  async fn test_membership_log_id_updates_on_reapply_and_survives_reopen() -> Result<(), io::Error>
  {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir.to_string_lossy(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();
    let mut sm = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    let mut voters = BTreeSet::new();
    voters.insert(1);
    voters.insert(2);
    let mut nodes = BTreeMap::new();
    for node_id in 1..=2u64 {
      nodes.insert(
        node_id,
        Node {
          node_id,
          endpoint: Endpoint::new("127.0.0.1", 7000 + node_id as u32),
        },
      );
    }
    let membership = Membership::new(vec![voters], nodes).unwrap();

    let first_log_id = create_log_id(1, 1, 10);
    let second_log_id = create_log_id(2, 2, 20);

    for log_id in [first_log_id, second_log_id] {
      let entries = vec![(
        Entry {
          log_id,
          payload: EntryPayload::Membership(membership.clone()),
        },
        None,
      )];
      sm.apply(stream::iter(entries.into_iter().map(Ok)))
        .await
        .unwrap();

      let stored = sm.get_last_membership()?;
      assert_eq!(
        stored.log_id(),
        &Some(log_id),
        "the live membership must follow the entry it was applied from"
      );
    }

    // Reopen: the second log id is the one staged into SM_META. If the staged
    // write compared only membership content, it would have been skipped as a
    // no-op and this would still hold the first log id.
    drop(sm);
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir).await?;
    let recovered = sm2.get_last_membership()?;
    assert_eq!(
      recovered.log_id(),
      &Some(second_log_id),
      "re-applying the same membership at a new log id must update the stored position"
    );

    Ok(())
  }

  /// Test Point: install_snapshot syncs last_applied and membership into
  /// _sm_meta so applied_state()/get_last_membership() agree with the
  /// restored KV data after reopen.
  #[tokio::test]
  async fn test_install_snapshot_syncs_meta() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![
        LOG_META_FAMILY.to_string(),
        LOG_DATA_FAMILY.to_string(),
        SM_META_FAMILY.to_string(),
        SM_DATA_FAMILY.to_string(),
      ],
    )
    .unwrap();

    let mut sm = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    // Pre-state: an old applied position and a one-node membership
    let old_log_id = create_log_id(1, 1, 5);
    sm.set_last_applied_log_id(Some(old_log_id))?;
    let old_node = Node {
      node_id: 1,
      endpoint: Endpoint::new("127.0.0.1", 7001),
    };
    sm.add_node(old_node)?;

    // A snapshot with a NEWER position and an EXTENDED membership
    // (node 2 joins, node 1's address changes) — snapshot meta carries both.
    let snap_log_id = create_log_id(2, 2, 100);
    let mut voters = BTreeSet::new();
    voters.insert(1);
    voters.insert(2);
    let mut snap_nodes = BTreeMap::new();
    snap_nodes.insert(
      1,
      Node {
        node_id: 1,
        endpoint: Endpoint::new("127.0.0.1", 7001),
      },
    );
    snap_nodes.insert(
      2,
      Node {
        node_id: 2,
        endpoint: Endpoint::new("127.0.0.1", 7002),
      },
    );
    let membership = Membership::new(vec![voters], snap_nodes).unwrap();
    let meta = SnapshotMeta {
      last_log_id: Some(snap_log_id),
      last_membership: StoredMembership::new(None, membership),
      snapshot_id: "sync-meta-snapshot".to_string(),
    };

    // A snapshot data file (empty KV payload is fine — meta sync is the subject)
    let snap_dir = temp_data_dir.join("snapshot").join(&meta.snapshot_id);
    tokio::fs::create_dir_all(&snap_dir).await.unwrap();
    let snap_data_path = snap_dir.join("snapshot");
    let file = std::fs::File::create(&snap_data_path).unwrap();
    let encoder = zstd::Encoder::new(file, 3).unwrap();
    encoder.finish().unwrap();

    // install_snapshot: recover KV + sync meta
    let snap_file = tokio::fs::File::open(&snap_data_path).await.unwrap();
    sm.install_snapshot(&meta, snap_file).await.unwrap();

    // In-memory view agrees with the snapshot meta
    assert_eq!(sm.get_last_applied_log_id()?, Some(snap_log_id));
    let after = sm.get_last_membership()?;
    let after_voters: BTreeSet<_> = after.membership().voter_ids().collect();
    assert_eq!(after_voters, BTreeSet::from([1, 2]));
    let after_nodes = sm.get_nodes()?;
    assert!(after_nodes.contains_key(&2));

    // Reopen: recovered state agrees with the installed snapshot
    drop(sm);
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir).await?;

    assert_eq!(sm2.get_last_applied_log_id()?, Some(snap_log_id));
    let recovered = sm2.get_last_membership()?;
    let recovered_voters: BTreeSet<_> = recovered.membership().voter_ids().collect();
    assert_eq!(recovered_voters, BTreeSet::from([1, 2]));
    assert_eq!(
      recovered.log_id(),
      &None,
      "the membership is stored exactly as the snapshot meta carries it"
    );
    assert_eq!(sm2.get_nodes()?.len(), 2);

    Ok(())
  }

  #[tokio::test]
  async fn test_recover_sys_data_all_fields() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir
        .clone()
        .into_os_string()
        .into_string()
        .unwrap(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();

    // Create state machine and write all fields
    let sm1 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    // Write last_applied
    let log_id = create_log_id(7, 9, 700);
    sm1.set_last_applied_log_id(Some(log_id))?;

    // Write nodes
    let node1 = Node {
      node_id: 1,
      endpoint: Endpoint::new("127.0.0.1", 9091),
    };
    sm1.add_node(node1)?;

    let node2 = Node {
      node_id: 2,
      endpoint: Endpoint::new("127.0.0.1", 9092),
    };
    sm1.add_node(node2)?;

    let node3 = Node {
      node_id: 3,
      endpoint: Endpoint::new("127.0.0.1", 9093),
    };
    sm1.add_node(node3)?;

    // Create a new state machine instance and verify all data is recovered
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    // Verify last_applied
    let recovered_log_id = sm2.get_last_applied_log_id()?.unwrap();
    assert_eq!(recovered_log_id.leader_id.term, 7);
    assert_eq!(recovered_log_id.leader_id.node_id, 9);
    assert_eq!(recovered_log_id.index, 700);

    // Verify membership is constructed from nodes
    let recovered_membership = sm2.get_last_membership()?;
    let voter_ids: BTreeSet<_> = recovered_membership.membership().voter_ids().collect();
    assert_eq!(voter_ids.len(), 3);
    assert!(voter_ids.contains(&1));
    assert!(voter_ids.contains(&2));
    assert!(voter_ids.contains(&3));

    // Verify nodes
    let sys_data = sm2
      .sys_data
      .lock()
      .expect("Mutex lock should not be poisoned");
    assert_eq!(sys_data.nodes.len(), 3);
    assert!(sys_data.nodes.contains_key(&1));
    assert!(sys_data.nodes.contains_key(&2));
    assert!(sys_data.nodes.contains_key(&3));

    // Verify node 3
    let recovered_node3 = sys_data.nodes.get(&3).unwrap();
    assert_eq!(recovered_node3.node_id, 3);
    assert_eq!(recovered_node3.endpoint.addr(), "127.0.0.1");
    assert_eq!(recovered_node3.endpoint.port(), 9093);

    Ok(())
  }

  /// The entry bound is a property of the node, not a constant: a state
  /// machine created with a small limit must refuse a snapshot the default
  /// limit would have accepted.
  #[tokio::test]
  async fn test_install_snapshot_honours_the_entry_limit() -> Result<(), io::Error> {
    use std::io::Write as _;

    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir.to_string_lossy(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();
    let mut sm = RocksStateMachine::new_with_snapshot_entry_limit(
      engine.db().clone(),
      temp_data_dir.clone(),
      4,
    )
    .await
    .unwrap();

    let payload = {
      let mut out = Vec::new();
      let mut encoder = zstd::Encoder::new(&mut out, 3).unwrap();
      encoder.write_all(&1u32.to_le_bytes()).unwrap();
      encoder.write_all(b"k").unwrap();
      // a five-byte value: one byte past this state machine's limit
      encoder.write_all(&5u32.to_le_bytes()).unwrap();
      encoder.write_all(b"abcde").unwrap();
      encoder.finish().unwrap();
      out
    };

    let path = temp_data_dir.join("limited.snapshot");
    std::fs::write(&path, payload)?;

    let meta = SnapshotMeta {
      last_log_id: None,
      last_membership: StoredMembership::default(),
      snapshot_id: "limited".to_string(),
    };
    let file = tokio::fs::File::open(&path).await.unwrap();
    let err = sm.install_snapshot(&meta, file).await.unwrap_err();

    assert!(
      err.to_string().contains("exceeds the 4 byte limit"),
      "the install must use the state machine's own limit, got: {}",
      err
    );

    Ok(())
  }

  /// A snapshot the state machine has already moved past is still the one a
  /// lagging follower has to receive: OpenRaft installs it and replays the
  /// entries between it and the leader's log. Reporting "no snapshot" until the
  /// next scheduled build delays that follower's catch-up by a whole
  /// `snapshot_policy`.
  #[tokio::test]
  async fn test_get_current_snapshot_survives_entries_applied_after_it() -> Result<(), io::Error> {
    let mut sm = create_test_state_machine().await;

    apply_entries(
      &mut sm,
      vec![create_apply_entry(
        1,
        Cmd::UpsertKV(UpsertKV::insert("key", b"v1")),
      )],
    )
    .await;

    let snapshot = sm.build_snapshot().await?;
    let snapshot_log_id = snapshot.meta.last_log_id;
    assert_eq!(
      snapshot_log_id,
      sm.get_last_applied_log_id()?,
      "the snapshot must cover exactly the applied log"
    );

    apply_entries(
      &mut sm,
      vec![create_apply_entry(
        2,
        Cmd::UpsertKV(UpsertKV::insert("key", b"v2")),
      )],
    )
    .await;
    assert!(
      sm.get_last_applied_log_id()? > snapshot_log_id,
      "the state machine has to be past its snapshot for this test to mean anything"
    );

    let current = sm.get_current_snapshot().await?;
    assert_eq!(
      current
        .expect("a snapshot older than last_applied is still the current snapshot")
        .meta
        .last_log_id,
      snapshot_log_id
    );

    Ok(())
  }

  /// `begin_receiving_snapshot` hands OpenRaft an empty, writable file to put
  /// an incoming snapshot into. It is the receiving side of the trait: handing
  /// back the node's own current snapshot gives the caller data it never wrote,
  /// and failing when there is no snapshot yet refuses a transfer that is
  /// perfectly possible on a fresh node.
  #[tokio::test]
  async fn test_begin_receiving_snapshot_hands_out_a_blank_writable_file() -> Result<(), io::Error>
  {
    // A kept temp dir: the receive path has to create a file in it.
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir.to_string_lossy(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();
    let mut sm = RocksStateMachine::new(engine.db().clone(), temp_data_dir)
      .await
      .unwrap();

    // A node that has never built a snapshot must still be able to receive one.
    let mut data = sm.begin_receiving_snapshot().await?;
    assert!(
      data.read_to_end(&mut Vec::new()).await? == 0,
      "the handle must start empty"
    );

    data.write_all(b"snapshot-bytes").await?;
    data.seek(std::io::SeekFrom::Start(0)).await?;
    let mut received = Vec::new();
    data.read_to_end(&mut received).await?;
    assert_eq!(
      received, b"snapshot-bytes",
      "the handle must accept the transfer"
    );

    // With a snapshot on disk, the handle must still be blank: the current
    // snapshot is read-only for this purpose.
    apply_entries(
      &mut sm,
      vec![create_apply_entry(
        1,
        Cmd::UpsertKV(UpsertKV::insert("key", b"v1")),
      )],
    )
    .await;
    sm.build_snapshot().await?;

    let mut data = sm.begin_receiving_snapshot().await?;
    let mut received = Vec::new();
    data.read_to_end(&mut received).await?;
    assert!(
      received.is_empty(),
      "the handle must not expose the current snapshot, got {} bytes",
      received.len()
    );

    Ok(())
  }

  /// The snapshot-receive path assembles a transfer in a temp file and hands
  /// that same handle to `install_snapshot`, which reads it back. The handle
  /// must therefore be opened read+write, and its directory must exist even on
  /// a node that has never built a snapshot itself.
  #[tokio::test]
  async fn test_snapshot_temp_file_is_readable_on_a_fresh_node() -> Result<(), io::Error> {
    // A kept temp dir: the receive path has to create files in it, so it must
    // outlive the constructor that produced the state machine.
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir.to_string_lossy(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();
    let sm = RocksStateMachine::new(engine.db().clone(), temp_data_dir)
      .await
      .unwrap();

    let id = "streamed";
    let path = sm.snapshot_temp_file_path(id);
    assert!(!path.exists(), "fresh node must not have the temp file yet");

    let std_file = sm.create_snapshot_temp_file(id)?;
    let mut file = tokio::fs::File::from_std(std_file);

    let payload = b"snapshot-bytes";
    let mut offset = 0u64;
    for chunk in payload.chunks(4) {
      file.seek(std::io::SeekFrom::Start(offset)).await?;
      file.write_all(chunk).await?;
      offset += chunk.len() as u64;
    }

    file.seek(std::io::SeekFrom::Start(0)).await?;
    let mut got = Vec::new();
    file.read_to_end(&mut got).await?;
    assert_eq!(got, payload, "the handle must be readable, not write-only");

    assert_eq!(path, sm.snapshot_temp_file_path(id));
    assert!(path.exists());

    Ok(())
  }

  /// `Cmd::AddNode` used to write the node table straight to RocksDB, ahead of
  /// the WriteBatch that carries the rest of the entry. It is staged now and
  /// written with the batch, so reopening after the batch sees it alongside the
  /// data the same batch applied.
  #[tokio::test]
  async fn test_apply_add_node_is_persisted_with_the_batch() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir.to_string_lossy(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();
    let mut sm = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone())
      .await
      .unwrap();

    let node = Node {
      node_id: 7,
      endpoint: Endpoint::new("127.0.0.1", 7007),
    };
    apply_entries(
      &mut sm,
      vec![
        create_apply_entry(
          1,
          Cmd::AddNode {
            node: node.clone(),
            overriding: true,
          },
        ),
        create_apply_entry(2, Cmd::UpsertKV(UpsertKV::insert("k", b"v"))),
      ],
    )
    .await;

    assert_eq!(sm.get_nodes()?.get(&7), Some(&node));

    drop(sm);
    let sm2 = RocksStateMachine::new(engine.db().clone(), temp_data_dir).await?;
    assert_eq!(
      sm2.get_nodes()?.get(&7),
      Some(&node),
      "the node table must land with the batch that applied it"
    );
    assert_eq!(sm2.get_kv("k")?, Some(b"v".to_vec()));
    assert!(sm2.get_last_applied_log_id()?.is_some());

    Ok(())
  }

  /// The snapshot id comes from the peer that streams the snapshot and is used
  /// as a file name; anything that could step out of the snapshot directory
  /// must be rejected before it reaches the filesystem.
  #[test]
  fn test_check_snapshot_id_rejects_path_escapes() {
    for bad in [
      "",
      ".",
      "..",
      "../escape",
      "a/b",
      "/etc/passwd",
      "a\\b",
      "id with space",
      "id_with_newline\n",
    ] {
      assert!(
        check_snapshot_id(bad).is_err(),
        "snapshot id {:?} must be rejected",
        bad
      );
    }

    for good in [
      "T3-N5-100-1234567890-1",
      "0-0-1700000000000-7",
      "snapshot_001",
      "a.b",
    ] {
      assert!(
        check_snapshot_id(good).is_ok(),
        "snapshot id {:?} must be accepted",
        good
      );
    }
  }

  #[tokio::test]
  async fn test_create_snapshot_temp_file_rejects_escaping_id() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    let err = sm
      .create_snapshot_temp_file("../../../etc/passwd")
      .expect_err("an escaping id must not reach the filesystem");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

    Ok(())
  }

  /// `Cmd::AddNode` carries an `overriding` flag: the join path sets it to
  /// false so a stale `AddNode` replayed from the log cannot rewrite the
  /// endpoint of a node that already registered.
  #[tokio::test]
  async fn test_apply_add_node_honours_overriding() -> Result<(), io::Error> {
    let mut sm = create_test_state_machine().await;

    let original = Node {
      node_id: 3,
      endpoint: Endpoint::new("127.0.0.1", 7003),
    };
    let replacement = Node {
      node_id: 3,
      endpoint: Endpoint::new("127.0.0.1", 9999),
    };

    let add = |index: u64, node: &Node, overriding: bool| {
      create_apply_entry(
        index,
        Cmd::AddNode {
          node: node.clone(),
          overriding,
        },
      )
    };

    apply_entries(&mut sm, vec![add(1, &original, true)]).await;
    assert_eq!(sm.get_nodes()?.get(&3), Some(&original));

    // overriding = false keeps the record that is already there
    apply_entries(&mut sm, vec![add(2, &replacement, false)]).await;
    assert_eq!(sm.get_nodes()?.get(&3), Some(&original));

    // overriding = true replaces it
    apply_entries(&mut sm, vec![add(3, &replacement, true)]).await;
    assert_eq!(sm.get_nodes()?.get(&3), Some(&replacement));

    Ok(())
  }
  #[tokio::test]
  async fn test_scan_prefix_basic() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    // Insert test data with different prefixes
    let cf_data = sm.cf_sm_data();
    sm.db
      .put_cf(&cf_data, b"user:1:name", b"Alice")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"user:1:age", b"25")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"user:2:name", b"Bob")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"user:2:age", b"30")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"product:1:name", b"Laptop")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"product:1:price", b"999")
      .map_err(read_logs_err)?;

    // Scan with "user:" prefix
    let user_results = sm.scan_prefix(b"user:")?;
    assert_eq!(user_results.len(), 4);

    // Verify all user keys are returned
    let keys: Vec<_> = user_results
      .iter()
      .map(|(k, _)| String::from_utf8_lossy(k).to_string())
      .collect();
    assert!(keys.contains(&"user:1:name".to_string()));
    assert!(keys.contains(&"user:1:age".to_string()));
    assert!(keys.contains(&"user:2:name".to_string()));
    assert!(keys.contains(&"user:2:age".to_string()));

    // Scan with "user:1:" prefix
    let user1_results = sm.scan_prefix(b"user:1:")?;
    assert_eq!(user1_results.len(), 2);

    let user1_keys: Vec<_> = user1_results
      .iter()
      .map(|(k, _)| String::from_utf8_lossy(k).to_string())
      .collect();
    assert!(user1_keys.contains(&"user:1:name".to_string()));
    assert!(user1_keys.contains(&"user:1:age".to_string()));

    // Scan with "product:" prefix
    let product_results = sm.scan_prefix(b"product:")?;
    assert_eq!(product_results.len(), 2);

    Ok(())
  }

  #[tokio::test]
  async fn test_scan_prefix_empty_result() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    // Insert test data
    let cf_data = sm.cf_sm_data();
    sm.db
      .put_cf(&cf_data, b"key1", b"value1")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"key2", b"value2")
      .map_err(read_logs_err)?;

    // Scan with non-existent prefix
    let results = sm.scan_prefix(b"nonexistent:")?;
    assert!(results.is_empty());

    Ok(())
  }

  #[tokio::test]
  async fn test_scan_prefix_binary_keys() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    // Insert binary data with prefixes
    let cf_data = sm.cf_sm_data();
    sm.db
      .put_cf(&cf_data, [0x01, 0x00, 0x01], b"data1")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, [0x01, 0x00, 0x02], b"data2")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, [0x01, 0x01, 0x01], b"data3")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, [0x02, 0x00, 0x01], b"data4")
      .map_err(read_logs_err)?;

    // Scan with prefix [0x01, 0x00]
    let results = sm.scan_prefix(&[0x01, 0x00])?;
    assert_eq!(results.len(), 2);

    // Verify correct data is returned
    let values: Vec<_> = results.iter().map(|(_, v)| v.clone()).collect();
    assert!(values.contains(&b"data1".to_vec()));
    assert!(values.contains(&b"data2".to_vec()));

    Ok(())
  }

  #[tokio::test]
  async fn test_scan_prefix_values_match() -> Result<(), io::Error> {
    let sm = create_test_state_machine().await;

    // Insert test data
    let cf_data = sm.cf_sm_data();
    sm.db
      .put_cf(&cf_data, b"config:host", b"localhost")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"config:port", b"8080")
      .map_err(read_logs_err)?;
    sm.db
      .put_cf(&cf_data, b"config:timeout", b"30")
      .map_err(read_logs_err)?;

    // Scan and verify values
    let results = sm.scan_prefix(b"config:")?;
    assert_eq!(results.len(), 3);

    // Convert to a map for easier verification
    let map: HashMap<_, _> = results.into_iter().collect();

    assert_eq!(
      map.get(b"config:host".as_slice()),
      Some(&b"localhost".to_vec())
    );
    assert_eq!(map.get(b"config:port".as_slice()), Some(&b"8080".to_vec()));
    assert_eq!(map.get(b"config:timeout".as_slice()), Some(&b"30".to_vec()));

    Ok(())
  }

  /// A restart must sweep `*_incomplete` files: the receive map is in-memory,
  /// so a file left by a previous process can never be resumed or evicted.
  #[tokio::test]
  async fn test_new_removes_leftover_incomplete_snapshot_files() -> Result<(), io::Error> {
    let temp_data_dir = tempfile::tempdir().unwrap().keep();
    let engine = RocksDBEngine::new(
      &temp_data_dir.to_string_lossy(),
      1024,
      vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
    )
    .unwrap();

    let snapshot_dir = temp_data_dir.join("snapshot");
    std::fs::create_dir_all(&snapshot_dir).unwrap();

    let leftover = snapshot_dir.join("leftover_incomplete");
    let kept = snapshot_dir.join("keep.txt");
    let built = snapshot_dir.join("1-1-123-0");
    std::fs::write(&leftover, b"partial").unwrap();
    std::fs::write(&kept, b"keep").unwrap();
    std::fs::create_dir_all(&built).unwrap();

    let _ = RocksStateMachine::new(engine.db().clone(), temp_data_dir.clone()).await?;

    assert!(!leftover.exists(), "a leftover transfer file must be swept");
    assert!(kept.exists(), "unrelated files must be kept");
    assert!(built.exists(), "built snapshot directories must be kept");

    Ok(())
  }
}
