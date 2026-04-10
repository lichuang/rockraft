//! Test utilities for RockRaft
//!
//! This module provides shared helper functions and fixtures for unit tests
//! across the codebase. It helps eliminate duplication and ensures consistency
//! in test setup.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use rocksdb::Options;
use tempfile::TempDir;

use crate::engine::RocksDBEngine;
use crate::raft::store::{
  RocksLogStore, RocksStateMachine,
  keys::{LOG_DATA_FAMILY, LOG_META_FAMILY, SM_DATA_FAMILY, SM_META_FAMILY},
};
use crate::raft::types::{Cmd, Entry, LeaderId, LogEntry, LogId, Operation, TypeConfig, UpsertKV};
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use rocksdb::DB;

/// Creates a `LogId` for testing purposes.
///
/// # Arguments
/// * `term` - The Raft term number
/// * `node_id` - The leader node ID
/// * `index` - The log entry index
///
/// # Example
/// ```
/// use rockraft::utils::test::create_log_id;
///
/// let log_id = create_log_id(1, 1, 100);
/// ```
pub fn create_log_id(term: u64, node_id: u64, index: u64) -> LogId {
  LogId {
    leader_id: LeaderId { term, node_id },
    index,
  }
}

/// Test fixture that owns a temporary directory for RocksDB storage.
///
/// This ensures the temp directory is cleaned up when the test completes.
pub struct TestStorageFixture {
  #[allow(dead_code)]
  temp_dir: TempDir,
  path: PathBuf,
}

impl TestStorageFixture {
  /// Creates a new test fixture with a temporary directory.
  pub fn new() -> Self {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_path_buf();
    Self { temp_dir, path }
  }

  /// Returns the path to the temporary directory.
  pub fn path(&self) -> &PathBuf {
    &self.path
  }

  /// Returns the path as a string.
  pub fn path_str(&self) -> String {
    self.path.to_string_lossy().to_string()
  }
}

/// Creates a test `RocksLogStore` with a temporary database.
///
/// The database is created in a temporary directory that will be cleaned up
/// when the test completes.
pub fn create_test_log_store() -> RocksLogStore<TypeConfig> {
  let fixture = TestStorageFixture::new();
  create_test_log_store_with_path(&fixture.path_str())
}

/// Creates a test `RocksLogStore` at the specified path.
///
/// # Arguments
/// * `path` - The directory path for the RocksDB database
pub fn create_test_log_store_with_path(path: &str) -> RocksLogStore<TypeConfig> {
  let mut opts = Options::default();
  opts.create_if_missing(true);
  opts.create_missing_column_families(true);

  let db = DB::open_cf(&opts, path, vec![LOG_META_FAMILY, LOG_DATA_FAMILY]).unwrap();

  RocksLogStore::create(Arc::new(db)).unwrap()
}

/// Creates a test `RocksStateMachine` with a temporary database.
///
/// The database is created in a temporary directory that will be cleaned up
/// when the test completes.
pub async fn create_test_state_machine() -> RocksStateMachine {
  let fixture = TestStorageFixture::new();
  create_test_state_machine_with_path(&fixture.path_str()).await
}

/// Creates a test `RocksStateMachine` at the specified path.
///
/// # Arguments
/// * `path` - The directory path for the RocksDB database
pub async fn create_test_state_machine_with_path(path: &str) -> RocksStateMachine {
  let engine = RocksDBEngine::new(
    path,
    1024,
    vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()],
  )
  .unwrap();

  RocksStateMachine::new(engine.db().clone(), PathBuf::from(path))
    .await
    .unwrap()
}

/// Creates a test Raft log entry.
///
/// # Arguments
/// * `term` - The Raft term number
/// * `node_id` - The leader node ID
/// * `index` - The log entry index
///
/// The entry contains a simple UpsertKV command with a generated key.
pub fn create_entry(term: u64, node_id: u64, index: u64) -> Entry {
  Entry {
    log_id: create_log_id(term, node_id, index),
    payload: openraft::EntryPayload::Normal(LogEntry::new(Cmd::UpsertKV(UpsertKV::new(
      format!("key_{}_{}", term, index),
      Operation::Update(format!("data_{}_{}", term, index).into_bytes()),
      None,
    )))),
  }
}

/// Appends entries to a log store for testing.
///
/// This is a convenience helper that uses `IOFlushed::noop()` for the callback.
///
/// # Arguments
/// * `log_store` - The log store to append to
/// * `entries` - The entries to append
pub async fn append_entries(
  log_store: &mut RocksLogStore<TypeConfig>,
  entries: Vec<Entry>,
) -> Result<(), io::Error> {
  log_store.append(entries, IOFlushed::noop()).await
}
