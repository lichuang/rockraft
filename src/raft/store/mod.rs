pub mod keys;
mod log_store;
mod meta;
mod snapshot;
mod statemachine;

pub use log_store::RocksLogStore;
pub use meta::StoreMeta;
pub use statemachine::RocksStateMachine;

use std::path::PathBuf;

use crate::engine::RocksDBEngine;
use crate::error::Result;
use crate::raft::types::TypeConfig;

/// Create a RocksDB engine pre-configured with all column families needed by
/// the log store and state machine.
///
/// Callers that need a shared database for both stores should use this
/// instead of assembling the column family list themselves.
pub fn create_storage_engine(data_path: &str, max_open_files: i32) -> Result<RocksDBEngine> {
  RocksDBEngine::new(data_path, max_open_files, keys::column_family_list())
}

/// Create both the log store and state machine from a shared RocksDB engine.
///
/// This encapsulates the coordination of constructing the two storage
/// components, so callers don't need to reach into engine internals.
pub async fn create_stores(
  engine: &RocksDBEngine,
  data_dir: PathBuf,
) -> Result<(RocksLogStore<TypeConfig>, RocksStateMachine)> {
  let db = engine.db().clone();
  let log_store = RocksLogStore::create(db.clone())?;
  let state_machine = RocksStateMachine::new(db, data_dir).await?;
  Ok((log_store, state_machine))
}
