pub mod keys;
mod log_store;
mod meta;
mod snapshot;
mod statemachine;

pub use log_store::RocksLogStore;
pub use meta::StoreMeta;
pub use statemachine::RocksStateMachine;

use crate::engine::RocksDBEngine;
use crate::error::Result;

/// Create a RocksDB engine pre-configured with all column families needed by
/// the log store and state machine.
///
/// Callers that need a shared database for both stores should use this
/// instead of assembling the column family list themselves.
pub fn create_storage_engine(data_path: &str, max_open_files: i32) -> Result<RocksDBEngine> {
  RocksDBEngine::new(data_path, max_open_files, keys::column_family_list())
}
