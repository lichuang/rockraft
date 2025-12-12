use std::sync::Arc;

use crate::config::Config;
use crate::engine::RocksDBEngine;
use crate::raft::store::RocksLogStore;
use crate::raft::store::column_family_list;
use crate::raft::types::TypeConfig;

pub struct RaftNode {
  engine: Arc<RocksDBEngine>,
  // log_store: RocksLogStore<TypeConfig>,
}

impl RaftNode {
  pub fn new(config: &Config) -> Self {
    let engine = Arc::new(RocksDBEngine::new(
      &config.rocksdb.data_path,
      config.rocksdb.max_open_files,
      column_family_list(),
    ));

    // let log_store = RocksLogStore::new(engine.db.clone());
    Self { engine }
  }
}
