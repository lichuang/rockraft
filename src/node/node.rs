use crate::error::Result;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::Config as OpenRaftConfig;
use openraft::Raft;

use crate::config::Config;
use crate::engine::RocksDBEngine;
use crate::raft::grpc_client::ClientPool;
use crate::raft::network::NetworkFactory;
use crate::raft::store::RocksLogStore;
use crate::raft::store::RocksStateMachine;
use crate::raft::store::column_family_list;
use crate::raft::types::TypeConfig;

pub struct RaftNode {
  engine: Arc<RocksDBEngine>,
  raft: Arc<Raft<TypeConfig>>,
}

impl RaftNode {
  pub async fn create(config: &Config) -> Result<Self> {
    let engine = Arc::new(RocksDBEngine::new(
      &config.rocksdb.data_path,
      config.rocksdb.max_open_files,
      column_family_list(),
    ));

    let node_id = config.node_id;

    // Create log store
    let log_store = RocksLogStore::create(engine.db.clone())?;

    // Create state machine
    let data_dir = PathBuf::from(&config.rocksdb.data_path);
    let state_machine = RocksStateMachine::new(engine.db.clone(), data_dir).await?;

    // Create client pool for network connections
    let client_pool = Arc::new(ClientPool::new(10));

    // Create network factory
    let network = NetworkFactory::new(client_pool);

    // Configure Raft
    let raft_config = OpenRaftConfig::default();

    // Create Raft instance
    let raft = Arc::new(
      Raft::new(
        node_id,
        Arc::new(raft_config),
        network,
        log_store,
        state_machine,
      )
      .await?,
    );

    Ok(Self { engine, raft })
  }
}
