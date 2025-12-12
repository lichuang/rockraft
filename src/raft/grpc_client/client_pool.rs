use std::time::Duration;

use crate::error::Result;
use dashmap::DashMap;
use mobc::{Connection, Pool};
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::error::RockRaftError;

use super::raft_service_manager::RaftServiceManager;

// Increased default timeout to handle network latency better
const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 10;

#[derive(Clone)]
pub struct ClientPool {
  max_open_connection: u64,
  connection_timeout: Duration,

  service_pool: DashMap<String, Pool<RaftServiceManager>>,
}

impl ClientPool {
  pub fn new(max_open_connection: u64) -> Self {
    Self::new_with_timeout(
      max_open_connection,
      Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_SECS),
    )
  }

  pub fn new_with_timeout(max_open_connection: u64, connection_timeout: Duration) -> Self {
    Self {
      max_open_connection,
      connection_timeout,
      service_pool: DashMap::with_capacity(2),
    }
  }

  pub async fn raft_service_client(&self, addr: &str) -> Result<Connection<RaftServiceManager>> {
    // Initialize pool if not exists
    if !self.service_pool.contains_key(addr) {
      debug!("Creating new connection pool at {}", addr);
      let manager = RaftServiceManager::new(addr.to_owned());
      let pool = Pool::builder()
        .max_open(self.max_open_connection)
        .build(manager);
      self.service_pool.insert(addr.to_owned(), pool);
      info!(
        "Connection pool at {} initialized (max_open: {}, timeout: {:?})",
        addr, self.max_open_connection, self.connection_timeout
      );
    }

    if let Some(pool) = self.service_pool.get(addr) {
      let pool_state_before = pool.state().await;
      debug!(
        "Attempting to get connection at {} (state: {:?})",
        addr, pool_state_before
      );

      match pool.get_timeout(self.connection_timeout).await {
        Ok(conn) => {
          debug!("Successfully obtained connection at {}", addr);
          return Ok(conn);
        }
        Err(e) => {
          let pool_state_after = pool.state().await;

          warn!(
            "Connection pool at {} has no connection available. Error: {}, State before: {:?}, State after: {:?}",
            addr, e, pool_state_before, pool_state_after
          );

          return Err(RockRaftError::NoAvailableGrpcConnection(format!(
            "get grpc client failed, err: {}, state: {:?}",
            e, pool_state_after
          )));
        }
      }
    }

    return Err(RockRaftError::NoAvailableGrpcConnection(
      "connection pool is not initialized".to_string(),
    ));
  }
}
