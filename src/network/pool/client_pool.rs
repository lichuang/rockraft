use std::io;
use std::time::Duration;

use crate::error::{Error, Result};
use dashmap::DashMap;
use mobc::{Connection, Pool};
use tracing::debug;
use tracing::warn;

use super::manager::RaftServiceManager;

// Increased default timeout to handle network latency better
const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 10;

#[derive(Clone)]
pub struct ClientPool {
  max_open_connection: u64,
  connection_timeout: Duration,
  max_message_size: usize,

  service_pool: DashMap<String, Pool<RaftServiceManager>>,
}

impl ClientPool {
  pub fn new(max_open_connection: u64) -> Self {
    Self::new_with_config(
      max_open_connection,
      Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_SECS),
      crate::config::DEFAULT_GRPC_MAX_MESSAGE_SIZE,
    )
  }

  pub fn new_with_timeout(max_open_connection: u64, connection_timeout: Duration) -> Self {
    Self::new_with_config(
      max_open_connection,
      connection_timeout,
      crate::config::DEFAULT_GRPC_MAX_MESSAGE_SIZE,
    )
  }

  /// Create a pool with an explicit gRPC max message size for pooled clients.
  pub fn new_with_max_message_size(max_open_connection: u64, max_message_size: usize) -> Self {
    Self::new_with_config(
      max_open_connection,
      Duration::from_secs(DEFAULT_CONNECTION_TIMEOUT_SECS),
      max_message_size,
    )
  }

  fn new_with_config(
    max_open_connection: u64,
    connection_timeout: Duration,
    max_message_size: usize,
  ) -> Self {
    Self {
      max_open_connection,
      connection_timeout,
      max_message_size,
      service_pool: DashMap::with_capacity(2),
    }
  }

  pub async fn raft_service_client(&self, addr: &str) -> Result<Connection<RaftServiceManager>> {
    // Atomically initialize the per-address pool: `entry()` avoids the
    // contains_key/insert race where concurrent first accesses could build
    // and drop multiple pools.
    let pool = self
      .service_pool
      .entry(addr.to_owned())
      .or_insert_with(|| {
        debug!("Creating new connection pool at {}", addr);
        let manager = RaftServiceManager::new(addr.to_owned(), self.max_message_size);
        Pool::builder()
          .max_open(self.max_open_connection)
          .build(manager)
      })
      .clone();

    match pool.get_timeout(self.connection_timeout).await {
      Ok(conn) => {
        debug!("Successfully obtained connection at {}", addr);
        Ok(conn)
      }
      Err(e) => {
        let pool_state = pool.state().await;

        warn!(
          "Connection pool at {} has no connection available. Error: {}, State: {:?}",
          addr, e, pool_state
        );

        Err(Error::retryable(io::Error::other(format!(
          "get grpc client failed, err: {}, state: {:?}",
          e, pool_state
        ))))
      }
    }
  }
}
