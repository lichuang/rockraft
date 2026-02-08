use crate::raft::types::TypeConfig;
use thiserror::Error;

/// OpenRaft specific errors
#[derive(Error, Debug)]
pub enum OpenRaft {
  #[error("Fatal error: {0}")]
  Fatal(#[from] openraft::error::Fatal<TypeConfig>),

  #[error("Client write error: {0}")]
  ClientWrite(openraft::error::ClientWriteError<TypeConfig>),
}
