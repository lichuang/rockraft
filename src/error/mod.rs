use thiserror::Error;

use crate::raft::types::TypeConfig;

#[derive(Error, Debug)]
pub enum RockRaftError {
  #[error("{0}")]
  TonicTransport(#[from] tonic::transport::Error),

  #[error("Tonic RPC status error: {0}")]
  TonicStatus(#[from] tonic::Status),

  #[error("Bincode serialization/deserialization error: {0}")]
  BincodeSerialize(#[from] bincode::Error),

  #[error("Connection pool has no connection information available. {0}")]
  NoAvailableGrpcConnection(String),

  #[error("IO error: {0}")]
  Io(#[from] std::io::Error),

  #[error("Fatal Raft error: {0}")]
  OpenraftFatal(#[from] openraft::error::Fatal<TypeConfig>),
}

pub type Result<T> = std::result::Result<T, RockRaftError>;
