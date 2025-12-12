use thiserror::Error;

#[derive(Error, Debug)]
pub enum RockRaftError {
  #[error("{0}")]
  TonicTransport(#[from] tonic::transport::Error),

  #[error("Connection pool has no connection information available. {0}")]
  NoAvailableGrpcConnection(String),
}

pub type Result<T> = std::result::Result<T, RockRaftError>;
