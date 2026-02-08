mod api_error;
mod grpc_connection_error;
mod management_error;
mod network_error;
mod openraft_error;
mod startup_error;

pub use api_error::APIError;
pub use grpc_connection_error::GrpcConnectionError;
pub use management_error::ManagementError;
pub use network_error::NetworkError;
pub use openraft_error::OpenRaft;
pub use startup_error::StartupError;

use anyerror::AnyError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RockRaftError {
  #[error("{0}")]
  DnsParseError(String),

  #[error("{0}")]
  TokioError(String),

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

  #[error("OpenRaft error: {0}")]
  OpenRaft(#[from] OpenRaft),

  #[error("OpenRaft error: {0}")]
  Raft(String),

  #[error("Storage error: {0}")]
  StorageError(#[from] AnyError),

  #[error("Startup error: {0}")]
  Startup(#[from] StartupError),

  #[error("Management error: {0}")]
  Management(#[from] ManagementError),

  #[error("Network error: {0}")]
  Network(#[from] NetworkError),

  #[error("API error: {0}")]
  API(#[from] APIError),

  #[error("Grpc Connect error: {0}")]
  GrpcConnection(#[from] GrpcConnectionError),
}

impl RockRaftError {
  pub fn is_retryable(&self) -> bool {
    match self {
      RockRaftError::API(api) => api.is_retryable(),
      RockRaftError::GrpcConnection(err) => {
        // Connection errors (e.g., Connection refused) are typically transient
        // and may resolve if the target service is still starting up
        matches!(err, GrpcConnectionError::CannotConnect { .. })
      }
      _ => false,
    }
  }
}

pub type Result<T> = std::result::Result<T, RockRaftError>;
