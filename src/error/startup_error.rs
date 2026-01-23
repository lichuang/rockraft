use anyerror::AnyError;
use thiserror::Error;

/// Errors that can occur during startup and initialization of the Raft cluster
#[derive(Error, Debug)]
pub enum StartupError {
  /// Error occurred when adding a node to the cluster
  #[error("Failed to add node to cluster: {source}")]
  AddNodeError { source: AnyError },

  /// Invalid configuration provided
  #[error("Invalid configuration: {0}")]
  InvalidConfig(String),

  #[error("{0}")]
  OtherError(String),
}

impl StartupError {
  /// Create a new AddNodeError with the given source error
  pub fn add_node_error(source: AnyError) -> Self {
    Self::AddNodeError { source }
  }

  /// Create a new InvalidConfig error with the given message
  pub fn invalid_config(message: impl Into<String>) -> Self {
    Self::InvalidConfig(message.into())
  }
}
