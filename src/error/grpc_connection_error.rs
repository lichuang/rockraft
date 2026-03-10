use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GrpcConnectionError {
  #[error("invalid uri: {uri}, error: {msg}")]
  InvalidUri { uri: String, msg: String },

  #[error("{action} client tls config, error: {msg}")]
  TLSConfigError { action: String, msg: String },

  #[error("can not connect to {uri}, error: {msg}")]
  CannotConnect { uri: String, msg: String },
}
