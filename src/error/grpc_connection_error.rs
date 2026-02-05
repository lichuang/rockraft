use anyerror::AnyError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GrpcConnectionError {
  #[error("invalid uri: {uri}, error: {source}")]
  InvalidUri {
    uri: String,
    #[source]
    source: AnyError,
  },

  #[error("{action} client tls config, error: {source}")]
  TLSConfigError {
    action: String,
    #[source]
    source: AnyError,
  },

  #[error("can not connect to {uri}, error: {source}")]
  CannotConnect {
    uri: String,
    #[source]
    source: AnyError,
  },
}
