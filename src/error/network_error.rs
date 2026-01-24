use anyerror::AnyError;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum NetworkError {
  #[error(transparent)]
  EndpointParseError(AnyError),
}
