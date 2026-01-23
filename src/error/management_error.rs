use anyerror::AnyError;
use serde::Deserialize;
use serde::Serialize;

/// Error occurs when managing meta service.
#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ManagementError {
  #[error(transparent)]
  Join(AnyError),

  #[error(transparent)]
  Leave(AnyError),
}
