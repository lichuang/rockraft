// Endpoint is now defined in the config module to avoid circular dependencies.
// This file re-exports it for backward compatibility.

pub use crate::config::Endpoint;
