//! Connection management for initial cluster operations.
//!
//! Provides connection factory for joining clusters, with DNS resolution
//! and TLS support.

mod client_config;
mod dns_resolver;
mod factory;

pub use factory::JoinConnectionFactory;
