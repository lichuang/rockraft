use crate::config::Config;
use crate::error::{Error, Result};
use crate::node::RaftNode;
use openraft::Config as OpenRaftConfig;
use std::sync::Arc;
use tracing::info;

/// Builder for constructing a RaftNode with progressive configuration.
///
/// This builder provides a fluent API for constructing a RaftNode with
/// various configuration options. It handles validation, defaults, and
/// the complex multi-step initialization process.
///
/// # Example
///
/// ```rust,no_run
/// use rockraft::node::RaftNodeBuilder;
///
/// async fn example(config: &rockraft::config::Config) -> Result<(), Box<dyn std::error::Error>> {
///     let node = RaftNodeBuilder::new()
///         .config(config)
///         .auto_init_cluster(true)
///         .grpc_timeout_seconds(30)
///         .build()
///         .await?;
///     
///     Ok(())
/// }
/// ```
pub struct RaftNodeBuilder<'a> {
  config: Option<&'a Config>,
  auto_init_cluster: bool,
  grpc_timeout_seconds: u64,
  max_client_pool_size: usize,
  raft_config: Option<OpenRaftConfig>,
}

impl<'a> Default for RaftNodeBuilder<'a> {
  fn default() -> Self {
    Self {
      config: None,
      auto_init_cluster: true,
      grpc_timeout_seconds: 10,
      max_client_pool_size: 10,
      raft_config: None,
    }
  }
}

impl<'a> RaftNodeBuilder<'a> {
  /// Create a new builder with sensible defaults.
  pub fn new() -> Self {
    Self::default()
  }

  /// Set the configuration (required).
  ///
  /// # Errors
  /// Build will fail if config is not provided.
  pub fn config(mut self, config: &'a Config) -> Self {
    self.config = Some(config);
    self
  }

  /// Whether to automatically initialize or join the cluster.
  ///
  /// - `true` (default): Automatically init cluster (single mode) or join (join list provided)
  /// - `false`: Create node without cluster membership - must call join manually
  pub fn auto_init_cluster(mut self, auto_init: bool) -> Self {
    self.auto_init_cluster = auto_init;
    self
  }

  /// Set gRPC request timeout in seconds.
  ///
  /// Default: 10 seconds
  pub fn grpc_timeout_seconds(mut self, seconds: u64) -> Self {
    self.grpc_timeout_seconds = seconds;
    self
  }

  /// Set maximum client pool size for gRPC connections.
  ///
  /// Default: 10
  pub fn max_client_pool_size(mut self, size: usize) -> Self {
    self.max_client_pool_size = size;
    self
  }

  /// Provide a custom OpenRaft configuration.
  ///
  /// If not provided, sensible defaults will be used.
  pub fn raft_config(mut self, config: OpenRaftConfig) -> Self {
    self.raft_config = Some(config);
    self
  }

  /// Validate the builder configuration before building.
  fn validate(&self) -> Result<()> {
    if self.config.is_none() {
      return Err(Error::config(
        "Config is required. Use .config() to provide one.",
      ));
    }

    if self.grpc_timeout_seconds == 0 {
      return Err(Error::config("grpc_timeout_seconds must be > 0"));
    }

    if self.max_client_pool_size == 0 {
      return Err(Error::config("max_client_pool_size must be > 0"));
    }

    Ok(())
  }

  /// Build and initialize the RaftNode.
  ///
  /// This method:
  /// 1. Validates the configuration
  /// 2. Creates the RaftNode (storage, raft instance, etc.)
  /// 3. Starts the gRPC service
  /// 4. Initializes or joins the cluster (if auto_init_cluster is true)
  pub async fn build(self) -> Result<Arc<RaftNode>> {
    // Validate configuration
    self.validate()?;
    let config = self
      .config
      .ok_or_else(|| Error::config("Config is required. Use .config() to provide one."))?;

    info!("Building RaftNode with node_id={}", config.node_id);

    // Create the RaftNode using the existing factory method
    let raft_node = RaftNode::create(config).await?;

    // Start the gRPC service
    RaftNode::start_raft_service(raft_node.clone()).await?;

    // Initialize or join cluster if requested
    if self.auto_init_cluster {
      if config.raft.join.is_empty() {
        info!("Initializing single-node cluster");
        let node = crate::raft::types::Node {
          node_id: config.node_id,
          endpoint: config.raft.endpoint.clone(),
        };
        raft_node.init_cluster(node).await?;
      } else {
        info!("Joining existing cluster via: {:?}", config.raft.join);
        raft_node.join_cluster().await?;
      }
    } else {
      info!("Auto-init disabled: node will not join cluster automatically");
    }

    info!("RaftNode built successfully");
    Ok(raft_node)
  }
}

/// Simplified convenience methods for common use cases.
impl<'a> RaftNodeBuilder<'a> {
  /// Build a RaftNode from a Config with all defaults.
  ///
  /// This is the simplest way to create a node:
  /// ```rust,no_run
  /// use rockraft::node::RaftNodeBuilder;
  ///
  /// async fn example(config: &rockraft::config::Config) -> Result<(), Box<dyn std::error::Error>> {
  ///     let node = RaftNodeBuilder::from_config(config).await?;
  ///     Ok(())
  /// }
  /// ```
  pub async fn from_config(config: &'a Config) -> Result<Arc<RaftNode>> {
    Self::new().config(config).build().await
  }

  /// Build a RaftNode without auto-joining the cluster.
  ///
  /// Use this when you need to defer cluster membership:
  /// ```rust,no_run
  /// use rockraft::node::RaftNodeBuilder;
  ///
  /// async fn example(config: &rockraft::config::Config) -> Result<(), Box<dyn std::error::Error>> {
  ///     let node = RaftNodeBuilder::standalone(config).await?;
  ///     // ... later ...
  ///     node.join_cluster().await?;
  ///     Ok(())
  /// }
  /// ```
  pub async fn standalone(config: &'a Config) -> Result<Arc<RaftNode>> {
    Self::new()
      .config(config)
      .auto_init_cluster(false)
      .build()
      .await
  }
}
