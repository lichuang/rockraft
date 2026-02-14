use axum::{
  Router,
  extract::{Query, State},
  http::StatusCode,
  response::{IntoResponse, Json},
  routing::{get, post},
};
use rockraft::config::Config as RockraftConfig;
use rockraft::node::RaftNodeBuilder;
use rockraft::raft::types::{Cmd, GetKVReq, LeaveRequest, LogEntry};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::sync::Arc;
use tokio::signal;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Log configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LogConfig {
  /// Log file path, if not set, logs will be printed to stdout
  pub file: Option<String>,
  /// Log level, default is "info"
  #[serde(default = "default_log_level")]
  pub level: String,
}

fn default_log_level() -> String {
  "info".to_string()
}

impl Default for LogConfig {
  fn default() -> Self {
    Self {
      file: None,
      level: default_log_level(),
    }
  }
}

/// Extended configuration with HTTP address
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
  #[serde(flatten)]
  pub base: RockraftConfig,
  pub http_addr: String,
  /// Log configuration
  #[serde(default)]
  pub log: LogConfig,
}

/// Request for setting a key-value pair
#[derive(Debug, Deserialize)]
pub struct SetValueRequest {
  pub key: String,
  pub value: String,
}

/// Request for leaving the cluster
#[derive(Debug, Deserialize)]
pub struct LeaveRequestHttp {
  pub node_id: u64,
}

/// Response for successful operations
#[derive(Debug, Serialize)]
pub struct SuccessResponse {
  pub success: bool,
  pub message: String,
}

/// Response for get operations
#[derive(Debug, Serialize)]
pub struct GetResponse {
  pub key: String,
  pub value: Option<String>,
}

/// Response for errors
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
  pub error: String,
}

/// Application state containing RaftNode
#[derive(Clone)]
pub struct AppState {
  pub raft_node: Arc<rockraft::node::RaftNode>,
}

impl IntoResponse for ErrorResponse {
  fn into_response(self) -> axum::response::Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(self)).into_response()
  }
}

fn init_logging(config: &LogConfig) -> Result<(), Box<dyn std::error::Error>> {
  let env_filter = if std::env::var("RUST_LOG").is_ok() {
    EnvFilter::from_default_env()
  } else {
    EnvFilter::new(&config.level)
  };

  match &config.file {
    Some(log_file) => {
      // Ensure parent directory exists
      if let Some(parent) = std::path::Path::new(log_file).parent() {
        std::fs::create_dir_all(parent)?;
      }

      let file_appender = tracing_appender::rolling::never(
        std::path::Path::new(log_file)
          .parent()
          .unwrap_or(std::path::Path::new(".")),
        std::path::Path::new(log_file)
          .file_name()
          .unwrap_or_default(),
      );

      tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_writer(file_appender))
        .init();

      println!("Logging to file: {}", log_file);
    }
    None => {
      tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
    }
  }

  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Parse command line arguments first to get config path
  let args: Vec<String> = env::args().collect();
  let config_path = if args.len() > 2 && args[1] == "--conf" {
    args[2].clone()
  } else {
    eprintln!("Usage: {} --conf <config-file>", args[0]);
    eprintln!("Example: {} --conf conf/node1.toml", args[0]);
    std::process::exit(1);
  };

  // Load configuration first (without logging)
  let config = load_config(&config_path)?;

  // Initialize tracing with config
  init_logging(&config.log)?;

  println!("Configuration loaded:");
  println!("  node_id: {}", config.base.node_id);
  println!("  raft_addr: {}", config.base.raft.address);
  println!("  http_addr: {}", config.http_addr);
  println!("  data_path: {}", config.base.rocksdb.data_path);
  println!("  single: {}", config.base.raft.single);
  println!("  join: {:?}", config.base.raft.join);
  println!("  log: {:?}", config.log);

  // Create Raft node
  info!("Creating Raft node...");
  let raft_node = RaftNodeBuilder::build(&config.base).await?;
  info!("Raft node created successfully");

  /*
    // Wait for initial log to be applied
    info!("Waiting for initial log to be committed...");
    let timeout = Some(std::time::Duration::from_secs(10));
    raft_node
      .raft()
      .wait(timeout)
      .applied_index(Some(1), "init")
      .await?;
    info!("Initial log committed successfully");
  */

  // Create application state
  let state = AppState {
    raft_node: raft_node.clone(),
  };

  // Build HTTP router
  let app = Router::new()
    .route("/get", get(get_handler))
    .route("/set", post(set_handler))
    .route("/delete", post(delete_handler))
    .route("/leave", post(leave_handler))
    .route("/members", get(members_handler))
    .route("/health", get(health_handler))
    .route("/metrics", get(metrics_handler))
    .layer(CorsLayer::permissive())
    .layer(TraceLayer::new_for_http())
    .with_state(state);

  // Start HTTP server
  let http_addr = config.http_addr.clone();
  let listener = tokio::net::TcpListener::bind(&http_addr).await?;
  info!("HTTP server listening on http://{}", http_addr);
  info!("Available endpoints:");
  info!(
    "  GET  http://{}/get?key=<key>    - Get a value by key",
    http_addr
  );
  info!(
    "  POST http://{}/set             - Set a key-value pair",
    http_addr
  );
  info!("  POST http://{}/delete          - Delete a key", http_addr);
  info!(
    "  POST http://{}/leave           - Remove a node from cluster",
    http_addr
  );
  info!(
    "  GET  http://{}/members         - Get cluster members",
    http_addr
  );
  info!("  GET  http://{}/health          - Health check", http_addr);
  info!(
    "  GET  http://{}/metrics         - Cluster metrics",
    http_addr
  );

  // Spawn HTTP server
  let http_server = tokio::spawn(async move { axum::serve(listener, app).await });

  // Wait for Ctrl+C
  info!("Press Ctrl+C to shutdown...");
  signal::ctrl_c().await?;

  info!("Shutting down Raft node...");
  raft_node.shutdown().await?;
  info!("Raft node shutdown successfully");

  // Abort HTTP server
  http_server.abort();

  info!("Server shutdown complete");
  Ok(())
}

/// Handler for GET /get endpoint
async fn get_handler(
  Query(params): Query<std::collections::HashMap<String, String>>,
  State(state): State<AppState>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let key = params.get("key").ok_or_else(|| ErrorResponse {
    error: "Missing 'key' parameter".to_string(),
  })?;

  // Use raft_node.read() which handles reading from the state machine
  let req = GetKVReq { key: key.clone() };
  match state.raft_node.read(req).await {
    Ok(value_opt) => {
      let value_str =
        value_opt.map(|v| String::from_utf8(v).unwrap_or_else(|_| "Invalid UTF-8".to_string()));
      Ok(Json(GetResponse {
        key: key.clone(),
        value: value_str,
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to read: {}", e),
    }),
  }
}

/// Handler for POST /set endpoint
async fn set_handler(
  State(state): State<AppState>,
  Json(payload): Json<SetValueRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
  // Create a LogEntry with UpsertKV command
  let upsert_kv = Cmd::UpsertKV(rockraft::raft::types::UpsertKV::insert(
    &payload.key,
    payload.value.as_bytes(),
  ));
  let entry = LogEntry::new(upsert_kv);

  // Use raft_node.write() which handles leader forwarding automatically
  match state.raft_node.write(entry).await {
    Ok(_) => {
      info!("Write successful: key='{}'", payload.key);
      Ok(Json(SuccessResponse {
        success: true,
        message: format!("Key '{}' set successfully", payload.key),
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to write: {}", e),
    }),
  }
}

/// Handler for POST /delete endpoint
async fn delete_handler(
  State(state): State<AppState>,
  Json(payload): Json<std::collections::HashMap<String, String>>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let key = payload.get("key").ok_or_else(|| ErrorResponse {
    error: "Missing 'key' field".to_string(),
  })?;

  // Create an UpsertKV with Delete operation
  let upsert_kv = Cmd::UpsertKV(rockraft::raft::types::UpsertKV::delete(key));
  let entry = LogEntry::new(upsert_kv);

  // Use raft_node.write() which handles leader forwarding automatically
  match state.raft_node.write(entry).await {
    Ok(_) => {
      info!("Delete successful: key='{}'", key);
      Ok(Json(SuccessResponse {
        success: true,
        message: format!("Key '{}' deleted successfully", key),
      }))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to delete: {}", e),
    }),
  }
}

/// Handler for GET /health endpoint
async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
  let metrics = state.raft_node.raft().metrics().borrow().clone();

  Json(serde_json::json!({
    "status": "ok",
    "node_id": state.raft_node.raft().node_id(),
    "is_leader": metrics.current_leader == Some(*state.raft_node.raft().node_id()),
    "current_leader": metrics.current_leader,
    "current_term": metrics.current_term,
    "state": metrics.running_state,
  }))
}

/// Handler for GET /metrics endpoint
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
  let metrics = state.raft_node.raft().metrics().borrow().clone();

  Json(serde_json::json!({
    "node_id": state.raft_node.raft().node_id(),
    "current_leader": metrics.current_leader,
    "current_term": metrics.current_term,
    "last_applied": metrics.last_applied,
    "last_log_index": metrics.last_log_index,
    "state": metrics.running_state,
    "membership": metrics.membership_config,
  }))
}

/// Handler for POST /leave endpoint
async fn leave_handler(
  State(state): State<AppState>,
  Json(payload): Json<LeaveRequestHttp>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let req = LeaveRequest {
    node_id: payload.node_id,
  };

  match state.raft_node.leave(req).await {
    Ok(()) => Ok(Json(SuccessResponse {
      success: true,
      message: format!("Node {} left the cluster successfully", payload.node_id),
    })),
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to leave cluster: {}", e),
    }),
  }
}

/// Handler for GET /members endpoint
async fn members_handler(
  State(state): State<AppState>,
) -> Result<impl IntoResponse, ErrorResponse> {
  let req = rockraft::raft::types::GetMembersReq {};

  match state.raft_node.get_members(req).await {
    Ok(members) => {
      let members_json: BTreeMap<String, serde_json::Value> = members
        .into_iter()
        .map(|(id, node)| {
          let node_json = serde_json::json!({
            "node_id": node.node_id,
            "endpoint": format!("{}", node.endpoint),
          });
          (id.to_string(), node_json)
        })
        .collect();
      Ok(Json(serde_json::json!({
        "success": true,
        "members": members_json,
        "count": members_json.len(),
      })))
    }
    Err(e) => Err(ErrorResponse {
      error: format!("Failed to get members: {}", e),
    }),
  }
}

/// Load configuration from TOML file
fn load_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
  let config_str = fs::read_to_string(path)
    .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

  let config: Config = toml::from_str(&config_str)
    .map_err(|e| format!("Failed to parse config file '{}': {}", path, e))?;

  Ok(config)
}
