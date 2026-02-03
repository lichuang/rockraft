use axum::{
  Router,
  extract::{Query, State},
  http::StatusCode,
  response::{IntoResponse, Json},
  routing::{get, post},
};
use rockraft::config::Config as RockraftConfig;
use rockraft::node::RaftNodeBuilder;
use rockraft::raft::types::{Cmd, Endpoint, Node};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::sync::Arc;
use tokio::signal;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Extended configuration with HTTP address
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
  #[serde(flatten)]
  pub base: RockraftConfig,
  pub http_addr: String,
}

/// Request for setting a key-value pair
#[derive(Debug, Deserialize)]
pub struct SetValueRequest {
  pub key: String,
  pub value: String,
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

/// Application state containing RaftNode and RocksDB
#[derive(Clone)]
pub struct AppState {
  pub raft_node: Arc<rockraft::node::RaftNode>,
  pub data_path: String,
}

impl IntoResponse for ErrorResponse {
  fn into_response(self) -> axum::response::Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(self)).into_response()
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // Initialize tracing
  tracing_subscriber::registry()
    .with(EnvFilter::new(std::env::var("RUST_LOG").unwrap_or_else(
      |_| "rockraft_cluster=info,tower_http=info,axum=info".into(),
    )))
    .with(tracing_subscriber::fmt::layer())
    .init();

  // Parse command line arguments
  let args: Vec<String> = env::args().collect();
  let config_path = if args.len() > 2 && args[1] == "--conf" {
    args[2].clone()
  } else {
    eprintln!("Usage: {} --conf <config-file>", args[0]);
    eprintln!("Example: {} --conf conf/node1.toml", args[0]);
    std::process::exit(1);
  };

  // Load configuration
  let config = load_config(&config_path)?;
  println!("Configuration loaded:");
  println!("  node_id: {}", config.base.node_id);
  println!("  raft_addr: {}", config.base.raft.addr);
  println!("  http_addr: {}", config.http_addr);
  println!("  data_path: {}", config.base.rocksdb.data_path);
  println!("  single: {}", config.base.raft.single);
  println!("  join: {:?}", config.base.raft.join);

  // Create Raft node
  println!("\nCreating Raft node...");
  let raft_node = RaftNodeBuilder::build(&config.base).await?;
  println!("✓ Raft node created successfully!");

  // Initialize cluster if this is node 1
  let data_path = config.base.rocksdb.data_path.clone();

  // Wait for initial log to be applied
  println!("\nWaiting for initial log to be committed...");
  let timeout = Some(std::time::Duration::from_secs(10));
  raft_node
    .raft()
    .wait(timeout)
    .applied_index(Some(0), "init")
    .await?;
  println!("✓ Initial log committed successfully!");

  // Create application state
  let state = AppState {
    raft_node: raft_node.clone(),
    data_path: data_path.clone(),
  };

  // Build HTTP router
  let app = Router::new()
    .route("/get", get(get_handler))
    .route("/set", post(set_handler))
    .route("/delete", post(delete_handler))
    .route("/health", get(health_handler))
    .route("/metrics", get(metrics_handler))
    .layer(CorsLayer::permissive())
    .layer(TraceLayer::new_for_http())
    .with_state(state);

  // Start HTTP server
  let http_addr = config.http_addr.clone();
  let listener = tokio::net::TcpListener::bind(&http_addr).await?;
  println!("\nHTTP server listening on http://{}", http_addr);
  println!("\nAvailable endpoints:");
  println!(
    "  GET  http://{}/get?key=<key>    - Get a value by key",
    http_addr
  );
  println!(
    "  POST http://{}/set             - Set a key-value pair",
    http_addr
  );
  println!("  POST http://{}/delete          - Delete a key", http_addr);
  println!("  GET  http://{}/health          - Health check", http_addr);
  println!(
    "  GET  http://{}/metrics         - Cluster metrics",
    http_addr
  );

  // Spawn HTTP server
  let http_server = tokio::spawn(async move { axum::serve(listener, app).await });

  // Wait for Ctrl+C
  println!("\nPress Ctrl+C to shutdown...");
  signal::ctrl_c().await?;

  println!("\nShutting down Raft node...");
  raft_node.shutdown().await?;
  println!("✓ Raft node shutdown successfully!");

  // Abort HTTP server
  http_server.abort();

  println!("\nServer shutdown complete!");
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

  // Open RocksDB connection for reading
  let db = rocksdb::DB::open_default(&state.data_path).map_err(|e| ErrorResponse {
    error: format!("Failed to open database: {}", e),
  })?;

  match db.get(key.as_bytes()) {
    Ok(Some(value)) => {
      let value_str = String::from_utf8(value).unwrap_or_else(|_| "Invalid UTF-8".to_string());
      Ok(Json(GetResponse {
        key: key.clone(),
        value: Some(value_str),
      }))
    }
    Ok(None) => Ok(Json(GetResponse {
      key: key.clone(),
      value: None,
    })),
    Err(e) => Err(ErrorResponse {
      error: format!("Database error: {}", e),
    }),
  }
}

/// Handler for POST /set endpoint
async fn set_handler(
  State(state): State<AppState>,
  Json(payload): Json<SetValueRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
  // Use LeaderHandler to write to Raft cluster
  match state.raft_node.assume_leader().await {
    Ok(leader_handler) => {
      // This node is leader, write directly
      let upsert_kv = Cmd::UpsertKV(rockraft::raft::types::UpsertKV::insert(
        &payload.key,
        payload.value.as_bytes(),
      ));

      match leader_handler.raft().client_write(upsert_kv).await {
        Ok(response) => {
          tracing::info!("Write successful: log_id={:?}", response.log_id);
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
    Err(forward_err) => {
      // This node is not leader
      let leader_id = forward_err
        .leader_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
      Err(ErrorResponse {
        error: format!("This node is not leader. Current leader: {}", leader_id),
      })
    }
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

  // Use LeaderHandler to delete from Raft cluster
  match state.raft_node.assume_leader().await {
    Ok(leader_handler) => {
      // Create an UpsertKV with Delete operation
      let upsert_kv = Cmd::UpsertKV(rockraft::raft::types::UpsertKV::delete(key));

      match leader_handler.raft().client_write(upsert_kv).await {
        Ok(response) => {
          tracing::info!("Delete successful: log_id={:?}", response.log_id);
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
    Err(forward_err) => {
      // This node is not leader
      let leader_id = forward_err
        .leader_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
      Err(ErrorResponse {
        error: format!("This node is not leader. Current leader: {}", leader_id),
      })
    }
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

/// Load configuration from TOML file
fn load_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
  let config_str = fs::read_to_string(path)
    .map_err(|e| format!("Failed to read config file '{}': {}", path, e))?;

  let config: Config = toml::from_str(&config_str)
    .map_err(|e| format!("Failed to parse config file '{}': {}", path, e))?;

  Ok(config)
}
