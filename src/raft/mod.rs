#[allow(clippy::all)]
pub mod protobuf {
  tonic::include_proto!("raftpb");
}

pub mod network;
pub mod store;
pub mod types;
