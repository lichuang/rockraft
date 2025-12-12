fn main() -> Result<(), Box<dyn std::error::Error>> {
  println!("cargo:rerun-if-changed=src/*");
  let mut config = prost_build::Config::new();
  config.protoc_arg("--experimental_allow_proto3_optional");
  let proto_files = ["src/raft/proto/raft.proto"];

  tonic_build::configure()
    .btree_map(["."])
    .type_attribute("raftpb.AppendRequest", "#[derive(Eq)]")
    .type_attribute("raftpb.AppendReply", "#[derive(Eq)]")
    .compile_protos_with_config(config, &proto_files, &["proto"])?;
  Ok(())
}
