use crate::raft::types::Node;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct GetMembersReq {}

pub type GetMembersReply = BTreeMap<u64, Node>;
