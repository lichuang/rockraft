# Rockraft 优化事项清单

> 基于对全部代码的通读整理，按严重程度分类。每一项均标记为未完成。

## 状态说明

- [ ] = 未完成
- [x] = 已完成

---

## 🔴 正确性 Bug（高优先级）

### 1. `apply()` 中 Txn 条件检查读到旧值（同批次内）
- [ ] **未完成**
- **位置**: `src/raft/store/statemachine.rs` 的 `apply()`
- **问题**: `apply()` 把所有写入累积到 `WriteBatch`，循环结束后才 `db.write(batch)`。但 Txn 的条件检查用 `self.db.get_cf()` 直接读 DB。若同一批次内两个 Txn 操作同一 key，第二个 Txn 的条件读到的是第一个 Txn 写入**之前**的值。条件判断与最终写入不一致。

### 2. `recover_snapshot` 异步后台执行，不等待完成
- [ ] **未完成**
- **位置**: `src/raft/store/snapshot/recover.rs` 的 `recover_snapshot`
- **问题**: `recover_snapshot` spawn 一个 task 立即返回 `Ok`。但 `RaftStateMachine::install_snapshot` 在 OpenRaft 语义里是**同步**的——OpenRaft 认为快照已安装后才继续。这里数据可能还没恢复完就返回了。测试里用 `sleep(100ms)` 等，是已知 hack。存在竞态：OpenRaft 可能基于未恢复完的数据继续 apply。

### 3. `Cmd::AddNode` 的 `overriding` 语义未实现
- [ ] **未完成**
- **位置**: `src/raft/store/statemachine.rs` 的 `apply()` / `add_node()`
- **问题**: `add_node()` 总是 `insert`（覆盖），不检查 `overriding: false` 时是否应跳过已存在节点。`LeaderHandler::add_node` 里 `overriding: false` 的语义丢失。

### 4. `EntryPayload::Membership` 在 apply 中被忽略
- [ ] **未完成**
- **位置**: `src/raft/store/statemachine.rs` 的 `apply()`
- **问题**: `apply()` 对 membership 变更 entry 只 log 不处理，nodes 只靠 `Cmd::AddNode/RemoveNode` 更新。但 OpenRaft 的 `change_membership` 产生的是 `EntryPayload::Membership`。两套 membership 来源（OpenRaft 的 membership vs state machine 的 nodes）可能不一致。

### 5. gRPC error 字段编码不一致
- [ ] **未完成**
- **位置**: `src/service/raft_service_impl.rs` 与 `src/raft/network/connection.rs`
- **问题**: `RaftServiceImpl::result_to_raft_reply` 把 error 写成 **String**（`error_msg.into()`），而 `NetworkConnection::forward` 却用 `decode::<ApiError>(&reply.error)` 反序列化。两处对 `RaftReply.error` 的编解码协议不一致，转发错误时客户端会反序列化失败。

---

## 🟠 死代码 / 配置未生效

### 6. `RaftNodeBuilder` 的多个配置项是死代码
- [ ] **未完成**
- **位置**: `src/node/node_builder.rs`
- **问题**:
  - `raft_config: Option<OpenRaftConfig>` — `build()` 里**从未使用**，实际用 `config.raft.to_openraft_config()`
  - `grpc_timeout_seconds` — 未传给 create
  - `max_client_pool_size` — 未传给 create，`RaftNode::create` 里硬编码 `ClientPool::new(10)`

### 7. TTL/过期功能完全未实现
- [ ] **未完成**
- **位置**: `src/raft/types/cmd/upsert_kv.rs`、`meta.rs`、`time.rs`、`src/raft/store/statemachine.rs`
- **问题**: `UpsertKV.value_meta`（`expire_at`/`ttl`）在 `apply_upsert_kv` 里被忽略，只写 value。`with_ttl()`/`with_expire_sec()` 是死代码，`MetaSpec`/`Interval`/`flexible_timestamp_to_duration` 全都没被消费。

### 8. `Cmd::Txn` 的 `result` 字段是死代码
- [ ] **未完成**
- **位置**: `src/raft/types/cmd/cmd.rs`、`src/raft/store/statemachine.rs`
- **问题**: `Cmd::Txn { req, result: None }`，apply 时忽略 `result`，直接返回 `AppliedState::Txn`。

### 9. `vacuum_snapshot_files` 是 stub
- [ ] **未完成**
- **位置**: `src/raft/store/snapshot/build.rs`
- **问题**: 注释明确 TODO，不清理旧快照 → 磁盘无限增长。

### 10. `RocksDBEngine` 的调优参数无法配置
- [ ] **未完成**
- **位置**: `src/engine/rocksdb.rs`、`src/config/config.rs`
- **问题**: `engine::RocksDBConfig`（block_cache_size 等）与 `config::RocksdbConfig`（data_path/max_open_files）是**两个同名不同类型**，且 engine 调优参数没暴露到 `Config`，无法从配置控制。

---

## 🟡 性能问题

### 11. `forward_request_to_leader` 每次新建 gRPC channel
- [ ] **未完成**
- **位置**: `src/node/forward.rs` 的 `send_forward_request`
- **问题**: 用 `JoinConnectionFactory::create_rpc_channel` 每次新建连接，不走 `ClientPool`。转发路径性能差。

### 12. `ClientPool::raft_service_client` 有竞态
- [ ] **未完成**
- **位置**: `src/network/pool/client_pool.rs`
- **问题**: `if !contains_key(addr) { insert }` 非原子，并发下可能创建多个 pool。应用 `DashMap::entry` API。

### 13. `truncate_after`/`purge` 用 `delete_range_cf`
- [ ] **未完成**
- **位置**: `src/raft/store/log_store.rs`
- **问题**: RocksDB 的 range delete 是 lazy 的，需 compaction 才真正释放空间，可能影响后续读性能。

### 14. `get_leader` 2s 超时 × 20 次重试
- [ ] **未完成**
- **位置**: `src/node/forward.rs`
- **问题**: `execute_or_forward` 最坏情况 2s×20 + 指数退避，可能长达 40s+，客户端等待过久。

---

## 🟢 健壮性 / 资源管理

### 15. `RocksDBEngine` 无 Drop 清理，`RaftNode.engine` 是 `#[allow(dead_code)]`
- [ ] **未完成**
- **位置**: `src/engine/rocksdb.rs`、`src/node/node.rs`
- **问题**: DB 依赖 Arc 自动释放，但 RocksDB 需要显式 flush/close。`engine` 字段存了却没用，可能泄漏。

### 16. `RaftNode::shutdown` 只停 gRPC service，不停 raft 实例
- [ ] **未完成**
- **位置**: `src/node/node.rs`
- **问题**: Raft runtime 未正确关闭。

### 17. `RaftServiceImpl::snapshot` 中 `guard.remove(&snapshot_id).unwrap()` 会 panic
- [ ] **未完成**
- **位置**: `src/service/raft_service_impl.rs`
- **问题**: 若 snapshot_id 不在 map（如已 evict），直接 panic。

### 18. `Endpoint::parse` 用 `splitn(2, ':')`
- [ ] **未完成**
- **位置**: `src/config/endpoint.rs`
- **问题**: IPv6 地址（`[::1]:8080`）无法解析。

### 19. `build_snapshot` 的 snapshot_id 用 `now_millis()`
- [ ] **未完成**
- **位置**: `src/raft/store/snapshot/build.rs`
- **问题**: 同一毫秒内多次 build 生成相同 ID，覆盖已有快照。

### 20. `set_last_applied_log_id` 与 batch write 非原子
- [ ] **未完成**
- **位置**: `src/raft/store/statemachine.rs`
- **问题**: 崩溃在 batch write 与 set_last_applied 之间时，数据已写但 last_applied 未更新，重放会重复应用（Txn 的 prev_values 会重复）。

---

## 建议的优化优先级

1. **#1**（Txn 同批次读旧值）
2. **#2**（快照异步恢复竞态）
3. **#5**（error 编解码不一致）
4. **#6/#7**（死代码清理 + TTL 实现）
5. 性能项（#11-#14）
