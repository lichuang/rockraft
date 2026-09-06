# Rockraft 优化事项清单

> 基于对全部代码的通读整理（2026-09），按严重程度分类。未包含任何已删除的历史文档内容。

## 状态说明

- [ ] = 未完成
- [x] = 已完成

---

## 一、正确性 / 一致性问题（最高优先级）

### 1. `apply()` 中 Txn 条件检查读到旧值（同批次内）— 有跨节点状态分歧风险
- [ ] **未完成**
- **位置**: `src/raft/store/statemachine.rs` 的 `apply()`
- **问题**: `apply()` 把所有写入累积进 `WriteBatch`，循环结束后才 `db.write(batch)`。但 Txn 的条件检查和 `prev_values` 采集用 `self.db.get_cf()` 直接读 DB，**看不到同批次前面 entry 的写入**。
- **危害**: 不只是语义错误——条件求值结果取决于批次边界，而 leader 和 follower 的 apply 批次划分可能不同（重启恢复、追日志时尤其如此），同一批 entry 在不同节点可能走不同分支 → **状态分歧**。
- **修法**: apply 循环内维护一个本地 pending-writes 覆盖层（HashMap），条件检查/prev_values 先查覆盖层再查 DB。

### 2. `install_snapshot` 异步恢复不等待完成
- [ ] **未完成**
- **位置**: `src/raft/store/snapshot/recover.rs` 的 `recover_snapshot`
- **问题**: `recover_snapshot` spawn 一个后台任务后立即返回 `Ok`。但 OpenRaft 语义是调用返回时快照已安装，之后会在其上继续 apply 日志——数据还没恢复完就开始 apply，存在竞态。
- **佐证**: 测试里用 `sleep(100ms)` 等待，是已知 hack。
- **修法**: 改为同步等待恢复完成（保留 spawn_blocking 避免阻塞 runtime，但要 join）再返回。

### 3. membership 双源问题
- [ ] **未完成**
- **位置**: `src/raft/store/statemachine.rs` 的 `apply()`；`src/node/cluster.rs` 的 `is_in_cluster()`
- **问题**: `apply()` 对 `EntryPayload::Membership` 只打日志不处理，SM 的 nodes 只靠 `Cmd::AddNode/RemoveNode` 维护。OpenRaft `change_membership` 产生的 membership entry 与 SM 的 nodes 是两套状态，learner、`retain` 语义等场景下可能不一致。
- **危害**: `get_last_membership()`（用于转发寻址、`is_in_cluster`）建立在 nodes 上，与 raft core 的真实 membership 可能漂移。
- **修法**: 统一 membership 来源——要么 apply 时同步 `EntryPayload::Membership` 到 nodes，要么转发寻址直接用 `raft.metrics()`。

### 4. gRPC `RaftReply.error` 编解码协议不一致
- [ ] **未完成**
- **位置**: `src/service/raft_service_impl.rs` 的 `result_to_raft_reply`；`src/raft/network/connection.rs` 的 `forward`；`src/node/cluster.rs` 的 `join_via`
- **问题**: 服务端把 error 写成**纯文本 String**（`error_msg.into()`）；但 `NetworkConnection::forward` 用 `decode::<ApiError>(&reply.error)`（postcard 二进制）反序列化；`join_via` 又用第三种方式（检查 `is_empty` + 字符串）。三处对同一字段的编解码协议不一致。
- **危害**: 远端转发出错时客户端 decode 失败，真实错误（含可重试性判断）丢失。
- **修法**: 统一为一个协议（建议服务端统一 postcard 序列化 `Error`/`ApiError`，客户端统一 decode）。

### 5. `last_applied` 与数据写入非原子
- [ ] **未完成**
- **位置**: `src/raft/store/statemachine.rs` 的 `apply()` / `set_last_applied_log_id()`
- **问题**: 数据走 `WriteBatch` 一次提交，`set_last_applied_log_id` 是另一次独立写。崩溃窗口内重启后会重放已应用日志。
- **危害**: 对 KV 幂等无影响，但 Txn 的 `prev_values` 响应会重复/错误。
- **修法**: 把 `LAST_APPLIED_LOG_KEY` 的写入放进同一个 WriteBatch。

---

## 二、未实现 / 死代码

### 6. TTL/过期功能全链路未实现
- [ ] **未完成**
- **位置**: `src/raft/types/cmd/upsert_kv.rs`、`meta.rs`、`time.rs`；`src/raft/store/statemachine.rs` 的 `apply_upsert_kv()`
- **问题**: `UpsertKV.value_meta`（`MetaSpec` 的 `expire_at`/`ttl`）在 apply 时被完全忽略，只写 value。`with_ttl()`/`with_expire_sec()` 是死代码，`MetaSpec`/`Interval`/`flexible_timestamp_to_duration` 无任何消费者。
- **决策**: 要么完整实现（apply 时写带 TTL 的 key、读取/scan 时过滤过期，`LogEntry.time_ms` 字段已有铺垫），要么删除整条链路。

### 7. `RaftNodeBuilder` 三个配置项是摆设
- [ ] **未完成**
- **位置**: `src/node/node_builder.rs`；`src/node/node.rs` 的 `create()`
- **问题**:
  - `raft_config: Option<OpenRaftConfig>` — `build()` 中从未使用，实际用 `config.raft.to_openraft_config()`
  - `grpc_timeout_seconds` — 只做校验，未传入 create
  - `max_client_pool_size` — 只做校验，未传入 create；`create()` 里硬编码 `ClientPool::new(10)`、转发超时 10s
- **修法**: 要么把 builder 选项真正接到 `create()`，要么删除。

### 8. `Cmd::Txn` 的 `result` 字段是死代码
- [ ] **未完成**
- **位置**: `src/raft/types/cmd/cmd.rs`；`src/raft/store/statemachine.rs` 的 `apply()`
- **问题**: 构造时永远 `result: None`，apply 时忽略 `result` 直接返回 `AppliedState::Txn`。
- **修法**: 删除该字段。

### 9. `vacuum_snapshot_files` 是 stub → 磁盘无限增长
- [ ] **未完成**
- **位置**: `src/raft/store/snapshot/build.rs`
- **问题**: 注释明确 TODO，不清理旧快照。每次 build snapshot 新增一个目录，旧的永不删除。
- **危害**: 长期运行磁盘无限增长。
- **修法**: 实现清理逻辑——遍历 snapshot_dir，保留 `last_snapshot_id` 指向的目录，删除其余（注意先校验新快照完整性再删旧）。

### 10. engine 调优参数不可配置
- [ ] **未完成**
- **位置**: `src/engine/rocksdb.rs`、`src/config/config.rs`
- **问题**: `engine::RocksDBConfig`（block_cache_size/write_buffer 等）与 `config::RocksdbConfig`（data_path/max_open_files）是**两个同名不同类型**，前者未暴露到 `Config`，用户无法从配置控制 RocksDB 调优参数。
- **修法**: 合并为一个配置结构，或在 `RocksdbConfig` 中增加调优字段。

---

## 三、性能

### 11. 转发热路径不走连接池
- [ ] **未完成**
- **位置**: `src/node/forward.rs` 的 `send_forward_request()`
- **问题**: 用 `JoinConnectionFactory::create_rpc_channel` 每次新建 channel（TCP 握手开销），而 `ClientPool` 只服务于 append/vote/snapshot RPC。
- **危害**: 转发是高频路径，每次请求付出建连成本。
- **修法**: 转发路径接入 `ClientPool`。

### 12. `ClientPool` 初始化有竞态
- [ ] **未完成**
- **位置**: `src/network/pool/client_pool.rs` 的 `raft_service_client()`
- **问题**: `if !contains_key(addr) { insert }` 非原子，并发首次访问同一 addr 时可能重复创建 pool（旧 pool 泄漏）。
- **修法**: 使用 `DashMap::entry(addr).or_insert_with(...)` API。

### 13. 无 leader 时客户端等待过长
- [ ] **未完成**
- **位置**: `src/node/forward.rs` 的 `get_leader()`（2s 超时）、`execute_or_forward()`（20 次重试 + 指数退避）
- **问题**: 最坏情况 2s×20 + 指数退避，阻塞可达 40s+，客户端超时体验差。
- **修法**: 缩短单次等待、降低总预算、尽早返回可重试错误让上层决策。

### 14. 读路径全部转发到 leader
- [ ] **未完成**
- **位置**: `src/node/node.rs` 的 `read()` / `scan_prefix()`
- **问题**: 读也走 `execute_or_forward` 转发到 leader，leader 成为读瓶颈，且每次转发有一次 postcard 编解码 + 网络往返。
- **说明**: 当前语义是读 leader 已 apply 数据（非线性一致读）。若接受 follower 本地读，可大幅降低 leader 压力；若要线性一致读则需引入 ReadIndex/lease 机制。
- **决策**: 明确读一致性语义后选择方案。

### 15. `truncate_after`/`purge` 用 `delete_range_cf`（惰性删除）
- [ ] **未完成**
- **位置**: `src/raft/store/log_store.rs`
- **问题**: RocksDB 的 range delete 是 lazy 的，需 compaction 才真正释放空间，大量删除可能影响后续读性能。
- **修法**: purge 后可配合 `compact_range` 触发空间回收（权衡 compaction 开销）。

---

## 四、健壮性 / 资源管理

### 16. `RaftNode::shutdown` 不关闭 raft 实例
- [ ] **未完成**
- **位置**: `src/node/node.rs` 的 `shutdown()`
- **问题**: 只停了 gRPC service，OpenRaft 的 `raft.shutdown()` 从未调用，raft runtime 未正确关闭。
- **修法**: shutdown 流程中调用 `self.raft.shutdown().await` 并等待完成。

### 17. `get_members` 绕过了 leader 检查
- [ ] **未完成**
- **位置**: `src/node/node.rs` 的 `get_members()`
- **问题**: 直接 `LeaderHandler::new(self).get_members()`，不走 `execute_or_forward`，与 `LeaderHandler` 文档"leader-only"矛盾。follower 返回本地（可能滞后）视图；`RequestPayload::GetMembers` 的转发路径没被本地 API 使用。
- **决策**: 要么走完整转发流程，要么修正文档明示"返回本地视图"。

### 18. snapshot 流式接收缺乏防御性校验
- [ ] **未完成**
- **位置**: `src/service/raft_service_impl.rs`
- **问题**:
  - 无 offset 连续性校验（任意 offset 直接 seek 写入，乱序块会静默产生损坏文件）
  - HashMap key 只有 snapshot_id（不同来源的快照流可能冲突）
  - `guard.remove(&snapshot_id).unwrap()` 依赖前置条件，若已被 evict 会 panic
- **修法**: 校验 offset 与当前文件长度一致；冲突时拒绝并要求重传；`remove` 改为防御性处理。

### 19. `Endpoint::parse` 不支持 IPv6
- [ ] **未完成**
- **位置**: `src/config/endpoint.rs` 的 `parse()`
- **问题**: `splitn(2, ':')` 无法解析 `[::1]:8080` 形式的 IPv6 地址。
- **修法**: 使用 `SocketAddr::from_str` 或处理方括号语法。

### 20. snapshot_id 用 `now_millis()` 可能碰撞
- [ ] **未完成**
- **位置**: `src/raft/store/snapshot/build.rs`
- **问题**: 同一毫秒内连续 trigger snapshot 会生成相同 ID，覆盖已有快照（集成测试有 rapid sequential 场景）。
- **修法**: ID 中加入原子自增序列或随机后缀。

### 21. mobc `Manager::check` 空实现
- [ ] **未完成**
- **位置**: `src/network/pool/manager.rs`
- **问题**: `check()` 直接返回 Ok，不校验 channel 存活，可能把已断连的连接分发给调用方（失败重试会掩盖问题但增加延迟）。
- **修法**: 至少校验 channel 状态，或在失败时从池中剔除并重试一次。

---

## 五、小问题

### 22. example/README.md 与实现不符
- [ ] **未完成**
- **位置**: `example/README.md` 的 "Read Path" 章节
- **问题**: 文档说"读直接读本地 RocksDB，无需 Raft 共识"，实际 `read()` 走 `execute_or_forward` 转发到 leader。

### 23. `From<ApiError> for Error` 丢失 leader 重定向信息
- [ ] **未完成**
- **位置**: `src/error.rs`
- **问题**: 转换为 `Error::retryable_with_reason(LeaderTransition)` 时丢弃了 `ForwardToLeader.leader_id`，远程转发错误丢失重定向信息。

### 24. 服务端 gRPC 无 TLS/auth 能力
- [ ] **未完成**
- **位置**: `src/node/cluster.rs` 的 `start_raft_service()`
- **问题**: 客户端 TLS 配置（`RpcClientTlsConfig`）存在但没接线，服务端无 TLS，且无任何认证机制。

### 25. 无 Prometheus metrics 导出
- [ ] **未完成**
- **位置**: 整个 crate
- **问题**: 无可观测性指标导出，排查线上问题只能靠日志。

---

## 建议的优化优先级

1. **#1**（Txn 同批次读旧值 → 状态分歧风险）
2. **#2**（快照异步恢复竞态）
3. **#4**（error 编解码协议统一）
4. **#9**（磁盘无限增长）、**#6/#7/#8**（死代码清理或实现决策）
5. **#11/#12**（转发性能）
6. **#16**（shutdown 正确性）
7. 其余按需

---

## 已知设计权衡（记录，非问题）

- **postcard + protobuf 双轨序列化**: protobuf 仅做 gRPC 传输帧（`bytes` 字段），实际数据用 postcard。理由：OpenRaft 泛型类型需要 serde；所有节点同版本 Rust，无需跨语言互操作。已在 `encoder.rs` 有说明文档。
- **读是非线性一致读**: 读 leader 本地已 apply 数据，无 ReadIndex/lease。需要明确文档化。
- **`get_leader` 依赖 metrics watch**: 2s 超时轮询，election 期间会有感知延迟。