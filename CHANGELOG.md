# Changelog

本文档记录 JMQX 的重要功能变更与架构调整。

## [Unreleased]

### 新增

- 文档体系升级为中文分册：
  - 概念与架构
  - 部署与运维
  - 最佳实践
  - 配置项完整参考
  - 集群故障演练手册
- 新增 `docs/README.md` 文档索引页。
- 新增 `.mvn/jvm.config`，内置 JDK 17 `--add-opens` 兼容参数。

### 变更

- 集群元数据路径切换为 `Raft-only`，移除内存伪集群网关。
- 共享订阅链路升级为“两级轮询”语义：
  - 先轮询节点
  - 再轮询节点内客户端
- 集群消息转发帧增加投递计划语义（普通订阅 + 共享组集合）。
- `PUBLISH` 路由改为始终查询本地订阅表，避免全局路由传播窗口导致本地漏投递。

### 修复

- 修复 ACL 判断命名语义歧义（`allowed` -> `isDenied`）。
- 修复 Replicant 元数据提交失败时的本地 fallback 一致性风险。
- 接入节点下线延迟清理流程，避免僵尸路由长期残留。
- 清理集群模块中的未使用代码与无效兜底路径。

### 移除

- 移除：
  - `InMemoryMetadataCommandGateway`
  - `NoopMetadataCommandGateway`
  - `NoopMetadataReplicator`

## [1.0.0] - 初始里程碑

### 能力

- MQTT/MQTTS/WS/WSS 基础接入。
- 会话管理、订阅管理、基础路由。
- Retained 消息存储（RocksDB）。
- Auth/ACL 插件化（allow_all/http/file/redis/db）。
- 基础消息桥接（Kafka/RocketMQ/MySQL）。

