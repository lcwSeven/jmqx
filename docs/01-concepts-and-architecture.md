# JMQX 概念与架构

## 1. 设计目标

JMQX 的核心目标是：

- 保持代码结构清晰，便于二次开发
- 支持插件化鉴权与桥接能力
- 通过 Core/Replicant 架构支持连接规模扩展
- 通过 Raft 保证元数据强一致

## 2. 关键概念

### 2.1 Core

Core 节点负责：

- 接收并提交元数据写请求（订阅注册/取消）
- 参与 Raft 选主与日志复制
- 对 Replicant 下发元数据增量事件与快照

### 2.2 Replicant

Replicant 节点负责：

- 承载海量客户端连接
- 持有本地只读全局路由副本
- 向 Core 发送元数据写请求
- 接收 Core 元数据同步流并本地应用

### 2.3 全局订阅表

全局路由结构维护：

- 普通订阅：`topicFilter -> nodeId 集合`
- 共享订阅：`sharedGroup + topicFilter -> nodeId 集合`
- 反向索引：`nodeId -> topicKey 集合`

## 3. 数据链路

## 3.1 SUBSCRIBE / UNSUBSCRIBE

1. 节点先更新本地订阅表
2. 向 Core 提交元数据命令
3. Core 通过 Raft 提交并应用
4. Core 向 Replicant 广播 EVENT
5. 全节点更新全局路由读模型

## 3.2 PUBLISH

1. 本地匹配本地路由表
2. 全局路由决定远端目标节点
3. 共享订阅采用“两级轮询”：
   - 先在节点层轮询
   - 再在节点内客户端轮询
4. 本地直接投递；远端通过集群消息通道投递

## 3.3 Replicant 重连补齐

当 Replicant 重连：

- 若 Core 缓冲中有完整增量，直接从断点继续 replay
- 若断档过大，Core 下发 RESET + Snapshot 全量重建

## 4. 一致性与可用性说明

- 元数据写路径为 `Raft-only`
- 取消了本地伪提交 fallback
- Replicant 断链后采用延迟清理策略，避免短暂抖动误删路由

## 5. 模块责任边界

- `jmqx-core`：业务逻辑（会话、路由、消息处理）
- `jmqx-cluster`：一致性与同步协议（Raft + Netty）
- `jmqx-transport`：接入层协议与连接生命周期
- `jmqx-app`：装配和启动配置

