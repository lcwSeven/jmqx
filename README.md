# JMQX

JMQX 是一个基于 Java + Netty 的 MQTT Broker，目标是提供可读、可扩展、可演进的 MQTT 服务端实现。

## 文档导航

- 快速开始：见“快速开始”章节
- 架构与集群：见“架构总览”与“集群部署”
- 配置参考：见“核心配置”
- 常见问题：见“FAQ”
- 分册文档：
  - [概念与架构](docs/01-concepts-and-architecture.md)
  - [部署与运维](docs/02-deployment-and-operations.md)
  - [最佳实践](docs/03-best-practices.md)
  - [配置项完整参考](docs/04-configuration-reference.md)
  - [集群故障演练手册](docs/05-cluster-failure-drill.md)
  - [管理页面（Admin Console）](docs/06-admin-console.md)

## 版本兼容矩阵

| 组件 | 当前建议版本 | 说明 |
|---|---|---|
| JDK | 17 | 项目编译与运行基线 |
| Maven | 3.9+ | 用于多模块构建与启动 |
| Netty | 4.2.x | 网络层 |
| SOFAJRaft | 1.4.x | Core 元数据强一致复制 |
| RocksDB（Java） | 由依赖树确定 | Retained 消息存储 |

## 架构总览

### 模块划分

- `jmqx-common`：通用配置与工具
- `jmqx-protocol`：协议边界接口
- `jmqx-plugin`：Auth / ACL 插件实现
- `jmqx-core`：Broker 核心（会话、路由、存储）
- `jmqx-cluster`：集群元数据与节点间消息通道
- `jmqx-transport`：MQTT / MQTTS / WS / WSS 接入
- `jmqx-bench`：压测工具
- `jmqx-app`：启动装配 + 内嵌管理页面（Admin Console）

### 运行角色

- `Core`：元数据写入入口，Raft 共识节点
- `Replicant`：元数据只读副本，承载大量连接

也支持仅 Core 组成集群（无 Replicant）。

## 快速开始

### 环境要求

- JDK 17
- Maven 3.9+

### 编译

```bash
mvn -DskipTests compile
```

### 启动 Broker

```bash
mvn -pl jmqx-app -am exec:java
```

### 访问管理页面

默认地址：

- `http://127.0.0.1:18081/admin/`

默认配置（见 `jmqx-app/src/main/resources/jmqx.yaml`）：

- `jmqx.admin.panel.enabled=true`
- `jmqx.admin.panel.host=0.0.0.0`
- `jmqx.admin.panel.port=18081`
- `jmqx.admin.panel.basePath=/admin`

集群统一概览补充：

- 需要将各节点的 `jmqx.admin.enabled=true`
- 需要将各节点的 `jmqx.admin.url` 指向同一个管理端地址，例如 `http://10.0.0.10:18081`
- `jmqx.admin.clusterId` 需要保持一致，管理页的“集群概览”才会按整个集群聚合

### 启动压测工具

```bash
mvn -pl jmqx-bench -am exec:java -Dexec.args="--host=127.0.0.1 --port=1883 --clients=10000 --connectRate=2000 --subscribe=false --publishRate=0"
```

## 集群部署

### 推荐拓扑

- 3 个 Core（奇数节点保证多数派）
- N 个 Replicant（按连接规模横向扩容）

### 核心配置（节选）

| 配置项 | 默认值 | 角色 | 说明 |
|---|---:|---|---|
| `jmqx.cluster.role` | `core` | Core/Replicant | 节点角色 |
| `jmqx.cluster.coreEndpoints` | `127.0.0.1:7800` | 全部 | Core 元数据地址列表 |
| `jmqx.cluster.core.bindHost` | `0.0.0.0` | Core | 元数据服务监听地址 |
| `jmqx.cluster.core.port` | `7800` | Core | 元数据服务端口 |
| `jmqx.cluster.raft.groupId` | `jmqx-metadata` | Core | Raft 组名 |
| `jmqx.cluster.raft.serverId` | `127.0.0.1:17800` | Core | 当前 Core 的 Raft 地址 |
| `jmqx.cluster.raft.initialConf` | `127.0.0.1:17800` | Core | 初始 Core 成员 |
| `jmqx.cluster.nodeDownCleanupDelayMs` | `15000` | Core | Replicant 断链延迟清理窗口 |
| `jmqx.cluster.replay.maxEvents` | `200000` | Core | 元数据重放缓冲上限 |

说明：

- 当前集群路径为 `Raft-only`，不再支持内存伪集群写路径。
- `jmqx.cluster.raft.serverId` 必须是可达 `IP:Port`，不能是 `0.0.0.0`。

## 核心配置

配置优先级：

`JVM -D` > `jmqx.yaml`（覆盖层） > `config/*.yaml`（模块默认） > 代码默认值

主配置文件：

- `jmqx-app/src/main/resources/jmqx.yaml`
- `jmqx-app/src/main/resources/jmqx-300k.yaml`

网络监听常用参数：

```bash
-Djmqx.broker.host=0.0.0.0
-Djmqx.broker.port=1883
-Djmqx.broker.mqtts.enabled=false
-Djmqx.broker.websocket.enabled=true
-Djmqx.broker.websocket.port=8083
-Djmqx.broker.websocket.path=/mqtt
-Djmqx.broker.wss.enabled=false
```

Retained 常用参数：

```bash
-Djmqx.retained.rocksdb.path=data/retained-rocksdb
-Djmqx.retained.maxEntries=100000
-Djmqx.retained.maxBytes=268435456
-Djmqx.retained.maxPayloadBytes=1048576
-Djmqx.retained.overflowStrategy=evict_lru
```

共享订阅常用参数：

```bash
-Djmqx.shared.maxSubscribersPerGroup=1000
-Djmqx.shared.slowConsumerStrikeThreshold=3
```

## 管理页面（Admin Console）

### 功能范围

- 集群概览：节点连接数、流量、节点角色
- 客户端列表与详情：`clientId / username / IP / keepalive / 连接方式 / 订阅主题`
- ACL 鉴权配置：启用状态、插件链、缓存时间
- 连接鉴权配置：启用状态、插件链、缓存时间、向导式创建
- 集群配置：共享订阅成员上限等参数

### 数据打通状态

已打通（读链路）：

- 概览与客户端数据来自 JMQX 运行态（`ConnectionMetrics`、`SessionRegistry`、`SubscriptionRegistry`）
- 实时数据来自 Broker 内部主题（`$SYS/dashboard/{clusterId}/...`）

已打通（写链路，动态生效）：

- `security/config` 保存后，动态重载 AUTH / ACL 插件链（无须重启）
- `cluster/config` 中 `sharedSubscriptionMaxMembersPerGroup` 保存后，动态更新共享订阅限制

暂未动态生效（仅管理态保存）：

- `coreNodes / replicantNodes / coreAcceptClientConnections`

详细设计与接口见 [管理页面（Admin Console）](docs/06-admin-console.md)。

## 插件能力

Auth 类型：

- `allow_all` / `http` / `file` / `redis` / `db`

ACL 类型：

- `allow_all` / `http` / `file` / `redis`

Bridge 类型：

- `kafka` / `rocketmq` / `mysql`

## FAQ

### 1. 启动报错 `Unable to make field ... ArrayList.elementData accessible`？

这是 JDK 17 模块封装导致的反射限制。  
项目已内置 `.mvn/jvm.config`：

`--add-opens=java.base/java.util=ALL-UNNAMED`

如果你用 IDE 直接运行 `JmqxApplication`，请在 VM options 手动加同样参数。

### 2. 共享订阅客户端收不到消息？

优先检查：

1. 订阅格式是否为 `$share/{group}/{topicFilter}`  
2. 发布 topic 是否匹配 `topicFilter`  
3. Auth/ACL 是否放行 SUBSCRIBE 和 PUBLISH  
4. 客户端连接是否 active/writable  
5. 集群模式下元数据写入是否成功并已同步到目标节点

### 3. Core 节点是否接收客户端连接？

支持。当前实现中 Core 与 Replicant 都可接入客户端，区别在于元数据职责。

### 4. 是否支持仅 Core 集群？

支持。可以不部署 Replicant，仅由多个 Core 提供服务。

## 生产建议

1. 上线前做故障演练（Leader 切换、网络抖动、节点重连）。
2. 用业务真实 topic/连接模型完成基线压测。
3. 补齐监控告警（连接数、消息速率、重放延迟、ACK 积压、慢消费者淘汰）。

## 许可证

见 [LICENSE](LICENSE)。
