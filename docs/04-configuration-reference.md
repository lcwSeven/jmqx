# JMQX 配置项完整参考

本文档以 `jmqx-app/src/main/resources/config/*.yaml` 为默认配置基线，按功能域分组说明配置项。  
`jmqx-app/src/main/resources/jmqx.yaml` 作为覆盖层，优先级高于模块默认配置。

## 1. 配置优先级

`JVM -D` > `jmqx.yaml`（覆盖层） > `config/*.yaml`（模块默认） > 代码默认值

默认模块文件：

- `config/broker.yaml`
- `config/security-auth.yaml`
- `config/security-acl.yaml`
- `config/retained.yaml`
- `config/shared.yaml`
- `config/cluster.yaml`
- `config/admin.yaml`
- `config/bridge.yaml`

## 2. Broker 网络层

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.broker.host` | `0.0.0.0` | MQTT 监听地址 |
| `jmqx.broker.port` | `1883` | MQTT 监听端口 |
| `jmqx.broker.mqtts.enabled` | `false` | 是否启用 MQTTS |
| `jmqx.broker.mqtts.host` | `0.0.0.0` | MQTTS 监听地址 |
| `jmqx.broker.mqtts.port` | `8883` | MQTTS 监听端口 |
| `jmqx.broker.bossThreads` | `1` | Netty boss 线程数 |
| `jmqx.broker.workerThreads` | `0` | Netty worker 线程数（0 表示 Netty 默认） |
| `jmqx.broker.readerIdleSeconds` | `120` | 读空闲超时（秒，0 表示关闭） |
| `jmqx.broker.maxQos` | `2` | Broker 最大 QoS（`0/1/2`） |
| `jmqx.broker.websocket.enabled` | `true` | 是否启用 WS |
| `jmqx.broker.websocket.host` | `0.0.0.0` | WS 监听地址 |
| `jmqx.broker.websocket.port` | `8083` | WS 监听端口 |
| `jmqx.broker.websocket.path` | `/mqtt` | WS 握手路径 |
| `jmqx.broker.wss.enabled` | `false` | 是否启用 WSS |
| `jmqx.broker.wss.host` | `0.0.0.0` | WSS 监听地址 |
| `jmqx.broker.wss.port` | `8084` | WSS 监听端口 |
| `jmqx.broker.wss.path` | `/mqtt` | WSS 握手路径 |
| `jmqx.broker.rateLimit.clientId.enabled` | `false` | 是否启用按 clientId 限流 |
| `jmqx.broker.rateLimit.clientId.perSecond` | `0` | 按 clientId 每秒 PUBLISH 限额（`0` 表示关闭） |
| `jmqx.broker.rateLimit.ip.enabled` | `false` | 是否启用按 IP 限流 |
| `jmqx.broker.rateLimit.ip.perSecond` | `0` | 按 IP 每秒 PUBLISH 限额（`0` 表示关闭） |
| `jmqx.broker.rateLimit.publish.strategy` | `fixed_window` | PUBLISH 限流策略：`fixed_window` / `sliding_window` / `token_bucket` |
| `jmqx.broker.rateLimit.connect.enabled` | `false` | 是否启用 CONNECT 限流 |
| `jmqx.broker.rateLimit.connect.globalPerSecond` | `0` | 全局每秒 CONNECT 限额（`0` 表示关闭） |
| `jmqx.broker.rateLimit.connect.ipPerSecond` | `0` | 单 IP 每秒 CONNECT 限额（`0` 表示关闭） |
| `jmqx.broker.rateLimit.connect.strategy` | `fixed_window` | CONNECT 限流策略：`fixed_window` / `sliding_window` / `token_bucket` |
| `jmqx.broker.rateLimit.cleanupIntervalSeconds` | `60` | 限流状态清理周期（秒） |
| `jmqx.broker.rateLimit.idleSeconds` | `300` | 限流键空闲清理阈值（秒） |

TLS 配置：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.broker.tls.certChainFile` | 空 | 证书链（PEM） |
| `jmqx.broker.tls.privateKeyFile` | 空 | 私钥文件（PEM） |
| `jmqx.broker.tls.privateKeyPassword` | 空 | 私钥密码 |

## 3. Retained 存储

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.retained.enabled` | `true` | 是否启用 Retained |
| `jmqx.retained.rocksdb.path` | `data/retained-rocksdb` | RocksDB 路径 |
| `jmqx.retained.maxEntries` | `100000` | 最大热缓存条数 |
| `jmqx.retained.maxBytes` | `268435456` | 最大热缓存字节数 |
| `jmqx.retained.maxPayloadBytes` | `1048576` | 单条 payload 最大字节数 |
| `jmqx.retained.overflowStrategy` | `evict_lru` | 超限策略：`evict_lru` / `reject_new` |

## 4. 共享订阅

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.shared.maxSubscribersPerGroup` | `1000` | 每个共享组最大订阅者数 |
| `jmqx.shared.slowConsumerStrikeThreshold` | `3` | 慢消费者剔除阈值 |

## 5. 集群配置（Raft-only）

通用：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.cluster.role` | `core` | 角色：`core` / `replicant` |
| `jmqx.cluster.coreEndpoints` | `127.0.0.1:7800` | Core 元数据地址列表（逗号分隔） |

Core 监听：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.cluster.core.bindHost` | `0.0.0.0` | Core 元数据服务监听地址 |
| `jmqx.cluster.core.port` | `7800` | Core 元数据服务端口 |

Replicant 与同步：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.cluster.netty.requestTimeoutMs` | `3000` | 元数据写请求超时 |
| `jmqx.cluster.netty.reconnectBackoffMs` | `1000` | 重连退避（毫秒） |
| `jmqx.cluster.netty.ackBatchSize` | `64` | ACK 批量阈值 |
| `jmqx.cluster.netty.ackFlushIntervalMs` | `200` | ACK 刷盘间隔（毫秒） |
| `jmqx.cluster.netty.replicantMaxInFlightEvents` | `2048` | Core 对单 Replicant 最大未 ACK 窗口 |
| `jmqx.cluster.netty.replicantPushBatchSize` | `256` | Core 单次推送上限 |
| `jmqx.cluster.nodeDownCleanupDelayMs` | `15000` | Replicant 断链后延迟清理窗口（毫秒） |
| `jmqx.cluster.replay.maxEvents` | `200000` | Core 重放缓冲上限 |

跨节点消息转发：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.cluster.message.bindHost` | `0.0.0.0` | 节点间消息监听地址 |
| `jmqx.cluster.message.port` | `7900` | 节点间消息监听端口 |
| `jmqx.cluster.nodeEndpoints` | `node-1=127.0.0.1:7900` | 节点 ID 到消息端点映射 |

Raft：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.cluster.raft.groupId` | `jmqx-metadata` | Raft 组名 |
| `jmqx.cluster.raft.serverId` | `127.0.0.1:17800` | 当前 Core 的 Raft 地址（必须可达） |
| `jmqx.cluster.raft.initialConf` | `127.0.0.1:17800` | 初始 Core 成员列表 |
| `jmqx.cluster.raft.dataPath` | `data/raft-metadata` | Raft 日志/快照路径 |
| `jmqx.cluster.raft.electionTimeoutMs` | `1000` | 选举超时 |
| `jmqx.cluster.raft.snapshotIntervalSecs` | `30` | 快照间隔（秒） |

## 6. 认证（Auth）

通用：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.auth.type` | `allow_all` | 插件类型 |
| `jmqx.auth.chain` | 空 | 链式执行顺序（逗号分隔） |
| `jmqx.auth.cacheMillis` | `60000` | 缓存时间（毫秒） |

HTTP：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.auth.http.url` | `http://127.0.0.1:8080/auth/check` | 鉴权接口 |
| `jmqx.auth.http.timeoutMs` | `2000` | 超时时间（毫秒） |

File：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.auth.file.path` | `auth-users.txt` | 本地用户文件 |

Redis：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.auth.redis.host` | `127.0.0.1` | Redis 地址 |
| `jmqx.auth.redis.port` | `6379` | Redis 端口 |
| `jmqx.auth.redis.password` | 空 | Redis 密码 |
| `jmqx.auth.redis.db` | `0` | Redis DB |
| `jmqx.auth.redis.keyPrefix` | `jmqx:auth` | 键前缀 |
| `jmqx.auth.redis.timeoutMs` | `2000` | 超时（毫秒） |

DB：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.auth.db.driver` | 空 | JDBC 驱动类 |
| `jmqx.auth.db.url` | `jdbc:mysql://127.0.0.1:3306/jmqx` | JDBC 地址 |
| `jmqx.auth.db.user` | `root` | 数据库用户名 |
| `jmqx.auth.db.password` | 空 | 数据库密码 |
| `jmqx.auth.db.query` | `SELECT password FROM mqtt_user WHERE username = ?` | 查询 SQL |

## 7. 鉴权（ACL）

通用：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.acl.type` | `allow_all` | ACL 插件类型 |
| `jmqx.acl.defaultAllow` | `false` | 未命中默认放行 |
| `jmqx.acl.cacheMillis` | `60000` | 缓存时间（毫秒） |

HTTP：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.acl.http.url` | `http://127.0.0.1:8080/acl/check` | ACL 接口 |
| `jmqx.acl.http.timeoutMs` | `2000` | 超时（毫秒） |

Redis：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.acl.redis.host` | `127.0.0.1` | Redis 地址 |
| `jmqx.acl.redis.port` | `6379` | Redis 端口 |
| `jmqx.acl.redis.password` | 空 | Redis 密码 |
| `jmqx.acl.redis.db` | `0` | Redis DB |
| `jmqx.acl.redis.keyPrefix` | `jmqx:acl` | 键前缀 |
| `jmqx.acl.redis.timeoutMs` | `2000` | 超时（毫秒） |

File：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.acl.file.path` | `acl-rules.txt` | ACL 规则文件 |

## 8. 消息桥接（Bridge）

通用：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.bridge.enabled` | `false` | 是否启用桥接 |
| `jmqx.bridge.types` | `kafka,rocketmq,mysql` | 目标类型列表 |
| `jmqx.bridge.async.enabled` | `true` | 是否异步（兼容旧键：`jmqx.bridge.async`） |
| `jmqx.bridge.async.queueCapacity` | `10000` | 异步队列容量 |
| `jmqx.bridge.async.workerCount` | `1` | 异步 worker 数 |

Kafka：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.bridge.kafka.bootstrapServers` | `127.0.0.1:9092` | Kafka 地址 |
| `jmqx.bridge.kafka.topic` | `jmqx-messages` | 目标 topic |
| `jmqx.bridge.kafka.acks` | `1` | acks 策略 |
| `jmqx.bridge.kafka.clientId` | `jmqx-bridge` | clientId |
| `jmqx.bridge.kafka.compressionType` | `none` | 压缩类型 |

RocketMQ：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.bridge.rocketmq.nameServer` | `127.0.0.1:9876` | NameServer 地址 |
| `jmqx.bridge.rocketmq.producerGroup` | `jmqx-bridge-group` | 生产者组 |
| `jmqx.bridge.rocketmq.topic` | `JMQX_MESSAGES` | 目标 topic |
| `jmqx.bridge.rocketmq.syncSend` | `false` | 同步发送 |
| `jmqx.bridge.rocketmq.timeoutMs` | `3000` | 超时（毫秒） |

MySQL：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `jmqx.bridge.mysql.driver` | 空 | JDBC 驱动类 |
| `jmqx.bridge.mysql.url` | `jdbc:mysql://127.0.0.1:3306/jmqx` | JDBC 地址 |
| `jmqx.bridge.mysql.user` | `root` | 用户名 |
| `jmqx.bridge.mysql.password` | 空 | 密码 |
| `jmqx.bridge.mysql.table` | `jmqx_bridge_message` | 表名 |
| `jmqx.bridge.mysql.autoCreateTable` | `true` | 启动自动建表 |

## 9. 推荐做法

1. 配置管理统一使用环境隔离文件，不要直接改默认模板。
2. 生产环境固定 Core 配置，变更采用灰度。
3. 对 `cacheMillis`、`timeoutMs`、`queueCapacity` 类参数建立基线。
4. 重大变更前执行 [集群故障演练手册](05-cluster-failure-drill.md)。
