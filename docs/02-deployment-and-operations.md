# JMQX 部署与运维手册

## 1. 环境准备

- JDK 17
- Maven 3.9+
- Linux 内核参数按连接规模调优（`ulimit`、`somaxconn`、`tcp_tw_reuse` 等）

## 2. 部署模式

## 2.1 单机模式

适用于开发、自测、小规模接入。

启动：

```bash
mvn -pl jmqx-app -am exec:java
```

## 2.2 集群模式（Core + Replicant）

推荐拓扑：

- Core：3 台
- Replicant：按接入规模扩容

启动顺序：

1. 启动 Core 并确认选主
2. 启动 Replicant 并等待元数据追平
3. 切流客户端

## 3. 关键配置建议

### 3.1 Core 配置重点

- `jmqx.cluster.raft.serverId`：必须是可达地址
- `jmqx.cluster.raft.initialConf`：首次建群必须一致
- `jmqx.cluster.nodeDownCleanupDelayMs`：建议 10s~60s，根据网络抖动调节
- `jmqx.cluster.replay.maxEvents`：按写入速率预留足够重放缓冲

### 3.2 Replicant 配置重点

- `jmqx.cluster.coreEndpoints`：填全部 Core 元数据地址
- `jmqx.cluster.netty.reconnectBackoffMs`：避免过低导致抖动风暴
- `jmqx.cluster.netty.ackBatchSize` 与 `ackFlushIntervalMs`：平衡实时性与吞吐

## 4. 可观测性建议

上线至少补齐以下监控：

- 在线连接数、连接建立/断开速率
- PUBLISH 入站速率、投递速率、失败速率
- Core 角色状态（leader/follower）
- 元数据重放滞后（Replicant lastAppliedLogIndex）
- ACK 积压与 in-flight 事件窗口
- 共享订阅慢消费者淘汰次数

## 5. 常见排障流程

## 5.1 共享订阅收不到消息

1. 检查订阅格式 `$share/{group}/{topicFilter}`
2. 检查发布 topic 与 filter 匹配
3. 检查 ACL/Auth 决策日志
4. 检查客户端 channel 是否 `active` 且 `writable`
5. 检查 Core 元数据提交是否成功
6. 检查 Replicant 是否已追平元数据

## 5.2 Replicant 一直重连 Core

1. 检查 `coreEndpoints` 可达性
2. 检查 Core 端口监听和防火墙
3. 检查请求超时是否过低
4. 检查 Core 是否频繁 leader 切换

## 5.3 JDK 17 反射封装报错

报错示例：

`Unable to make field transient java.lang.Object[] java.util.ArrayList.elementData accessible`

解决：

- Maven 启动使用 `.mvn/jvm.config`（已内置）
- IDE 启动增加 VM 参数：
  - `--add-opens=java.base/java.util=ALL-UNNAMED`

## 6. 升级与变更建议

1. 先灰度 Replicant，再滚动 Core
2. 保持 `initialConf` 与现网成员管理策略一致
3. 升级前做一次快照与配置备份
4. 升级后优先验证：
   - 元数据写入链路
   - 共享订阅投递链路
   - 节点下线清理链路

