# JMQX 性能优化路线图

本文档用于沉淀 JMQX 当前阶段的性能瓶颈判断、优化优先级和压测验收标准，方便研发、测试和运维协同推进。

## 1. 目标

- 建立一套可信的单机与集群压测基线
- 明确当前系统的主要性能瓶颈
- 按阶段推进优化，避免一次性大改带来过高风险
- 为后续 `100k / 200k / 300k` 连接目标提供执行依据

## 2. 当前高优先级瓶颈

### 2.1 外部鉴权/ACL 仍在热路径内同步执行

当前 `CONNECT` 和 `PUBLISH` 主链路里，连接鉴权和 ACL 鉴权仍可能直接触发同步外部调用：

- `CONNECT -> clientAuthenticator.authenticateResult(...)`
- `PUBLISH -> aclAuthorizer.isAllowed(...)`

如果使用 HTTP / Redis / 数据库等外部数据源，Broker 的协议处理线程会直接受到下游服务抖动影响。

风险表现：

- 建连速率下降
- 发布延迟出现尖峰
- 单个下游依赖故障放大成全局连接波动

### 2.2 元数据提交仍有同步等待

当前元数据命令虽然具备失败重试队列，但首次提交仍然是同步执行：

- 订阅注册/注销
- 客户端上线事件
- 断连后的鉴权缓存驱逐
- retained 复制

在集群模式下，这意味着 `CONNECT / SUBSCRIBE / DISCONNECT` 等协议链路仍然会受 Raft 与节点间网络延迟影响。

风险表现：

- SUBSCRIBE RTT 偏高
- 集群抖动时 CONNECT 明显变慢
- Core / Replicant 之间网络抖动直接影响协议吞吐

### 2.3 retained 写入会进入发布主路径

当前 retained 消息写入时，会在普通发布链路里同步触发元数据复制。

风险表现：

- retained 发布延迟明显高于普通发布
- retained 场景下吞吐上限会被一致性路径拉低

### 2.4 Bridge 路径仍存在明显吞吐瓶颈

当前 Bridge 能力已经可用，但仍存在两个明显约束：

- 异步 bridge 实际只有单消费者
- MySQL bridge 仍是“每条消息新建一个 JDBC 连接”

风险表现：

- bridge 启用后吞吐骤降
- MySQL bridge 在中高流量下很快成为瓶颈
- queue 满后会出现消息丢弃

### 2.5 QoS2 的可靠性增强带来了同步存储成本

为了缩小 QoS2 的崩溃窗口，当前 inflight 状态存储改得更保守，安全性更好，但同步 RocksDB 写会明显增加热路径成本。

风险表现：

- QoS2 吞吐明显低于 QoS0 / QoS1
- 磁盘写延迟会直接反映到发布时延

### 2.6 会话恢复判断仍有扫描成本

当前 CONNECT 恢复会话时，需要检查：

- 订阅状态
- QoS1 inflight
- QoS2 outbound
- QoS2 inbound

在重连风暴下，这类“恢复前扫描”会放大 CPU 与存储读取压力。

## 3. 本周能改

### 3.1 固化纯 Broker 基线压测配置

建议固定以下基线：

- `QoS0`
- 关闭 retained
- 关闭 bridge
- 关闭外部 auth / acl
- 仅 MQTT over TCP
- 长连接 + 心跳主导

目标：

- 先拿到“Broker 本体”的可重复结果
- 后续所有功能项都基于该基线做对比

### 3.2 收紧 HTTP auth / ACL 默认配置

建议默认值：

- `connectTimeoutMs = 100 ~ 300`
- `requestTimeoutMs = 100 ~ 300`
- 本地缓存默认开启
- 限流默认开启
- 超时快速失败

目标：

- 避免下游鉴权服务拖慢 Broker
- 把故障影响限制在最小范围

### 3.3 给关键热路径补齐指标

至少补这些指标：

- CONNECT 鉴权耗时
- PUBLISH ACL 耗时
- metadata submit 耗时
- RocksDB inflight 读写耗时
- bridge queue backlog / drop count
- EventLoop 延迟

目标：

- 先量化热点，再做优化
- 所有优化都能回看收益

### 3.4 先修 MySQL bridge

建议立即做：

- 引入连接池
- 复用 PreparedStatement 或批量写
- 为异步 worker 提供批量 flush 能力

目标：

- 去掉“每条消息建连接”这种高成本操作

### 3.5 压测模板统一化

建议把以下内容统一沉淀为固定模板：

- JVM 参数
- Linux 内核参数
- JMQX 启动配置
- Bench 启动命令
- 指标采集项

目标：

- 降低每次压测的环境漂移
- 保证多轮数据可比

## 4. 下个版本改

### 4.1 元数据提交彻底异步化

目标方案：

- 协议线程只负责把命令写入本地队列
- metadata worker 异步提交到 Core / Raft
- 失败进入统一重试链
- 协议线程不直接等待提交结果

预期收益：

- CONNECT / SUBSCRIBE / DISCONNECT 明显去抖
- 集群网络波动不再直接卡住协议线程

### 4.2 HTTP auth / ACL 脱离 I/O 线程

目标方案：

- 协议线程负责解码和快速校验
- auth / acl worker 执行外部调用
- 回调到 channel 上写响应

预期收益：

- EventLoop 更稳定
- 外部鉴权服务慢时不会直接拖死接入线程

### 4.3 retained 复制异步化

目标方案：

- retained 修改进入专用复制队列
- 普通发布主路径只做本地投递
- 集群一致性复制后台推进

预期收益：

- retained 发布路径和普通发布路径解耦

### 4.4 重构 bridge 并发模型

目标方案：

- 真正支持多 worker 并行消费
- 区分“本地排队”和“下游发送”
- 提供可观测的堆积、超时和丢弃指标

预期收益：

- bridge 吞吐更平滑
- 更适合 Kafka / RocketMQ / MySQL 等异构目标

### 4.5 增加会话状态摘要索引

目标方案：

- 为每个 `clientId` 维护 `hasSubscription / hasQos1 / hasQos2 / hasSessionState` 摘要位
- CONNECT 恢复时不再扫描多个存储结构

预期收益：

- 重连恢复更快
- 大规模重连场景更稳定

### 4.6 QoS2 提供性能模式分层

建议增加三档策略：

- `strict`：可靠性优先，同步持久化
- `balanced`：批量持久化，平衡吞吐与恢复能力
- `memory_first`：性能优先，适合压测或低风险场景

预期收益：

- 不同业务场景可以按需选择
- 避免“一套 QoS2 策略打所有场景”

## 5. 压测执行顺序

建议按以下顺序推进：

1. 单机纯 Broker 基线
2. 单机开启 auth
3. 单机开启 acl
4. 单机开启 retained
5. 单机开启 bridge
6. 单机 QoS2 专项压测
7. 集群 `3 Core + N Replicant` 混合压测
8. 长稳压测（建议 `12h`）

## 6. 压测验收标准

### 6.1 单机基线

- `100k / 200k / 300k` 分阶段建连
- 在线稳定率 `>= 99.9%`
- 无 OOM
- 无持续性 RSS 无界增长
- 无明显 GC 长暂停

### 6.2 功能退化评估

每打开一项功能，都要和基线对比：

- 吞吐退化比例
- 建连速率退化比例
- 平均时延与 p99 时延变化
- CPU / RSS / GC 变化

重点功能项：

- auth
- acl
- retained
- bridge
- QoS2

### 6.3 集群稳定性

- `3 Core + 2 Replicant` 下概览与客户端数一致
- 跨节点路由正确
- leader 切换后元数据写链路恢复正常
- 黑名单/鉴权/桥接配置同步正常

## 7. 指标采集清单

建议统一采集：

- 在线连接数
- 建连成功率 / 失败率
- 发布成功率 / 失败率
- 进程 RSS / Heap / Direct Memory
- CPU 使用率
- GC 停顿（avg / p95 / p99 / max）
- EventLoop 延迟
- HTTP auth/ACL 调用耗时
- metadata submit RTT
- RocksDB put/get p99
- bridge queue backlog / drop count

## 8. 推荐推进顺序

### 第一阶段：先有可重复结果

- 固化压测模板
- 固化 JVM / Linux / Broker 配置
- 建立单机纯 Broker 基线

### 第二阶段：先修最粗的热点

- 修 MySQL bridge
- 收紧 HTTP auth/ACL 默认超时
- 补齐关键热路径指标

### 第三阶段：解决系统性阻塞

- metadata submit 异步化
- auth / acl 异步化
- retained 异步化

### 第四阶段：做结构性性能提升

- bridge 真并发
- session 摘要索引
- QoS2 模式分层

## 9. 结论

当前 JMQX 的主要性能瓶颈，不在 Netty 本身，而在于：

- 热路径里仍然存在同步外部调用
- 热路径里仍然存在同步共识提交
- 部分能力（Bridge、QoS2、Retained）仍以“功能完整”为主，尚未完全优化到高并发形态

优化路线的核心原则是：

- 先建立可信基线
- 再把外部依赖和共识提交流程从主协议线程剥离
- 最后再做结构性吞吐优化

这样推进，风险最低，也最容易拿到稳定收益。
