# jmqx 单机 30 万连接里程碑

本清单用于第一个容量里程碑：在单节点上稳定承载 `300,000` 在线连接。

## 1. 范围

- 协议范围：仅 MQTT over TCP。
- 目标流量特征：低消息吞吐、心跳主导、长连接。
- 建议基线：
  - `QoS0`
  - 关闭 retained
  - 关闭 bridge
  - 关闭 MQTTS/WSS
  - 外部 auth/acl 关闭或强缓存

## 2. jmqx 运行时配置

使用 `jmqx-app/src/main/resources/jmqx-300k.yaml` 作为基线，并确保最终生效配置一致。

关键配置：

- `jmqx.broker.mqtts.enabled=false`
- `jmqx.broker.websocket.enabled=false`
- `jmqx.broker.wss.enabled=false`
- `jmqx.retained.enabled=false`
- `jmqx.bridge.enabled=false`
- `jmqx.auth.type=allow_all`
- `jmqx.acl.type=allow_all`

## 3. JVM 配置（参考）

Java 17 启动参数示例：

```bash
-Xms8g
-Xmx8g
-XX:+UseZGC
-XX:MaxDirectMemorySize=8g
-Dio.netty.allocator.type=pooled
-Dio.netty.leakDetection.level=disabled
```

说明：

- 建议 `Xms == Xmx`，避免运行期堆扩容抖动。
- Direct Memory 需要与连接规模一起规划。
- 压测期间建议关闭高开销调试项（如泄漏检测）。

## 4. Linux 基线（参考）

仅在压测环境执行：

```bash
ulimit -n 2000000
sysctl -w net.core.somaxconn=65535
sysctl -w net.core.netdev_max_backlog=250000
sysctl -w net.ipv4.tcp_max_syn_backlog=262144
sysctl -w net.ipv4.ip_local_port_range="10000 65535"
sysctl -w fs.file-max=4000000
```

同时确保：

- 主机内存与 CPU 核数充足。
- 网卡队列与 IRQ 亲和性已优化。
- 压测机器不运行其他重负载任务。

## 5. 压测阶段

阶段 A：

- 建连 `100k`，保持 `1h`。
- 验收：无 OOM、无大规模重连波动、内存趋势平稳。

阶段 B：

- 建连 `200k`，保持 `2h`。
- 验收：在线数稳定，CPU 不出现长时间满载。

阶段 C（里程碑）：

- 建连 `300k`，保持 `12h`。
- 验收：
  - 在线连接达到目标的 `99.9%` 以上
  - 无明显长时间 GC 停顿峰值
  - 无重连风暴
  - 进程内存无持续无界增长

## 6. 采集指标

- 在线连接数时序
- 进程 RSS / Heap / Direct Memory
- GC 停顿（avg / p99 / max）
- 新建连接速率与断开速率
- CPU 利用率与系统负载
- 错误日志按类型抽样统计

## 7. 常见失败信号

- RSS 快速增长：通常是 Direct Memory 压力或 Buffer 滞留。
- 重连峰值：常见于内核队列限制或 KeepAlive 抖动。
- EventLoop 延迟尖峰：常见于日志、bridge、auth 进入热路径。
- GC 停顿突增：常见于订阅/路由路径对象分配抖动。
