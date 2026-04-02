# jmqtt

`jmqtt` 是一个基于 Java + Netty + Spring Boot 的 MQTT Broker 多模块工程。

## 项目结构分析

- `jmqtt-common`: 通用配置与工具类（如 `BrokerProperties`、Topic 匹配器）。
- `jmqtt-protocol`: 协议扩展点（当前提供 `ClientAuthenticator` 认证接口）。
- `jmqtt-session`: 客户端会话管理（`SessionRegistry` + 内存实现）。
- `jmqtt-router`: 订阅路由管理（topic filter 到 clientId 的匹配）。
- `jmqtt-store`: retained message 存储（内存实现）。
- `jmqtt-broker`: MQTT 协议处理核心（CONNECT / SUBSCRIBE / PUBLISH / PINGREQ / DISCONNECT）。
- `jmqtt-transport`: Netty TCP Server 与 MQTT 编解码、消息分发。
- `jmqtt-app`: Spring Boot 启动模块，负责 Bean 装配与生命周期管理。

## 当前已填充能力

- 多模块 Maven 依赖关系已补齐，并统一继承父工程。
- 可启动的 MQTT TCP 服务端（默认 `0.0.0.0:1883`）。
- 支持消息流程：
  - CONNECT + CONNACK
  - SUBSCRIBE + SUBACK（订阅后回放 retained）
  - PUBLISH（支持 QoS 0/1 基本流，QoS1 返回 PUBACK）
  - PINGREQ + PINGRESP
  - DISCONNECT / 连接断开时会话清理
- 支持 retained message 的保存、删除（空 payload）与订阅回放。
- 提供认证扩展点，默认放行（`ClientAuthenticator.ALLOW_ALL`）。

## 启动方式

1. 安装 JDK 21。
2. 安装 Maven 3.9+。
3. 在仓库根目录执行：

```bash
mvn -DskipTests compile
mvn -pl jmqtt-app spring-boot:run
```

## 配置

`jmqtt-app/src/main/resources/application.yml`:

```yaml
jmqtt:
  broker:
    host: 0.0.0.0
    port: 1883
    boss-threads: 1
    worker-threads: 0
```
