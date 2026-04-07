# jmqx

`jmqx` 是一个基于 Java + Netty 的 MQTT Broker 多模块工程。

## 项目结构分析

- `jmqx-common`: 通用配置与工具类（`com.jmqx.common`）。
- `jmqx-protocol`: 核心协议边界接口（`com.jmqx.protocol`，如 `ClientAuthenticator`、`BrokerMessageHandler`），用于解耦 `transport` 与 `core`。
- `jmqx-plugin`: Auth/ACL 插件实现与工厂（`com.jmqx.auth`、`com.jmqx.acl`）。
- `jmqx-core`: Broker 核心、会话、路由、存储（`com.jmqx.broker`、`session/router/store`）。
- `jmqx-transport`: Netty TCP 接入与连接指标（`com.jmqx.transport`）。
- `jmqx-admin`: 管理后台模块（后端 SpringBoot + 前端 React）。
- `jmqx-app`: 启动装配模块（`com.jmqx.JmqxApplication` + `jmqx.properties`）。

## 当前已填充能力

- Maven 多模块结构。
- 可启动的 MQTT TCP 服务端（默认 `0.0.0.0:1883`）。
- 支持消息流程：
  - CONNECT + CONNACK
  - SUBSCRIBE + SUBACK（订阅后回放 retained）
  - PUBLISH（支持 QoS 0/1 基本流，QoS1 返回 PUBACK）
  - PINGREQ + PINGRESP
  - DISCONNECT / 连接断开时会话清理
- 支持 retained message 的保存、删除（空 payload）与订阅回放。
- 支持共享订阅（`$share/{group}/{topicFilter}`），同组内消息按轮询投递到一个客户端。
- 提供认证插件化能力（`allow_all/http/file/redis/db`）。
- 支持遗嘱消息（Will）：客户端非正常断开时自动发布遗嘱。
- 支持连接/断开公共事件 Topic：
  - `$SYS/jmqx/events/client/connected`
  - `$SYS/jmqx/events/client/disconnected`

## 启动方式

1. 安装 JDK 17。
2. 安装 Maven 3.9+。
3. 在仓库根目录执行：

```bash
mvn -DskipTests compile
mvn -pl jmqx-app -am exec:java
```

## 配置

默认配置文件：`jmqx-app/src/main/resources/jmqx.properties`

配置优先级：`JVM 参数 > jmqx.properties > 代码默认值`。

## 管理后台

当前采用“控制面独立 + 数据面轻量管理 API”的模式：

- `jmqx-app` 提供轻量节点管理接口（默认 `http://127.0.0.1:28083/api/admin`）
- `jmqx-admin` 独立启动，聚合一个或多个 `jmqx` 节点并提供页面

### 启动 jmqx 节点（数据面）

```bash
mvn -pl jmqx-app -am exec:java
```

节点 API 相关配置（`jmqx-app/src/main/resources/jmqx.properties`）：

```properties
jmqx.nodeAdmin.enabled=true
jmqx.nodeAdmin.host=0.0.0.0
jmqx.nodeAdmin.port=28083
```

### 启动 admin（控制面）

```bash
mvn -pl jmqx-admin -am exec:java
```

独立 admin 配置（`jmqx-admin/src/main/resources/application.properties`）：

```properties
server.port=18083
jmqx.admin.nodes=local=http://127.0.0.1:28083/api/admin
jmqx.admin.nodeTimeoutMs=2000
jmqx.admin.frontend.integrated=true
jmqx.admin.frontend.buildOnStart=false
```

页面地址：`http://127.0.0.1:18083`
后端接口：`http://127.0.0.1:18083/api/admin`

`/api/admin/config` 支持在线更新并热加载 Auth/ACL 配置（无需重启 broker）。
支持字段包括：`authType/authChain/authCacheMillis`、Auth 的 `http/file/redis/db` 参数，以及 ACL 的 `type/cache/defaultAllow/http/file/redis` 参数。

开发前端（可选）：

```bash
cd jmqx-admin/frontend
npm install
npm run dev
```

前端默认请求 `http://127.0.0.1:18083/api/admin`，也可以通过环境变量覆盖：

```bash
VITE_ADMIN_API_BASE=http://127.0.0.1:18083/api/admin
```

## 当前定位

当前版本专注单机稳定性与性能，不包含集群复制逻辑。

可通过 JVM 参数覆盖默认监听地址与端口：

```bash
-Djmqx.broker.host=0.0.0.0
-Djmqx.broker.port=1883
-Djmqx.broker.mqtts.enabled=false
-Djmqx.broker.mqtts.host=0.0.0.0
-Djmqx.broker.mqtts.port=8883
-Djmqx.broker.bossThreads=1
-Djmqx.broker.workerThreads=0
-Djmqx.broker.readerIdleSeconds=120
-Djmqx.broker.websocket.enabled=true
-Djmqx.broker.websocket.host=0.0.0.0
-Djmqx.broker.websocket.port=8083
-Djmqx.broker.websocket.path=/mqtt
-Djmqx.broker.wss.enabled=false
-Djmqx.broker.wss.host=0.0.0.0
-Djmqx.broker.wss.port=8084
-Djmqx.broker.wss.path=/mqtt
-Djmqx.broker.tls.certChainFile=/path/to/server.crt
-Djmqx.broker.tls.privateKeyFile=/path/to/server.key
-Djmqx.broker.tls.privateKeyPassword=
```

`jmqx.broker.readerIdleSeconds` 为读空闲超时秒数，设为 `0` 可关闭空闲连接检测。
`jmqx.broker.websocket.enabled=false` 可关闭 websocket 接入。
`jmqx.broker.mqtts.enabled=true` 或 `jmqx.broker.wss.enabled=true` 后，需要同时配置 TLS 证书与私钥路径。

Retained 内存保护配置（默认启用）：

```bash
-Djmqx.retained.rocksdb.path=data/retained-rocksdb
-Djmqx.retained.maxEntries=100000
-Djmqx.retained.maxBytes=268435456
-Djmqx.retained.maxPayloadBytes=1048576
-Djmqx.retained.overflowStrategy=evict_lru
```

`jmqx.retained.overflowStrategy` 支持：
- `evict_lru`：超限时按 LRU 淘汰旧 retained（默认）
- `reject_new`：超限时拒绝新 retained 写入

Shared subscription manager:

```bash
-Djmqx.shared.maxSubscribersPerGroup=1000
-Djmqx.shared.slowConsumerStrikeThreshold=3
```

WebSocket MQTT 接入地址示例：

```text
ws://127.0.0.1:8083/mqtt
```

MQTTS / WSS 接入地址示例：

```text
mqtts://127.0.0.1:8883
wss://127.0.0.1:8084/mqtt
```

## 用户密码认证插件化

Broker 已支持认证插件化，内置 5 种类型：

- `allow_all`：全部放行（默认）
- `http`：通过 HTTP 请求外部认证服务
- `file`：读取本地账号文件
- `redis`：通过 Redis `GET` 查询用户密码
- `db`：通过数据库 SQL 查询用户密码

公共参数：

```bash
-Djmqx.auth.type=allow_all|http|file|redis|db
-Djmqx.auth.chain=file,redis,http
-Djmqx.auth.cacheMillis=60000
```

`jmqx.auth.cacheMillis` 为认证结果本地缓存毫秒数，默认 `60000`；设为 `0` 可关闭缓存。
`jmqx.auth.chain` 为链式鉴权顺序，按顺序执行；当前插件返回“未命中(not found)”时会继续下一个插件，全部未命中则拒绝。

### HTTP Auth

```bash
-Djmqx.auth.type=http
-Djmqx.auth.http.url=http://127.0.0.1:8080/auth/check
-Djmqx.auth.http.timeoutMs=2000
```

请求体示例：

```json
{"clientId":"c1","username":"alice","password":"alice123"}
```

响应体支持：

- `{"allow": true}` / `true` / `allow`
- `{"notFound": true}` / `not_found` / `notfound`（表示未命中，链式场景会继续后续插件）
- 其他返回都视为认证失败

### File Auth

```bash
-Djmqx.auth.type=file
-Djmqx.auth.file.path=auth-users.txt
```

文件格式（每行一条）：

```text
alice:alice123
admin:admin123
```

### Redis Auth

```bash
-Djmqx.auth.type=redis
-Djmqx.auth.redis.host=127.0.0.1
-Djmqx.auth.redis.port=6379
-Djmqx.auth.redis.password=
-Djmqx.auth.redis.db=0
-Djmqx.auth.redis.keyPrefix=jmqx:auth
-Djmqx.auth.redis.timeoutMs=2000
```

键格式：`{prefix}:{username}`，值为明文密码。示例：

```text
jmqx:auth:alice = alice123
jmqx:auth:admin = admin123
```

### DB Auth

```bash
-Djmqx.auth.type=db
-Djmqx.auth.db.driver=com.mysql.cj.jdbc.Driver
-Djmqx.auth.db.url=jdbc:mysql://127.0.0.1:3306/jmqx
-Djmqx.auth.db.user=root
-Djmqx.auth.db.password=123456
-Djmqx.auth.db.query=SELECT password FROM mqtt_user WHERE username = ?
```

默认读取 SQL 第一列作为用户密码，并与客户端密码做等值比较。

## 消息桥接（Kafka / RocketMQ / MySQL）

Broker 在处理到 `PUBLISH` 后，除了本地路由，还可以把消息桥接转发到外部系统。

- 支持类型：`kafka`、`rocketmq`、`mysql`
- 支持多目标并行：`jmqx.bridge.types=kafka,rocketmq,mysql`
- 支持异步投递：基于 Disruptor RingBuffer（无锁）+ worker 线程（队列满时丢弃并记录日志）

公共参数：

```bash
-Djmqx.bridge.enabled=true
-Djmqx.bridge.types=kafka,rocketmq,mysql
-Djmqx.bridge.async=true
-Djmqx.bridge.async.queueCapacity=10000
-Djmqx.bridge.async.workerCount=1
```

Kafka 参数：

```bash
-Djmqx.bridge.kafka.bootstrapServers=127.0.0.1:9092
-Djmqx.bridge.kafka.topic=jmqx-messages
-Djmqx.bridge.kafka.acks=1
-Djmqx.bridge.kafka.clientId=jmqx-bridge
-Djmqx.bridge.kafka.compressionType=none
```

RocketMQ 参数：

```bash
-Djmqx.bridge.rocketmq.nameServer=127.0.0.1:9876
-Djmqx.bridge.rocketmq.producerGroup=jmqx-bridge-group
-Djmqx.bridge.rocketmq.topic=JMQX_MESSAGES
-Djmqx.bridge.rocketmq.syncSend=false
-Djmqx.bridge.rocketmq.timeoutMs=3000
```

MySQL 参数：

```bash
-Djmqx.bridge.mysql.driver=com.mysql.cj.jdbc.Driver
-Djmqx.bridge.mysql.url=jdbc:mysql://127.0.0.1:3306/jmqx
-Djmqx.bridge.mysql.user=root
-Djmqx.bridge.mysql.password=123456
-Djmqx.bridge.mysql.table=jmqx_bridge_message
-Djmqx.bridge.mysql.autoCreateTable=true
```

## ACL 插件化鉴权

Broker 已支持 ACL 插件化，内置 4 种类型：

- `allow_all`：全部放行（默认）
- `http`：通过 HTTP 请求外部鉴权服务
- `redis`：通过 Redis `GET` 查询规则
- `file`：读取本地规则文件

公共参数：

```bash
-Djmqx.acl.type=allow_all|http|redis|file
-Djmqx.acl.defaultAllow=false
-Djmqx.acl.cacheMillis=60000
```

`jmqx.acl.cacheMillis` 为 ACL 本地缓存毫秒数，默认 `60000`；设为 `0` 可关闭缓存。

### HTTP ACL

```bash
-Djmqx.acl.type=http
-Djmqx.acl.http.url=http://127.0.0.1:8080/acl/check
-Djmqx.acl.http.timeoutMs=2000
```

请求体示例：

```json
{"clientId":"c1","username":"u1","topic":"test/a","action":"publish"}
```

响应体支持：

- `{"allow": true}` / `true` / `allow`
- `{"allow": false}` / `false` / `deny`

### Redis ACL

```bash
-Djmqx.acl.type=redis
-Djmqx.acl.redis.host=127.0.0.1
-Djmqx.acl.redis.port=6379
-Djmqx.acl.redis.password=
-Djmqx.acl.redis.db=0
-Djmqx.acl.redis.keyPrefix=jmqx:acl
-Djmqx.acl.redis.timeoutMs=2000
```

键格式：

`{prefix}:{action}:{username}:{topic}`

示例：

```text
jmqx:acl:publish:alice:sensor/temp = allow
jmqx:acl:publish:alice:* = deny
jmqx:acl:subscribe:alice:sensor/# = allow
jmqx:acl:subscribe:*:* = deny
```

值支持：`allow/deny/true/false/1/0`。

说明：Redis 模式是按键查询（`GET`），更适合精确 topic 或 `*` 级别通配；复杂 ACL 规则建议使用 `file` 或 `http` 模式。

### File ACL

```bash
-Djmqx.acl.type=file
-Djmqx.acl.file.path=acl-rules.txt
```

规则文件格式（每行一条）：

```text
allow publish alice sensor/#
deny  subscribe bob   secure/#
allow * * #
```
