# jmqtt

`jmqtt` 是一个基于 Java + Netty 的 MQTT Broker 单模块工程。

## 项目结构分析

- `src/main/java/com/jmqtt/common`: 通用配置与工具类（如 `BrokerProperties`、Topic 匹配器）。
- `src/main/java/com/jmqtt/protocol`: 协议扩展点（当前提供 `ClientAuthenticator` 认证接口）。
- `src/main/java/com/jmqtt/session`: 客户端会话管理（`SessionRegistry` + 内存实现）。
- `src/main/java/com/jmqtt/router`: 订阅路由管理（topic filter 到 clientId 的匹配）。
- `src/main/java/com/jmqtt/store`: retained message 存储（内存实现）。
- `src/main/java/com/jmqtt/broker`: MQTT 协议处理核心（CONNECT / SUBSCRIBE / PUBLISH / PINGREQ / DISCONNECT）。
- `src/main/java/com/jmqtt/transport`: Netty TCP Server 与 MQTT 编解码、消息分发。
- `src/main/java/com/jmqtt/JmqttApplication.java`: 程序启动入口。

## 当前已填充能力

- 单模块 Maven 结构。
- 可启动的 MQTT TCP 服务端（默认 `0.0.0.0:1883`）。
- 支持消息流程：
  - CONNECT + CONNACK
  - SUBSCRIBE + SUBACK（订阅后回放 retained）
  - PUBLISH（支持 QoS 0/1 基本流，QoS1 返回 PUBACK）
  - PINGREQ + PINGRESP
  - DISCONNECT / 连接断开时会话清理
- 支持 retained message 的保存、删除（空 payload）与订阅回放。
- 提供认证插件化能力（`allow_all/http/file/redis/db`）。

## 启动方式

1. 安装 JDK 17。
2. 安装 Maven 3.9+。
3. 在仓库根目录执行：

```bash
mvn -DskipTests compile
mvn exec:java
```

## 配置

默认配置文件：`src/main/resources/jmqtt.properties`

配置优先级：`JVM 参数 > jmqtt.properties > 代码默认值`。

可通过 JVM 参数覆盖默认监听地址与端口：

```bash
-Djmqtt.broker.host=0.0.0.0
-Djmqtt.broker.port=1883
-Djmqtt.broker.bossThreads=1
-Djmqtt.broker.workerThreads=0
-Djmqtt.broker.readerIdleSeconds=120
```

`jmqtt.broker.readerIdleSeconds` 为读空闲超时秒数，设为 `0` 可关闭空闲连接检测。

## 用户密码认证插件化

Broker 已支持认证插件化，内置 5 种类型：

- `allow_all`：全部放行（默认）
- `http`：通过 HTTP 请求外部认证服务
- `file`：读取本地账号文件
- `redis`：通过 Redis `GET` 查询用户密码
- `db`：通过数据库 SQL 查询用户密码

公共参数：

```bash
-Djmqtt.auth.type=allow_all|http|file|redis|db
-Djmqtt.auth.cacheSeconds=60
```

`jmqtt.auth.cacheSeconds` 为认证结果本地缓存秒数，默认 `60`；设为 `0` 可关闭缓存。

### HTTP Auth

```bash
-Djmqtt.auth.type=http
-Djmqtt.auth.http.url=http://127.0.0.1:8080/auth/check
-Djmqtt.auth.http.timeoutMs=2000
```

请求体示例：

```json
{"username":"alice","password":"alice123"}
```

响应体支持：

- `{"allow": true}` / `true` / `allow`
- 其他返回都视为认证失败

### File Auth

```bash
-Djmqtt.auth.type=file
-Djmqtt.auth.file.path=auth-users.txt
```

文件格式（每行一条）：

```text
alice:alice123
admin:admin123
```

### Redis Auth

```bash
-Djmqtt.auth.type=redis
-Djmqtt.auth.redis.host=127.0.0.1
-Djmqtt.auth.redis.port=6379
-Djmqtt.auth.redis.password=
-Djmqtt.auth.redis.db=0
-Djmqtt.auth.redis.keyPrefix=jmqtt:auth
-Djmqtt.auth.redis.timeoutMs=2000
```

键格式：`{prefix}:{username}`，值为明文密码。示例：

```text
jmqtt:auth:alice = alice123
jmqtt:auth:admin = admin123
```

### DB Auth

```bash
-Djmqtt.auth.type=db
-Djmqtt.auth.db.driver=com.mysql.cj.jdbc.Driver
-Djmqtt.auth.db.url=jdbc:mysql://127.0.0.1:3306/jmqtt
-Djmqtt.auth.db.user=root
-Djmqtt.auth.db.password=123456
-Djmqtt.auth.db.query=SELECT password FROM mqtt_user WHERE username = ?
```

默认读取 SQL 第一列作为用户密码，并与客户端密码做等值比较。

## ACL 插件化鉴权

Broker 已支持 ACL 插件化，内置 4 种类型：

- `allow_all`：全部放行（默认）
- `http`：通过 HTTP 请求外部鉴权服务
- `redis`：通过 Redis `GET` 查询规则
- `file`：读取本地规则文件

公共参数：

```bash
-Djmqtt.acl.type=allow_all|http|redis|file
-Djmqtt.acl.defaultAllow=false
-Djmqtt.acl.cacheSeconds=60
```

`jmqtt.acl.cacheSeconds` 为 ACL 本地缓存秒数，默认 `60`；设为 `0` 可关闭缓存。

### HTTP ACL

```bash
-Djmqtt.acl.type=http
-Djmqtt.acl.http.url=http://127.0.0.1:8080/acl/check
-Djmqtt.acl.http.timeoutMs=2000
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
-Djmqtt.acl.type=redis
-Djmqtt.acl.redis.host=127.0.0.1
-Djmqtt.acl.redis.port=6379
-Djmqtt.acl.redis.password=
-Djmqtt.acl.redis.db=0
-Djmqtt.acl.redis.keyPrefix=jmqtt:acl
-Djmqtt.acl.redis.timeoutMs=2000
```

键格式：

`{prefix}:{action}:{username}:{topic}`

示例：

```text
jmqtt:acl:publish:alice:sensor/temp = allow
jmqtt:acl:publish:alice:* = deny
jmqtt:acl:subscribe:alice:sensor/# = allow
jmqtt:acl:subscribe:*:* = deny
```

值支持：`allow/deny/true/false/1/0`。

说明：Redis 模式是按键查询（`GET`），更适合精确 topic 或 `*` 级别通配；复杂 ACL 规则建议使用 `file` 或 `http` 模式。

### File ACL

```bash
-Djmqtt.acl.type=file
-Djmqtt.acl.file.path=acl-rules.txt
```

规则文件格式（每行一条）：

```text
allow publish alice sensor/#
deny  subscribe bob   secure/#
allow * * #
```
