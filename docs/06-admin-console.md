# 管理页面（Admin Console）

## 1. 目标与定位

JMQX 管理页面是 `jmqx-app` 的内嵌控制台，目标是提供：

- Broker 运行态可观测（连接、流量、客户端状态）
- 安全策略可配置（ACL / AUTH）
- 部分配置动态生效（降低重启频率）

该管理页面默认与 Broker 同进程启动，适合单机与中小规模集群运维场景。

## 2. 启动与访问

默认配置（`jmqx.yaml`）：

- `jmqx.admin.panel.enabled=true`
- `jmqx.admin.panel.host=0.0.0.0`
- `jmqx.admin.panel.port=18081`
- `jmqx.admin.panel.basePath=/admin`

访问地址：

- `http://<host>:18081/admin/`

## 3. 架构说明

管理页面采用“静态前端 + 内嵌 HTTP API + Broker 运行时对象直连”的模式：

1. 前端静态资源由 `AdminPanelServer` 直接托管
2. 页面通过 `/api/v1/*` 访问管理 API
3. API 读取 Broker 运行时对象（会话、订阅、连接指标）
4. 页面通过 MQTT over WebSocket 订阅 `$SYS/dashboard/...` 获取实时更新

关键组件：

- `jmqx-app/src/main/java/com/jmqx/admin/embedded/AdminPanelServer.java`
- `jmqx-app/src/main/java/com/jmqx/admin/embedded/EmbeddedAdminStateStore.java`
- `jmqx-core/src/main/java/com/jmqx/broker/MqttBrokerMessageHandler.java`

集群统一概览前提：

- 需要在各节点开启 `jmqx.admin.enabled=true`
- 需要让各节点的 `jmqx.admin.url` 指向同一个管理端，例如 `http://10.0.0.10:18081`
- 需要让同一集群内节点使用相同的 `jmqx.admin.clusterId`
- 管理端会通过 `/api/v1/internal/nodes/{nodeId}/metrics` 聚合各节点指标，再由 `/api/v1/cluster/overview` 对外返回

## 4. 功能清单

### 4.1 集群概览

- 展示连接总数、入/出流量、节点列表、节点角色
- 支持接口拉取 + MQTT 实时推送双路径更新

### 4.2 客户端列表/详情

- 列表字段：客户端 ID、节点、IP、Keepalive、连接方式、用户名、上线时间
- 支持按 `clientId`、`userName` 搜索
- 支持查看客户端订阅主题详情

### 4.3 安全配置

- ACL 配置页
- 连接鉴权配置页（向导式创建）
- 插件链、缓存时间、启用状态可配置

### 4.4 集群配置

- 支持配置展示与保存
- 当前已动态生效项：`sharedSubscriptionMaxMembersPerGroup`

## 5. 接口总览（/api/v1）

| 接口 | 方法 | 用途 |
|---|---|---|
| `/clusters` | GET | 查询集群列表 |
| `/cluster/overview` | GET | 查询概览 |
| `/clients` | GET | 查询客户端列表 |
| `/clients/{clientId}` | GET | 查询客户端详情 |
| `/security/config` | GET/PUT | 查询/更新 ACL 与 AUTH 配置 |
| `/cluster/config` | GET/PUT | 查询/更新集群配置 |
| `/cluster/full-config` | GET | 查询完整配置 |

## 6. 数据打通与生效范围

### 6.1 读链路（已打通）

- `/cluster/overview` 读取管理端已聚合的节点指标；若远端节点尚未上报，则至少包含当前节点快照
- `/clients*` 读取 `SessionRegistry` 与 `SubscriptionRegistry`
- 页面实时流订阅：
  - `$SYS/dashboard/{clusterId}/cluster/overview`
  - `$SYS/dashboard/{clusterId}/client/connected`
  - `$SYS/dashboard/{clusterId}/client/disconnected`

### 6.1.1 集群指标上报链路

- Broker 节点通过 `HttpAdminReporter` 周期性上报节点指标
- 上报地址由 `jmqx.admin.url` 决定
- 建议将所有节点统一上报到承载管理页的节点 `http://<admin-host>:18081`

### 6.2 写链路（已动态生效）

- `PUT /security/config`
  - 动态重载 `ReloadableAuthProvider`
  - 动态重载 `ReloadableAclAuthorizer`
- `PUT /cluster/config`
  - 动态更新 `SharedSubscriptionManager` 的订阅上限参数

### 6.3 写链路（当前仅管理态保存）

- `coreNodes`
- `replicantNodes`
- `coreAcceptClientConnections`

以上字段已可在页面编辑保存，但尚未接入运行时热更新。

## 7. 运维建议

1. 生产环境建议将管理页面监听在内网地址，并结合网关做访问控制。
2. 对 `security/config` 变更建立审计（谁在何时修改了插件链与缓存时间）。
3. 将 `$SYS/dashboard` 主题纳入监控采集，便于定位页面与 Broker 数据差异。
4. 对“动态生效项”与“仅保存项”做发布前确认，避免误判配置已生效。

## 8. 常见排障

### 8.1 页面打不开

检查：

- `jmqx.admin.panel.enabled=true`
- `jmqx.admin.panel.port` 未被占用
- 访问路径是否为 `/admin/`（注意末尾斜杠）

### 8.2 页面实时数据不更新

检查：

- Broker WS 端口与路径：`jmqx.broker.websocket.port`、`jmqx.broker.websocket.path`
- 页面是否可连 `ws://<broker-host>:<ws-port>/mqtt`
- `$SYS/dashboard/{clusterId}/...` 主题是否有数据

### 8.3 配置保存后未生效

检查：

- 是否属于“当前仅管理态保存”字段
- AUTH/ACL 插件链配置是否合法（插件名、地址、凭据）
- 日志中是否出现插件初始化异常
