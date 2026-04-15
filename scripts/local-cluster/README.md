# Local Cluster Scripts

这套脚本用于在一台机器上快速拉起一套本地 JMQX 集群，默认拓扑是：

- `core-1`
- `core-2`
- `core-3`
- `rep-1`
- `rep-2`

## 默认端口

| 节点 | MQTT | WS | Core Metadata | Message | Raft | Admin Panel |
|---|---:|---:|---:|---:|---:|---:|
| `core-1` | `1883` | `8083` | `7800` | `7900` | `17800` | `18081` |
| `core-2` | `1884` | `8084` | `7801` | `7901` | `17801` | - |
| `core-3` | `1885` | `8085` | `7802` | `7902` | `17802` | - |
| `rep-1` | `1886` | `8086` | - | `7903` | - | - |
| `rep-2` | `1887` | `8087` | - | `7904` | - | - |

## 工作目录

每个节点都会使用独立目录，默认根路径：

`/tmp/jmqx-local`

例如：

- `/tmp/jmqx-local/core-1`
- `/tmp/jmqx-local/core-2`
- `/tmp/jmqx-local/core-3`
- `/tmp/jmqx-local/rep-1`
- `/tmp/jmqx-local/rep-2`

这样可以避免多个节点争用：

- `data/raft-metadata`
- `data/retained-rocksdb`
- `data/admin-state-rocksdb`

## 快速开始

先确认本机没有别的 JMQX 实例占用默认端口，然后执行：

```bash
./scripts/local-cluster/start-all.sh
```

如果本机 `mvn` 不在 `PATH`，脚本会自动尝试以下位置：

- 仓库根目录 `./mvnw`
- `/Users/liucaiwen/Documents/maven/apache-maven-3.9.9/bin/mvn`
- `/opt/homebrew/bin/mvn`
- `/usr/local/bin/mvn`

你也可以显式指定：

```bash
MAVEN_BIN=/absolute/path/to/mvn ./scripts/local-cluster/start-all.sh
```

查看状态：

```bash
./scripts/local-cluster/status.sh
```

停止全部节点：

```bash
./scripts/local-cluster/stop-all.sh
```

## 单节点启动

```bash
./scripts/local-cluster/start-core-1.sh
./scripts/local-cluster/start-core-2.sh
./scripts/local-cluster/start-core-3.sh
./scripts/local-cluster/start-rep-1.sh
./scripts/local-cluster/start-rep-2.sh
```

## 前台运行（便于调试）

如果你想单独观察某个节点的实时日志，可以直接前台跑：

```bash
./scripts/local-cluster/run-core-1.sh
./scripts/local-cluster/run-core-2.sh
./scripts/local-cluster/run-core-3.sh
./scripts/local-cluster/run-rep-1.sh
./scripts/local-cluster/run-rep-2.sh
```

## 管理端

- 地址：`http://127.0.0.1:18081/admin/`
- 账号：`admin`
- 密码：`public`

所有节点都会把管理数据上报到：

`http://127.0.0.1:18081`

并统一使用：

`jmqx.admin.clusterId=local-lab`

## 可覆盖环境变量

- `CLUSTER_ROOT`
- `MAVEN_BIN`
- `MAVEN_REPO_LOCAL`
- `JAVA_BIN`
- `JMQX_JAVA_OPTS`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `CLUSTER_ID`

示例：

```bash
CLUSTER_ROOT=/tmp/jmqx-lab MAVEN_BIN=/Users/liucaiwen/Documents/maven/apache-maven-3.9.9/bin/mvn ./scripts/local-cluster/start-all.sh
```
