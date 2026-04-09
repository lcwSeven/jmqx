# jmqx Single-Node 300k Connections Milestone

This checklist is for the first capacity milestone: stable `300,000` online connections on one node.

## 1. Scope

- Protocol: MQTT over TCP only.
- Target traffic profile: low message throughput, heartbeat-dominant, long-lived connections.
- Recommended baseline:
  - `QoS0`
  - retained disabled
  - bridge disabled
  - MQTTS/WSS disabled
  - external auth/acl disabled or strongly cached

## 2. jmqx Runtime Configuration

Use `jmqx-app/src/main/resources/jmqx-300k.properties` as baseline and keep the effective runtime aligned.

Key options:

- `jmqx.broker.mqtts.enabled=false`
- `jmqx.broker.websocket.enabled=false`
- `jmqx.broker.wss.enabled=false`
- `jmqx.retained.enabled=false`
- `jmqx.bridge.enabled=false`
- `jmqx.auth.type=allow_all`
- `jmqx.acl.type=allow_all`

## 3. JVM Configuration (Reference)

Example startup args for Java 17:

```bash
-Xms8g
-Xmx8g
-XX:+UseZGC
-XX:MaxDirectMemorySize=8g
-Dio.netty.allocator.type=pooled
-Dio.netty.leakDetection.level=disabled
```

Notes:

- Keep `Xms == Xmx` to avoid runtime heap resizing.
- Direct memory must be planned together with connection count.
- Disable expensive debug/leak tools in benchmark runs.

## 4. Linux Baseline (Reference)

Apply on test hosts only:

```bash
ulimit -n 2000000
sysctl -w net.core.somaxconn=65535
sysctl -w net.core.netdev_max_backlog=250000
sysctl -w net.ipv4.tcp_max_syn_backlog=262144
sysctl -w net.ipv4.ip_local_port_range="10000 65535"
sysctl -w fs.file-max=4000000
```

Also ensure:

- Sufficient RAM and CPU cores.
- NIC queue and IRQ affinity tuned.
- No extra background workloads on benchmark host.

## 5. Test Stages

Stage A:

- Connect `100k`, hold for `1h`.
- Acceptance: no OOM, no mass reconnect waves, flat memory trend.

Stage B:

- Connect `200k`, hold for `2h`.
- Acceptance: stable connection count, CPU not saturating for long periods.

Stage C (milestone):

- Connect `300k`, hold for `12h`.
- Acceptance:
  - online connections >= 99.9% target
  - no long GC pause spikes
  - reconnect storms absent
  - process memory trend stable (no unbounded growth)

## 6. Metrics to Record

- Online connection count over time.
- Process RSS / heap / direct memory.
- GC pause (avg/p99/max).
- New connect rate and disconnect rate.
- CPU utilization and system load.
- Error logs sampled by type.

## 7. Common Failure Signals

- Fast RSS growth: often direct memory pressure or buffer retention.
- Spike reconnect loops: kernel queue limits or keepalive storms.
- Event-loop latency spikes: logging/bridge/auth in hot path.
- GC pause bursts: object churn in subscribe/publish route path.
