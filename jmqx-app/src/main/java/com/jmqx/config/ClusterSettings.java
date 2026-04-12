package com.jmqx.config;

import java.util.Map;

/**
 * 集群相关配置集合。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public record ClusterSettings(
        String coreBindHost,
        int coreBindPort,
        int clusterRequestTimeoutMs,
        int clusterReplayMaxEvents,
        int clusterReconnectBackoffMs,
        int clusterAckBatchSize,
        int clusterAckFlushIntervalMs,
        int clusterReplicantMaxInFlightEvents,
        int clusterReplicantPushBatchSize,
        int clusterNodeDownCleanupDelayMs,
        String clusterMessageBindHost,
        int clusterMessageBindPort,
        Map<String, String> clusterNodeEndpoints,
        String raftGroupId,
        String raftServerId,
        String raftInitialConf,
        String raftDataPath,
        int raftElectionTimeoutMs,
        int raftSnapshotIntervalSecs
) {
}
