package com.jmqtt.admin;

import com.jmqtt.transport.ConnectionMetrics;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminBackendState {
    private final ConnectionMetrics connectionMetrics;
    private final RuntimeConfigService runtimeConfigService;

    public AdminBackendState(ConnectionMetrics connectionMetrics, RuntimeConfigService runtimeConfigService) {
        this.connectionMetrics = connectionMetrics;
        this.runtimeConfigService = runtimeConfigService;
    }

    public ConnectionMetrics getConnectionMetrics() {
        return connectionMetrics;
    }

    public RuntimeConfigService getRuntimeConfigService() {
        return runtimeConfigService;
    }
}
