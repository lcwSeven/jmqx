package com.jmqx.admin;

import com.jmqx.router.SubscriptionRegistry;
import com.jmqx.session.SessionRegistry;
import com.jmqx.transport.ConnectionMetrics;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminBackendState {
    private final ConnectionMetrics connectionMetrics;
    private final RuntimeConfigService runtimeConfigService;
    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;

    public AdminBackendState(
        ConnectionMetrics connectionMetrics,
        RuntimeConfigService runtimeConfigService,
        SessionRegistry sessionRegistry,
        SubscriptionRegistry subscriptionRegistry
    ) {
        this.connectionMetrics = connectionMetrics;
        this.runtimeConfigService = runtimeConfigService;
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
    }

    public ConnectionMetrics getConnectionMetrics() {
        return connectionMetrics;
    }

    public RuntimeConfigService getRuntimeConfigService() {
        return runtimeConfigService;
    }

    public SessionRegistry getSessionRegistry() {
        return sessionRegistry;
    }

    public SubscriptionRegistry getSubscriptionRegistry() {
        return subscriptionRegistry;
    }
}
