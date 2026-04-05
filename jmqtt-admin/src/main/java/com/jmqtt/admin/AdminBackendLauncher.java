package com.jmqtt.admin;

import com.jmqtt.router.SubscriptionRegistry;
import com.jmqtt.session.SessionRegistry;
import com.jmqtt.transport.ConnectionMetrics;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminBackendLauncher {
    private final AdminProperties adminProperties;
    private final ConnectionMetrics connectionMetrics;
    private final RuntimeConfigService runtimeConfigService;
    private final SessionRegistry sessionRegistry;
    private final SubscriptionRegistry subscriptionRegistry;
    private ConfigurableApplicationContext context;

    public AdminBackendLauncher(
        AdminProperties adminProperties,
        ConnectionMetrics connectionMetrics,
        RuntimeConfigService runtimeConfigService,
        SessionRegistry sessionRegistry,
        SubscriptionRegistry subscriptionRegistry
    ) {
        this.adminProperties = adminProperties;
        this.connectionMetrics = connectionMetrics;
        this.runtimeConfigService = runtimeConfigService;
        this.sessionRegistry = sessionRegistry;
        this.subscriptionRegistry = subscriptionRegistry;
    }

    public synchronized void start() {
        if (context != null) {
            return;
        }
        Map<String, Object> props = new HashMap<>();
        props.put("server.port", adminProperties.getPort());
        props.put("server.address", adminProperties.getHost());
        props.put("spring.main.banner-mode", "off");
        props.put("logging.level.root", "INFO");

        context = new SpringApplicationBuilder(AdminBackendApplication.class)
            .web(WebApplicationType.SERVLET)
            .properties(props)
            .initializers(appContext ->
                appContext.getBeanFactory().registerSingleton(
                    "adminBackendState",
                    new AdminBackendState(connectionMetrics, runtimeConfigService, sessionRegistry, subscriptionRegistry)
                )
            )
            .run();
    }

    public synchronized void stop() {
        if (context != null) {
            context.close();
            context = null;
        }
    }
}
