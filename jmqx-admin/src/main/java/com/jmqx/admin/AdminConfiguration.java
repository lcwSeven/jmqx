package com.jmqx.admin;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
@Configuration
public class AdminConfiguration {
    @Bean
    public AdminProperties adminProperties(Environment environment) {
        AdminProperties properties = new AdminProperties();
        properties.setEnabled(getBoolean(environment, "jmqx.admin.enabled", properties.isEnabled()));
        properties.setHost(getString(environment, "jmqx.admin.host", properties.getHost()));
        properties.setPort(getInt(environment, "jmqx.admin.port", properties.getPort()));
        properties.setNodes(getString(environment, "jmqx.admin.nodes", properties.getNodes()));
        properties.setNodeTimeoutMs(getInt(environment, "jmqx.admin.nodeTimeoutMs", properties.getNodeTimeoutMs()));
        properties.setFrontendIntegrated(getBoolean(
            environment,
            "jmqx.admin.frontend.integrated",
            properties.isFrontendIntegrated()
        ));
        properties.setFrontendBuildOnStart(getBoolean(
            environment,
            "jmqx.admin.frontend.buildOnStart",
            properties.isFrontendBuildOnStart()
        ));
        properties.setFrontendBuildWorkDir(getString(
            environment,
            "jmqx.admin.frontend.build.workDir",
            properties.getFrontendBuildWorkDir()
        ));
        properties.setFrontendBuildCommand(getString(
            environment,
            "jmqx.admin.frontend.build.command",
            properties.getFrontendBuildCommand()
        ));
        properties.setFrontendDistDir(getString(
            environment,
            "jmqx.admin.frontend.distDir",
            properties.getFrontendDistDir()
        ));
        return properties;
    }

    private static String getString(Environment environment, String key, String defaultValue) {
        String value = environment.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    private static int getInt(Environment environment, String key, int defaultValue) {
        String value = environment.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static boolean getBoolean(Environment environment, String key, boolean defaultValue) {
        String value = environment.getProperty(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value) || "1".equals(value) || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value) || "no".equalsIgnoreCase(value)) {
            return false;
        }
        return defaultValue;
    }
}
