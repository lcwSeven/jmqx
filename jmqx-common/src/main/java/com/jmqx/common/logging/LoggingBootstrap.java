package com.jmqx.common.logging;

import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 统一初始化日志系统，把 JUL 日志桥接到 SLF4J。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public final class LoggingBootstrap {

    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final String SOFA_MIDDLEWARE_LOG_DISABLE = "sofa.middleware.log.disable";
    private static final String SOFA_LOGBACK_MIDDLEWARE_LOG_DISABLE = "logback.middleware.log.disable";

    private LoggingBootstrap() {
    }

    public static void initialize() {
        if (!INITIALIZED.compareAndSet(false, true)) {
            return;
        }
        setDefaultSystemProperty(SOFA_MIDDLEWARE_LOG_DISABLE, "true");
        setDefaultSystemProperty(SOFA_LOGBACK_MIDDLEWARE_LOG_DISABLE, "true");
        java.util.logging.LogManager.getLogManager().reset();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    private static void setDefaultSystemProperty(String key, String value) {
        if (System.getProperty(key) == null) {
            System.setProperty(key, value);
        }
    }
}
