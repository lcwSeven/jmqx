package com.jmqx.common.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 项目统一日志入口。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public final class Loggers {

    private Loggers() {
    }

    public static Logger getLogger(Class<?> type) {
        return LoggerFactory.getLogger(type);
    }
}
