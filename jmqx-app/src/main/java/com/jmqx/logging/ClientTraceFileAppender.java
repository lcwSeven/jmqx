package com.jmqx.logging;

import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.jmqx.common.logging.ClientLogContext;
import com.jmqx.common.logging.ClientTraceManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将指定 clientId 的日志分流到独立文件。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public class ClientTraceFileAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private static final Pattern CLIENT_ID_PATTERN = Pattern.compile("clientId=([^,\\s]+)");
    private static final long CLEANUP_INTERVAL_MILLIS = 60_000L;

    private final ClientTraceManager traceManager = ClientTraceManager.getInstance();
    private final Map<String, FileAppender<ILoggingEvent>> delegates = new ConcurrentHashMap<>();
    private volatile String pattern = "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger{36} [clientId=%X{clientId}] - %msg%n";
    private volatile long lastCleanupAt;

    public void setPattern(String pattern) {
        if (pattern == null || pattern.isBlank()) {
            return;
        }
        this.pattern = pattern;
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        if (eventObject == null) {
            return;
        }
        long now = eventObject.getTimeStamp();
        String clientId = resolveClientId(eventObject);
        if (clientId != null && !clientId.isBlank()) {
            List<ClientTraceManager.ClientTraceTask> tasks = traceManager.findActiveTasks(clientId, now);
            for (ClientTraceManager.ClientTraceTask task : tasks) {
                FileAppender<ILoggingEvent> delegate = delegates.computeIfAbsent(task.id(), ignored -> createDelegate(task));
                if (delegate != null) {
                    delegate.doAppend(eventObject);
                }
            }
        }
        if (now - lastCleanupAt >= CLEANUP_INTERVAL_MILLIS) {
            cleanupExpiredDelegates(now);
        }
    }

    @Override
    public void stop() {
        delegates.values().forEach(FileAppender::stop);
        delegates.clear();
        super.stop();
    }

    private FileAppender<ILoggingEvent> createDelegate(ClientTraceManager.ClientTraceTask task) {
        try {
            Path path = Path.of(task.filePath());
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            PatternLayoutEncoder encoder = new PatternLayoutEncoder();
            encoder.setContext(getContext());
            encoder.setPattern(pattern);
            encoder.start();

            FileAppender<ILoggingEvent> delegate = new FileAppender<>();
            delegate.setContext(getContext());
            delegate.setFile(path.toString());
            delegate.setAppend(true);
            delegate.setImmediateFlush(true);
            delegate.setEncoder(encoder);
            delegate.setName("CLIENT_TRACE_" + task.id());
            delegate.start();
            return delegate;
        } catch (Exception exception) {
            addError("create client trace appender failed for task=" + task.id(), exception);
            return null;
        }
    }

    private void cleanupExpiredDelegates(long now) {
        lastCleanupAt = now;
        for (Map.Entry<String, FileAppender<ILoggingEvent>> entry : delegates.entrySet()) {
            ClientTraceManager.ClientTraceTask task = traceManager.get(entry.getKey());
            if (task != null && task.endAt() >= now) {
                continue;
            }
            FileAppender<ILoggingEvent> removed = delegates.remove(entry.getKey());
            if (removed != null) {
                removed.stop();
            }
        }
    }

    private static String resolveClientId(ILoggingEvent event) {
        String fromMdc = event.getMDCPropertyMap() == null ? null : event.getMDCPropertyMap().get(ClientLogContext.CLIENT_ID_KEY);
        if (fromMdc != null && !fromMdc.isBlank()) {
            return fromMdc.trim();
        }
        String formattedMessage = event.getFormattedMessage();
        if (formattedMessage == null || formattedMessage.isBlank()) {
            return null;
        }
        Matcher matcher = CLIENT_ID_PATTERN.matcher(formattedMessage);
        if (!matcher.find()) {
            return null;
        }
        return matcher.group(1);
    }
}
