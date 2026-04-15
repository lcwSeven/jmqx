package com.jmqx.broker.core;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/**
 * 连接鉴权与 ACL 鉴权链路指标。
 *
 * @author liucaiwen
 * @date 2026/4/15
 */
public class SecurityPipelineMetrics {
    private static final long SLOW_THRESHOLD_MS = 200L;
    private static final long SLOW_LOG_INTERVAL_MS = 5000L;

    private final Logger logger;
    private final LongAdder connectAuthSuccess = new LongAdder();
    private final LongAdder connectAuthFailure = new LongAdder();
    private final LongAdder connectAuthError = new LongAdder();
    private final LongAdder connectAuthSlow = new LongAdder();
    private final LongAdder connectAuthLatencyNanos = new LongAdder();
    private final AtomicLong connectAuthMaxNanos = new AtomicLong();
    private final AtomicLong connectAuthLastSlowLogAtMs = new AtomicLong(0L);

    private final LongAdder publishAclAllow = new LongAdder();
    private final LongAdder publishAclDeny = new LongAdder();
    private final LongAdder publishAclError = new LongAdder();
    private final LongAdder publishAclSlow = new LongAdder();
    private final LongAdder publishAclLatencyNanos = new LongAdder();
    private final AtomicLong publishAclMaxNanos = new AtomicLong();
    private final AtomicLong publishAclLastSlowLogAtMs = new AtomicLong(0L);

    public SecurityPipelineMetrics(Logger logger) {
        this.logger = logger == null ? Logger.getLogger(SecurityPipelineMetrics.class.getName()) : logger;
    }

    public void recordConnectAuthSuccess(long durationNanos, String clientId) {
        connectAuthSuccess.increment();
        recordConnectLatency(durationNanos, clientId, null);
    }

    public void recordConnectAuthFailure(long durationNanos, String clientId) {
        connectAuthFailure.increment();
        recordConnectLatency(durationNanos, clientId, "deny");
    }

    public void recordConnectAuthError(long durationNanos, String clientId, Throwable error) {
        connectAuthError.increment();
        recordConnectLatency(durationNanos, clientId, error == null ? "error" : error.getClass().getSimpleName());
    }

    public void recordPublishAclAllow(long durationNanos, String clientId, String topic) {
        publishAclAllow.increment();
        recordPublishAclLatency(durationNanos, clientId, topic, null);
    }

    public void recordPublishAclDeny(long durationNanos, String clientId, String topic) {
        publishAclDeny.increment();
        recordPublishAclLatency(durationNanos, clientId, topic, "deny");
    }

    public void recordPublishAclError(long durationNanos, String clientId, String topic, Throwable error) {
        publishAclError.increment();
        recordPublishAclLatency(durationNanos, clientId, topic, error == null ? "error" : error.getClass().getSimpleName());
    }

    public Snapshot snapshot() {
        long connectSuccessCount = connectAuthSuccess.sum();
        long connectFailureCount = connectAuthFailure.sum();
        long connectErrorCount = connectAuthError.sum();
        long connectTotalCount = connectSuccessCount + connectFailureCount + connectErrorCount;
        long connectTotalLatencyNanos = connectAuthLatencyNanos.sum();

        long publishAllowCount = publishAclAllow.sum();
        long publishDenyCount = publishAclDeny.sum();
        long publishErrorCount = publishAclError.sum();
        long publishTotalCount = publishAllowCount + publishDenyCount + publishErrorCount;
        long publishTotalLatencyNanos = publishAclLatencyNanos.sum();

        return new Snapshot(
            connectSuccessCount,
            connectFailureCount,
            connectErrorCount,
            connectAuthSlow.sum(),
            nanosToMillis(connectTotalCount == 0 ? 0L : connectTotalLatencyNanos / connectTotalCount),
            nanosToMillis(connectAuthMaxNanos.get()),
            publishAllowCount,
            publishDenyCount,
            publishErrorCount,
            publishAclSlow.sum(),
            nanosToMillis(publishTotalCount == 0 ? 0L : publishTotalLatencyNanos / publishTotalCount),
            nanosToMillis(publishAclMaxNanos.get())
        );
    }

    private void recordConnectLatency(long durationNanos, String clientId, String outcome) {
        connectAuthLatencyNanos.add(Math.max(0L, durationNanos));
        updateMax(connectAuthMaxNanos, durationNanos);
        logIfSlow(connectAuthSlow, connectAuthLastSlowLogAtMs, durationNanos,
            "[AUTH] slow connect auth, clientId=" + clientId + ", outcome=" + outcome
                + ", costMs=" + nanosToMillis(durationNanos));
    }

    private void recordPublishAclLatency(long durationNanos, String clientId, String topic, String outcome) {
        publishAclLatencyNanos.add(Math.max(0L, durationNanos));
        updateMax(publishAclMaxNanos, durationNanos);
        logIfSlow(publishAclSlow, publishAclLastSlowLogAtMs, durationNanos,
            "[ACL] slow publish acl, clientId=" + clientId + ", topic=" + topic
                + ", outcome=" + outcome + ", costMs=" + nanosToMillis(durationNanos));
    }

    private void logIfSlow(LongAdder slowCounter, AtomicLong lastLogAtMs, long durationNanos, String message) {
        long costMs = nanosToMillis(durationNanos);
        if (costMs < SLOW_THRESHOLD_MS) {
            return;
        }
        slowCounter.increment();
        long now = System.currentTimeMillis();
        long last = lastLogAtMs.get();
        if (now - last < SLOW_LOG_INTERVAL_MS) {
            return;
        }
        if (!lastLogAtMs.compareAndSet(last, now)) {
            return;
        }
        logger.warning(message);
    }

    private static void updateMax(AtomicLong max, long candidate) {
        long safeCandidate = Math.max(0L, candidate);
        long current = max.get();
        while (safeCandidate > current && !max.compareAndSet(current, safeCandidate)) {
            current = max.get();
        }
    }

    private static long nanosToMillis(long nanos) {
        return nanos <= 0L ? 0L : nanos / 1_000_000L;
    }

    public record Snapshot(
        long connectAuthSuccess,
        long connectAuthFailure,
        long connectAuthError,
        long connectAuthSlow,
        long connectAuthAvgMs,
        long connectAuthMaxMs,
        long publishAclAllow,
        long publishAclDeny,
        long publishAclError,
        long publishAclSlow,
        long publishAclAvgMs,
        long publishAclMaxMs
    ) {
    }
}
