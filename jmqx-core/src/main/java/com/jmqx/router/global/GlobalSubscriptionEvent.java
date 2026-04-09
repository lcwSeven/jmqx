package com.jmqx.router.global;

/**
 * Replicated global subscription event.
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class GlobalSubscriptionEvent {
    private final long logIndex;
    private final GlobalSubscriptionEventType type;
    private final String nodeId;
    private final String topicFilter;
    private final String sharedGroup;

    private GlobalSubscriptionEvent(
        long logIndex,
        GlobalSubscriptionEventType type,
        String nodeId,
        String topicFilter,
        String sharedGroup
    ) {
        this.logIndex = logIndex;
        this.type = type;
        this.nodeId = nodeId;
        this.topicFilter = topicFilter;
        this.sharedGroup = sharedGroup;
    }

    public static GlobalSubscriptionEvent register(long logIndex, String nodeId, String topicFilter, String sharedGroup) {
        return new GlobalSubscriptionEvent(logIndex, GlobalSubscriptionEventType.REGISTER, nodeId, topicFilter, sharedGroup);
    }

    public static GlobalSubscriptionEvent unregister(long logIndex, String nodeId, String topicFilter, String sharedGroup) {
        return new GlobalSubscriptionEvent(logIndex, GlobalSubscriptionEventType.UNREGISTER, nodeId, topicFilter, sharedGroup);
    }

    public long getLogIndex() {
        return logIndex;
    }

    public GlobalSubscriptionEventType getType() {
        return type;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getTopicFilter() {
        return topicFilter;
    }

    public String getSharedGroup() {
        return sharedGroup;
    }
}
