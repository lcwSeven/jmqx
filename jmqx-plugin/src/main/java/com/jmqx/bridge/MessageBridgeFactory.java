package com.jmqx.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public final class MessageBridgeFactory {
    private static final Logger LOG = Logger.getLogger(MessageBridgeFactory.class.getName());

    private MessageBridgeFactory() {
    }

    public static MessageBridge create(BridgeProperties properties) {
        if (properties == null || !properties.isEnabled()) {
            return MessageBridge.NOOP;
        }

        List<String> types = parseTypes(properties.getTypes());
        if (types.isEmpty()) {
            LOG.warning("[BRIDGE] enabled=true but no types configured");
            return MessageBridge.NOOP;
        }

        List<MessageBridge> delegates = new ArrayList<>();
        for (String type : types) {
            try {
                addBridgeIfSupported(delegates, type, properties);
            } catch (Exception e) {
                LOG.warning("[BRIDGE] init " + type + " failed: " + e.getMessage());
            }
        }

        if (delegates.isEmpty()) {
            return MessageBridge.NOOP;
        }

        MessageBridge delegate = delegates.size() == 1 ? delegates.get(0) : new MultiMessageBridge(delegates);
        if (!properties.isAsync()) {
            return delegate;
        }
        return new AsyncMessageBridge(
            delegate,
            properties.getAsyncQueueCapacity(),
            properties.getAsyncWorkerCount()
        );
    }

    private static void addBridgeIfSupported(List<MessageBridge> delegates, String type, BridgeProperties properties) {
        MessageBridge bridge = createBridge(type, properties);
        if (bridge != null) {
            delegates.add(bridge);
        }
    }

    private static MessageBridge createBridge(String type, BridgeProperties properties) {
        if ("kafka".equals(type)) {
            return wrapWithTopicFilter(
                new KafkaMessageBridge(properties),
                properties.getKafkaSourceTopicFilters()
            );
        }
        if ("rocketmq".equals(type)) {
            return wrapWithTopicFilter(
                new RocketMqMessageBridge(properties),
                properties.getRocketmqSourceTopicFilters()
            );
        }
        if ("mysql".equals(type)) {
            return wrapWithTopicFilter(
                new MysqlMessageBridge(properties),
                properties.getMysqlSourceTopicFilters()
            );
        }
        LOG.warning("[BRIDGE] unsupported type: " + type);
        return null;
    }

    private static List<String> parseTypes(String raw) {
        List<String> types = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return types;
        }
        String[] parts = raw.split(",");
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            types.add(part.trim().toLowerCase(Locale.ROOT));
        }
        return types;
    }

    private static MessageBridge wrapWithTopicFilter(MessageBridge delegate, String rawFilters) {
        List<String> filters = parseRawFilters(rawFilters);
        if (filters.isEmpty()) {
            return delegate;
        }
        return new FilteredMessageBridge(delegate, filters);
    }

    private static List<String> parseRawFilters(String raw) {
        List<String> filters = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return filters;
        }
        String[] parts = raw.split(",");
        for (String part : parts) {
            if (part == null) {
                continue;
            }
            String filter = part.trim();
            if (!filter.isBlank()) {
                filters.add(filter);
            }
        }
        return filters;
    }
}
