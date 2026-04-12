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
                MessageBridge bridge = createSingle(type, properties);
                if (bridge != null) {
                    delegates.add(bridge);
                }
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

    private static MessageBridge createSingle(String type, BridgeProperties properties) {
        return switch (type) {
            case "kafka" -> wrapWithTopicFilter(new KafkaMessageBridge(properties), properties.getKafkaSourceTopicFilters());
            case "rocketmq" -> wrapWithTopicFilter(new RocketMqMessageBridge(properties), properties.getRocketmqSourceTopicFilters());
            case "mysql" -> wrapWithTopicFilter(new MysqlMessageBridge(properties), properties.getMysqlSourceTopicFilters());
            default -> {
                LOG.warning("[BRIDGE] unsupported type: " + type);
                yield null;
            }
        };
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

    /**
     * 仅在消息主题命中过滤器时才转发给下游桥接器。
     */
    private static final class FilteredMessageBridge implements MessageBridge {
        private final MessageBridge delegate;
        private final List<String> filters;

        private FilteredMessageBridge(MessageBridge delegate, List<String> filters) {
            this.delegate = delegate;
            this.filters = List.copyOf(filters);
        }

        @Override
        public void publish(BridgeMessage message) {
            if (message == null || message.topic() == null || message.topic().isBlank()) {
                return;
            }
            for (String filter : filters) {
                if (matchesTopicFilter(filter, message.topic())) {
                    delegate.publish(message);
                    return;
                }
            }
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static boolean matchesTopicFilter(String filter, String topic) {
        if (filter == null || filter.isBlank() || topic == null || topic.isBlank()) {
            return false;
        }
        String[] filterLevels = filter.split("/", -1);
        String[] topicLevels = topic.split("/", -1);
        int fi = 0;
        int ti = 0;
        while (fi < filterLevels.length && ti < topicLevels.length) {
            String level = filterLevels[fi];
            if ("#".equals(level)) {
                return fi == filterLevels.length - 1;
            }
            if ("+".equals(level) || level.equals(topicLevels[ti])) {
                fi++;
                ti++;
                continue;
            }
            return false;
        }
        if (fi == filterLevels.length && ti == topicLevels.length) {
            return true;
        }
        return fi == filterLevels.length - 1 && "#".equals(filterLevels[fi]);
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
