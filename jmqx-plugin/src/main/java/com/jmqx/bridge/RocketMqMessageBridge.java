package com.jmqx.bridge;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class RocketMqMessageBridge implements MessageBridge {
    private static final Logger LOG = Logger.getLogger(RocketMqMessageBridge.class.getName());

    private final String topic;
    private final boolean syncSend;
    private final int timeoutMs;
    private final DefaultMQProducer producer;

    public RocketMqMessageBridge(BridgeProperties properties) {
        this.topic = properties.getRocketmqTopic();
        this.syncSend = properties.isRocketmqSyncSend();
        this.timeoutMs = Math.max(properties.getRocketmqTimeoutMs(), 100);
        this.producer = new DefaultMQProducer(properties.getRocketmqProducerGroup());
        this.producer.setNamesrvAddr(properties.getRocketmqNameServer());
        try {
            this.producer.start();
        } catch (Exception e) {
            throw new IllegalStateException("start rocketmq producer failed", e);
        }
    }

    @Override
    public void publish(BridgeMessage message) {
        try {
            Message mqMessage = new Message(topic, toTag(message.getTopic()), message.getPayload());
            mqMessage.putUserProperty("mqttTopic", nullToEmpty(message.getTopic()));
            mqMessage.putUserProperty("clientId", nullToEmpty(message.getClientId()));
            mqMessage.putUserProperty("qos", Integer.toString(message.getQos()));
            mqMessage.putUserProperty("retain", Boolean.toString(message.isRetain()));
            mqMessage.putUserProperty("publishedAt", Long.toString(message.getPublishedAt()));
            mqMessage.setKeys(nullToEmpty(message.getClientId()) + ":" + nullToEmpty(message.getTopic()));
            if (syncSend) {
                producer.send(mqMessage, timeoutMs);
                return;
            }
            producer.sendOneway(mqMessage);
        } catch (Exception e) {
            LOG.warning("[BRIDGE][ROCKETMQ] send failed topic=" + message.getTopic() + ", error=" + e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            producer.shutdown();
        } catch (Exception ignored) {
        }
    }

    private static String toTag(String mqttTopic) {
        if (mqttTopic == null || mqttTopic.isBlank()) {
            return "mqtt";
        }
        String normalized = mqttTopic.replace('/', '_').replace('+', 'p').replace('#', 's');
        if (normalized.length() > 120) {
            return normalized.substring(0, 120);
        }
        return normalized;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
