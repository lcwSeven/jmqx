package com.jmqx.bridge;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class KafkaMessageBridge implements MessageBridge {
    private static final Logger LOG = Logger.getLogger(KafkaMessageBridge.class.getName());

    private final String topic;
    private final KafkaProducer<String, byte[]> producer;

    public KafkaMessageBridge(BridgeProperties properties) {
        this.topic = properties.getKafkaTopic();
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", properties.getKafkaBootstrapServers());
        producerProperties.setProperty("acks", properties.getKafkaAcks());
        producerProperties.setProperty("client.id", properties.getKafkaClientId());
        producerProperties.setProperty("compression.type", properties.getKafkaCompressionType());
        producerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.producer = new KafkaProducer<>(producerProperties);
    }

    @Override
    public void publish(BridgeMessage message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, message.getTopic(), message.getPayload());
        record.headers().add(new RecordHeader("mqtt-topic", toBytes(message.getTopic())));
        record.headers().add(new RecordHeader("mqtt-client-id", toBytes(message.getClientId())));
        record.headers().add(new RecordHeader("mqtt-qos", toBytes(Integer.toString(message.getQos()))));
        record.headers().add(new RecordHeader("mqtt-retain", toBytes(Boolean.toString(message.isRetain()))));
        record.headers().add(new RecordHeader("mqtt-published-at", toBytes(Long.toString(message.getPublishedAt()))));
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOG.warning("[BRIDGE][KAFKA] send failed topic=" + message.getTopic() + ", error=" + exception.getMessage());
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private static byte[] toBytes(String value) {
        if (value == null) {
            return new byte[0];
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
