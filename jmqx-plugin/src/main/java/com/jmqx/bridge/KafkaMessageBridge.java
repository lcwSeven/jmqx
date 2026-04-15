package com.jmqx.bridge;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
        this.producer = new KafkaProducer<>(buildProducerProperties(properties));
    }

    @Override
    public void publish(BridgeMessage message) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, message.topic(), message.payload());
        addHeader(record, "mqtt-topic", message.topic());
        addHeader(record, "mqtt-client-id", message.clientId());
        addHeader(record, "mqtt-qos", Integer.toString(message.qos()));
        addHeader(record, "mqtt-retain", Boolean.toString(message.retain()));
        addHeader(record, "mqtt-published-at", Long.toString(message.publishedAt()));
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    LOG.warning("[BRIDGE][KAFKA] send failed topic=" + message.topic() + ", error=" + exception.getMessage());
                }
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private static Properties buildProducerProperties(BridgeProperties properties) {
        Properties result = new Properties();
        result.setProperty("bootstrap.servers", properties.getKafkaBootstrapServers());
        result.setProperty("acks", properties.getKafkaAcks());
        result.setProperty("client.id", properties.getKafkaClientId());
        result.setProperty("compression.type", properties.getKafkaCompressionType());
        result.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        result.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return result;
    }

    private static void addHeader(ProducerRecord<String, byte[]> record, String name, String value) {
        record.headers().add(new RecordHeader(name, toBytes(value)));
    }

    private static byte[] toBytes(String value) {
        if (value == null) {
            return new byte[0];
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
