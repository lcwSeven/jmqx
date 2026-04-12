package com.jmqx.broker.protocol;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * MQTT 报文构建与解析工具。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public final class MqttPacketFactory {
    private MqttPacketFactory() {
    }

    public static MqttPublishMessage buildQos1PublishMessage(String topic, byte[] payload, int packetId, boolean dup) {
        return new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, dup, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttPublishVariableHeader(topic, packetId, MqttProperties.NO_PROPERTIES),
                Unpooled.wrappedBuffer(payload)
        );
    }

    public static MqttPublishMessage buildQos2PublishMessage(String topic, byte[] payload, int packetId, boolean dup) {
        return new MqttPublishMessage(
                new MqttFixedHeader(MqttMessageType.PUBLISH, dup, MqttQoS.EXACTLY_ONCE, false, 0),
                new MqttPublishVariableHeader(topic, packetId, MqttProperties.NO_PROPERTIES),
                Unpooled.wrappedBuffer(payload)
        );
    }

    public static MqttMessage buildPubRecMessage(int packetId) {
        return new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId)
        );
    }

    public static MqttMessage buildPubRelMessage(int packetId) {
        return new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId)
        );
    }

    public static MqttMessage buildPubCompMessage(int packetId) {
        return new MqttMessage(
                new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId)
        );
    }

    public static Integer extractPacketId(MqttMessage message) {
        if (message == null) {
            return null;
        }
        if (message.variableHeader() instanceof MqttMessageIdVariableHeader messageIdVariableHeader) {
            return messageIdVariableHeader.messageId();
        }
        return null;
    }
}
