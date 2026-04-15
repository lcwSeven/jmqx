package com.jmqx.store.qos;

/**
 * QoS2 上行状态：
 * WAIT_PUBREL 表示已收到 PUBLISH 并回了 PUBREC，等待客户端发送 PUBREL；
 * COMPLETED 表示业务投递已完成，重复 PUBREL 只需重回 PUBCOMP。
 *
 * @author liucaiwen
 * @date 2026/4/15
 */
public enum Qos2InboundState {
    WAIT_PUBREL((byte) 0),
    COMPLETED((byte) 1);

    private final byte code;

    Qos2InboundState(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static Qos2InboundState fromCode(byte code) {
        for (Qos2InboundState value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return WAIT_PUBREL;
    }
}
