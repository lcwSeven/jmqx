package com.jmqx.broker;

/**
 * QoS2 下行消息状态机阶段。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public enum Qos2State {
    WAIT_PUBREC(1),
    WAIT_PUBCOMP(2);

    private final int code;

    Qos2State(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static Qos2State fromCode(int code) {
        if (code == WAIT_PUBCOMP.code) {
            return WAIT_PUBCOMP;
        }
        return WAIT_PUBREC;
    }
}
