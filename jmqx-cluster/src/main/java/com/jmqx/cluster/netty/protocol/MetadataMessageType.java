package com.jmqx.cluster.netty.protocol;

/**
 * 元数据传输消息类型定义。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public final class MetadataMessageType {
    public static final byte SUBMIT_REQUEST = 1;
    public static final byte SUBMIT_RESPONSE = 2;
    public static final byte SUBSCRIBE_REQUEST = 3;
    public static final byte EVENT = 4;
    public static final byte ACK_REQUEST = 5;
    public static final byte ACK_RESPONSE = 6;
    public static final byte RESET = 7;

    private MetadataMessageType() {
    }
}
