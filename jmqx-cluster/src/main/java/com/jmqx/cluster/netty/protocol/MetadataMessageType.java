package com.jmqx.cluster.netty.protocol;

/**
 * 元数据传输消息类型定义。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public final class MetadataMessageType {
    // 元数据请求
    public static final byte SUBMIT_REQUEST = 1;
    // 元数据响应
    public static final byte SUBMIT_RESPONSE = 2;
    // 订阅请求
    public static final byte SUBSCRIBE_REQUEST = 3;
    // 事件请求
    public static final byte EVENT = 4;
    // 确认请求
    public static final byte ACK_REQUEST = 5;
    // 确认响应
    public static final byte ACK_RESPONSE = 6;
    // 重置请求
    public static final byte RESET = 7;

    private MetadataMessageType() {
    }
}
