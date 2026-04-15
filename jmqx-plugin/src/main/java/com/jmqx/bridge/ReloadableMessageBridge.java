package com.jmqx.bridge;

/**
 * 支持运行时替换的桥接器包装。
 *
 * @author liucaiwen
 * @date 2026/4/14
 */
public class ReloadableMessageBridge implements MessageBridge {
    private volatile MessageBridge delegate;

    public ReloadableMessageBridge(MessageBridge delegate) {
        this.delegate = normalizeDelegate(delegate);
    }

    public void setDelegate(MessageBridge delegate) {
        MessageBridge previous = this.delegate;
        this.delegate = normalizeDelegate(delegate);
        if (previous != null && previous != this.delegate) {
            previous.close();
        }
    }

    @Override
    public void publish(BridgeMessage message) {
        delegate.publish(message);
    }

    @Override
    public void close() {
        MessageBridge current = delegate;
        delegate = MessageBridge.NOOP;
        if (current != null) {
            current.close();
        }
    }

    private static MessageBridge normalizeDelegate(MessageBridge delegate) {
        if (delegate == null) {
            return MessageBridge.NOOP;
        }
        return delegate;
    }
}
