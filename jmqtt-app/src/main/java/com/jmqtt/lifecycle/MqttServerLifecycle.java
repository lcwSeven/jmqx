package com.jmqtt.lifecycle;

import com.jmqtt.transport.NettyMqttServer;
import org.springframework.context.SmartLifecycle;

public class MqttServerLifecycle implements SmartLifecycle {
    private final NettyMqttServer nettyMqttServer;
    private volatile boolean running;

    public MqttServerLifecycle(NettyMqttServer nettyMqttServer) {
        this.nettyMqttServer = nettyMqttServer;
    }

    @Override
    public void start() {
        if (running) {
            return;
        }

        try {
            nettyMqttServer.start();
            running = true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while starting MQTT server", e);
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        nettyMqttServer.stop();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
