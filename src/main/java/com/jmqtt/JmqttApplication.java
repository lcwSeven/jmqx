package com.jmqtt;

import com.jmqtt.broker.BrokerMessageHandler;
import com.jmqtt.broker.SimpleBrokerMessageHandler;
import com.jmqtt.common.BrokerProperties;
import com.jmqtt.protocol.ClientAuthenticator;
import com.jmqtt.router.InMemorySubscriptionRegistry;
import com.jmqtt.router.SubscriptionRegistry;
import com.jmqtt.session.InMemorySessionRegistry;
import com.jmqtt.session.SessionRegistry;
import com.jmqtt.store.InMemoryRetainedMessageStore;
import com.jmqtt.store.RetainedMessageStore;
import com.jmqtt.transport.NettyMqttServer;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class JmqttApplication {
    public static void main(String[] args) throws InterruptedException {
        BrokerProperties brokerProperties = loadBrokerPropertiesFromSystem();
        SessionRegistry sessionRegistry = new InMemorySessionRegistry();
        SubscriptionRegistry subscriptionRegistry = new InMemorySubscriptionRegistry();
        RetainedMessageStore retainedMessageStore = new InMemoryRetainedMessageStore();

        BrokerMessageHandler brokerMessageHandler = new SimpleBrokerMessageHandler(
            sessionRegistry,
            subscriptionRegistry,
            retainedMessageStore,
            ClientAuthenticator.ALLOW_ALL
        );

        NettyMqttServer mqttServer = new NettyMqttServer(brokerProperties, brokerMessageHandler);
        mqttServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(mqttServer::stop));
        System.out.println("JMQTT started on " + brokerProperties.getHost() + ":" + brokerProperties.getPort());

        Thread.currentThread().join();
    }

    private static BrokerProperties loadBrokerPropertiesFromSystem() {
        BrokerProperties properties = new BrokerProperties();
        properties.setHost(System.getProperty("jmqtt.broker.host", properties.getHost()));
        properties.setPort(getIntProperty("jmqtt.broker.port", properties.getPort()));
        properties.setBossThreads(getIntProperty("jmqtt.broker.bossThreads", properties.getBossThreads()));
        properties.setWorkerThreads(getIntProperty("jmqtt.broker.workerThreads", properties.getWorkerThreads()));
        return properties;
    }

    private static int getIntProperty(String key, int defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }
}
