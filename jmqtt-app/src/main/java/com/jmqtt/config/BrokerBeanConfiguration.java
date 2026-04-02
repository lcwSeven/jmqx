package com.jmqtt.config;

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
import com.jmqtt.lifecycle.MqttServerLifecycle;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BrokerBeanConfiguration {
    @Bean
    @ConfigurationProperties(prefix = "jmqtt.broker")
    public BrokerProperties brokerProperties() {
        return new BrokerProperties();
    }

    @Bean
    public SessionRegistry sessionRegistry() {
        return new InMemorySessionRegistry();
    }

    @Bean
    public SubscriptionRegistry subscriptionRegistry() {
        return new InMemorySubscriptionRegistry();
    }

    @Bean
    public RetainedMessageStore retainedMessageStore() {
        return new InMemoryRetainedMessageStore();
    }

    @Bean
    public ClientAuthenticator clientAuthenticator() {
        return ClientAuthenticator.ALLOW_ALL;
    }

    @Bean
    public BrokerMessageHandler brokerMessageHandler(
        SessionRegistry sessionRegistry,
        SubscriptionRegistry subscriptionRegistry,
        RetainedMessageStore retainedMessageStore,
        ClientAuthenticator clientAuthenticator
    ) {
        return new SimpleBrokerMessageHandler(
            sessionRegistry,
            subscriptionRegistry,
            retainedMessageStore,
            clientAuthenticator
        );
    }

    @Bean
    public NettyMqttServer nettyMqttServer(
        BrokerProperties brokerProperties,
        BrokerMessageHandler brokerMessageHandler
    ) {
        return new NettyMqttServer(brokerProperties, brokerMessageHandler);
    }

    @Bean
    public MqttServerLifecycle mqttServerLifecycle(NettyMqttServer nettyMqttServer) {
        return new MqttServerLifecycle(nettyMqttServer);
    }
}
