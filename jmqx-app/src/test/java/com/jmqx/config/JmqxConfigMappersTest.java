package com.jmqx.config;

import com.jmqx.common.BrokerProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JmqxConfigMappersTest {
    private static final String NO_LOCAL_KEY = "jmqx.broker.publish.noLocalEnabled";

    @AfterEach
    void clearSystemProperties() {
        System.clearProperty(NO_LOCAL_KEY);
    }

    @Test
    void shouldLoadNoLocalPublishFlagFromDefaultConfig() {
        BrokerProperties properties = JmqxConfigMappers.loadBrokerProperties(JmqxConfig.loadDefault());

        assertTrue(properties.isPublishNoLocalEnabled());
    }

    @Test
    void shouldAllowSystemPropertyToOverrideNoLocalPublishFlag() {
        System.setProperty(NO_LOCAL_KEY, "false");

        BrokerProperties properties = JmqxConfigMappers.loadBrokerProperties(JmqxConfig.loadDefault());

        assertFalse(properties.isPublishNoLocalEnabled());
    }

    @Test
    void shouldNormalizeAuthChainAndRemoveLegacyPlugins() {
        com.jmqx.auth.AuthProperties properties = new com.jmqx.auth.AuthProperties();
        properties.setChain(" allow_all , HTTP , file , redis ");

        List<String> chain = JmqxConfigMappers.resolveAuthChain(properties);

        assertEquals(List.of("http", "redis"), chain);
    }
}
