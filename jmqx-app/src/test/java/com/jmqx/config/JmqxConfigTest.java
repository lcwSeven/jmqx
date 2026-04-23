package com.jmqx.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JmqxConfigTest {
    @AfterEach
    void clearSystemProperties() {
        System.clearProperty("jmqx.test.string");
        System.clearProperty("jmqx.test.boolean");
    }

    @Test
    void shouldPreferSystemPropertyOverFlattenedValue() throws Exception {
        JmqxConfig config = createConfig(Map.of("jmqx.test.string", "from-yaml"));
        System.setProperty("jmqx.test.string", "from-system");

        assertEquals("from-system", config.getString("jmqx.test.string", "default"));
    }

    @Test
    void shouldParseBooleanSetAndMapValues() throws Exception {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("jmqx.test.boolean", "yes");
        values.put("jmqx.test.set", "alpha, beta ,gamma");
        values.put("jmqx.test.map", "core-1=127.0.0.1:7900, core-2=127.0.0.1:7901");
        JmqxConfig config = createConfig(values);

        assertTrue(config.getBoolean("jmqx.test.boolean", false));
        assertEquals(Set.of("alpha", "beta", "gamma"), config.getStringSet("jmqx.test.set"));
        assertEquals(
            Map.of("core-1", "127.0.0.1:7900", "core-2", "127.0.0.1:7901"),
            config.getStringMap("jmqx.test.map")
        );
    }

    @Test
    void shouldFallbackToDefaultValuesWhenInputIsInvalid() throws Exception {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("jmqx.test.int", "not-a-number");
        values.put("jmqx.test.boolean", "maybe");
        JmqxConfig config = createConfig(values);

        assertEquals(42, config.getInt("jmqx.test.int", 42));
        assertFalse(config.getBoolean("jmqx.test.boolean", false));
    }

    @SuppressWarnings("unchecked")
    private JmqxConfig createConfig(Map<String, String> values) throws Exception {
        Constructor<JmqxConfig> constructor = JmqxConfig.class.getDeclaredConstructor(Map.class);
        constructor.setAccessible(true);
        return constructor.newInstance(values);
    }
}
