package com.jmqx.config;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * YAML 配置读取与点分键访问工具。
 *
 * @author liucaiwen
 * @date 2026/4/12
 */
public final class JmqxConfig {
    /**
     * YAML 默认配置加载顺序（先模块默认，再根文件覆盖）。
     */
    private static final List<String> DEFAULT_YAML_CONFIG_RESOURCES = List.of(
            "config/broker.yaml",
            "config/security-auth.yaml",
            "config/security-acl.yaml",
            "config/retained.yaml",
            "config/shared.yaml",
            "config/cluster.yaml",
            "config/admin.yaml",
            "config/bridge.yaml",
            "jmqx.yaml"
    );

    private final Map<String, String> values;

    private JmqxConfig(Map<String, String> values) {
        this.values = values == null ? Map.of() : Map.copyOf(values);
    }

    public static JmqxConfig loadDefault() {
        Map<String, String> flattened = new LinkedHashMap<>();
        for (String resource : DEFAULT_YAML_CONFIG_RESOURCES) {
            try (InputStream in = JmqxConfig.class.getClassLoader().getResourceAsStream(resource)) {
                if (in == null) {
                    continue;
                }
                loadYamlIntoMap(in, flattened);
            } catch (IOException ignored) {
            }
        }
        return new JmqxConfig(flattened);
    }

    public String getString(String key, String defaultValue) {
        String system = System.getProperty(key);
        if (system != null && !system.isBlank()) {
            return system;
        }
        String raw = values.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        return raw;
    }

    public int getInt(String key, int defaultValue) {
        String raw = getString(key, Integer.toString(defaultValue));
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    public long getLong(String key, long defaultValue) {
        String raw = getString(key, Long.toString(defaultValue));
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String raw = getString(key, Boolean.toString(defaultValue));
        if ("true".equalsIgnoreCase(raw) || "1".equals(raw) || "yes".equalsIgnoreCase(raw)) {
            return true;
        }
        if ("false".equalsIgnoreCase(raw) || "0".equals(raw) || "no".equalsIgnoreCase(raw)) {
            return false;
        }
        return defaultValue;
    }

    public Set<String> getStringSet(String key) {
        String raw = getString(key, "");
        if (raw.isBlank()) {
            return Set.of();
        }
        String[] items = raw.split(",");
        Set<String> result = new LinkedHashSet<>();
        for (String item : items) {
            if (item == null || item.isBlank()) {
                continue;
            }
            result.add(item.trim());
        }
        return result;
    }

    public Map<String, String> getStringMap(String key) {
        String raw = getString(key, "");
        if (raw.isBlank()) {
            return Map.of();
        }
        String[] items = raw.split(",");
        Map<String, String> result = new LinkedHashMap<>();
        for (String item : items) {
            if (item == null || item.isBlank()) {
                continue;
            }
            int index = item.indexOf('=');
            if (index <= 0 || index >= item.length() - 1) {
                continue;
            }
            String mapKey = item.substring(0, index).trim();
            String mapValue = item.substring(index + 1).trim();
            if (mapKey.isEmpty() || mapValue.isEmpty()) {
                continue;
            }
            result.put(mapKey, mapValue);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static void loadYamlIntoMap(InputStream in, Map<String, String> flattened) {
        Object root = new Yaml().load(in);
        if (!(root instanceof Map<?, ?> rootMap)) {
            return;
        }
        flattenYamlMap("", (Map<Object, Object>) rootMap, flattened);
    }

    private static void flattenYamlMap(String prefix, Map<Object, Object> map, Map<String, String> flattened) {
        if (map == null) {
            return;
        }
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            String key = entry.getKey().toString().trim();
            if (key.isEmpty()) {
                continue;
            }
            String fullKey = prefix == null || prefix.isBlank() ? key : prefix + "." + key;
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> nestedMap) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> casted = (Map<Object, Object>) nestedMap;
                flattenYamlMap(fullKey, casted, flattened);
                continue;
            }
            if (value instanceof List<?> listValue) {
                List<String> normalized = new ArrayList<>();
                for (Object item : listValue) {
                    if (item != null) {
                        normalized.add(item.toString());
                    }
                }
                flattened.put(fullKey, String.join(",", normalized));
                continue;
            }
            flattened.put(fullKey, value == null ? "" : value.toString());
        }
    }
}
