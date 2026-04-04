package com.jmqtt.acl;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public final class AclAuthorizerFactory {
    private AclAuthorizerFactory() {
    }

    public static AclAuthorizer create(AclProperties properties) {
        String type = normalizeType(properties.getType());
        Map<String, AclAuthorizerPlugin> plugins = new HashMap<>();
        registerBuiltins(plugins);
        loadExtensions(plugins);

        AclAuthorizerPlugin plugin = plugins.get(type);
        if (plugin == null) {
            plugin = plugins.get("allow_all");
        }
        AclAuthorizer delegate = plugin.create(properties);
        if (properties.getCacheSeconds() <= 0) {
            return delegate;
        }
        return new CachedAclAuthorizer(delegate, properties.getCacheSeconds());
    }

    private static void registerBuiltins(Map<String, AclAuthorizerPlugin> plugins) {
        plugins.put("allow_all", new NamedPlugin("allow_all", p -> new AllowAllAclAuthorizer()));
        plugins.put("http", new NamedPlugin("http", HttpAclAuthorizer::new));
        plugins.put("redis", new NamedPlugin("redis", RedisAclAuthorizer::new));
        plugins.put("file", new NamedPlugin("file", FileAclAuthorizer::new));
    }

    private static void loadExtensions(Map<String, AclAuthorizerPlugin> plugins) {
        ServiceLoader<AclAuthorizerPlugin> loader = ServiceLoader.load(AclAuthorizerPlugin.class);
        for (AclAuthorizerPlugin plugin : loader) {
            plugins.put(normalizeType(plugin.type()), plugin);
        }
    }

    private static String normalizeType(String type) {
        if (type == null || type.isBlank()) {
            return "allow_all";
        }
        return type.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private static class NamedPlugin implements AclAuthorizerPlugin {
        private final String type;
        private final Creator creator;

        private NamedPlugin(String type, Creator creator) {
            this.type = type;
            this.creator = creator;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public AclAuthorizer create(AclProperties properties) {
            return creator.create(properties);
        }
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private interface Creator {
        AclAuthorizer create(AclProperties properties);
    }
}
