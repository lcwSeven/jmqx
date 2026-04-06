package com.jmqx.auth;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public final class AuthProviderFactory {
    private AuthProviderFactory() {
    }

    public static AuthProvider create(AuthProperties properties) {
        String type = normalizeType(properties.getType());
        Map<String, AuthProviderPlugin> plugins = new HashMap<>();
        registerBuiltins(plugins);
        loadExtensions(plugins);

        AuthProviderPlugin plugin = plugins.get(type);
        if (plugin == null) {
            plugin = plugins.get("allow_all");
        }
        AuthProvider delegate = plugin.create(properties);
        if (properties.getCacheMillis() <= 0) {
            return delegate;
        }
        return new CachedAuthProvider(delegate, properties.getCacheMillis());
    }

    private static void registerBuiltins(Map<String, AuthProviderPlugin> plugins) {
        plugins.put("allow_all", new NamedPlugin("allow_all", p -> new AllowAllAuthProvider()));
        plugins.put("http", new NamedPlugin("http", HttpAuthProvider::new));
        plugins.put("file", new NamedPlugin("file", FileAuthProvider::new));
        plugins.put("redis", new NamedPlugin("redis", RedisAuthProvider::new));
        plugins.put("db", new NamedPlugin("db", DbAuthProvider::new));
    }

    private static void loadExtensions(Map<String, AuthProviderPlugin> plugins) {
        ServiceLoader<AuthProviderPlugin> loader = ServiceLoader.load(AuthProviderPlugin.class);
        for (AuthProviderPlugin plugin : loader) {
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
    private static class NamedPlugin implements AuthProviderPlugin {
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
        public AuthProvider create(AuthProperties properties) {
            return creator.create(properties);
        }
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private interface Creator {
        AuthProvider create(AuthProperties properties);
    }
}
