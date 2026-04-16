package com.jmqx.auth;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Auth provider 装配工厂。
 * 支持内置插件、SPI 扩展、链式鉴权以及统一缓存包装。
 *
 * @author liucaiwen
 * @date 2026/4/4
 */
public final class AuthProviderFactory {
    private AuthProviderFactory() {
    }

    public static AuthProvider create(AuthProperties properties) {
        Map<String, AuthProviderPlugin> plugins = new HashMap<>();
        registerBuiltins(plugins);
        loadExtensions(plugins);

        // 先构建真实 provider，再按需统一加缓存，避免每个插件都重复实现缓存逻辑。
        AuthProvider delegate = createDelegate(properties, plugins);
        if (properties.getCacheMillis() <= 0) {
            return delegate;
        }
        return new CachedAuthProvider(delegate, properties.getCacheMillis());
    }

    private static AuthProvider createDelegate(
        AuthProperties properties,
        Map<String, AuthProviderPlugin> plugins
    ) {
        List<String> chainTypes = parseChain(properties.getChain());
        if (!chainTypes.isEmpty()) {
            List<AuthProvider> chain = new ArrayList<>();
            for (String chainType : chainTypes) {
                AuthProviderPlugin plugin = plugins.get(chainType);
                if (plugin != null) {
                    chain.add(plugin.create(properties));
                }
            }
            if (chain.isEmpty()) {
                return plugins.get("allow_all").create(properties);
            }
            return new ChainedAuthProvider(chain);
        }
        return plugins.get("allow_all").create(properties);
    }

    private static List<String> parseChain(String chainRaw) {
        List<String> result = new ArrayList<>();
        if (chainRaw == null || chainRaw.isBlank()) {
            return result;
        }
        String[] values = chainRaw.split(",");
        for (String value : values) {
            String normalized = normalizeChainType(value);
            if (!normalized.isBlank()) {
                result.add(normalized);
            }
        }
        return result;
    }

    private static void registerBuiltins(Map<String, AuthProviderPlugin> plugins) {
        plugins.put("allow_all", new AllowAllPlugin());
        plugins.put("built_in_database", new BuiltInDatabasePlugin());
        plugins.put("http", new HttpPlugin());
        plugins.put("redis", new RedisPlugin());
        plugins.put("mysql", new MysqlPlugin());
        plugins.put("postgresql", new PostgresqlPlugin());
    }

    private static void loadExtensions(Map<String, AuthProviderPlugin> plugins) {
        ServiceLoader<AuthProviderPlugin> loader = ServiceLoader.load(AuthProviderPlugin.class);
        for (AuthProviderPlugin plugin : loader) {
            plugins.put(normalizeChainType(plugin.type()), plugin);
        }
    }

    private static String normalizeChainType(String type) {
        if (type == null) {
            return "";
        }
        return type.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private static class AllowAllPlugin implements AuthProviderPlugin {
        @Override
        public String type() {
            return "allow_all";
        }

        @Override
        public AuthProvider create(AuthProperties properties) {
            return new AllowAllAuthProvider();
        }
    }

    private static class BuiltInDatabasePlugin implements AuthProviderPlugin {
        @Override
        public String type() {
            return "built_in_database";
        }

        @Override
        public AuthProvider create(AuthProperties properties) {
            return new BuiltInDatabaseAuthProvider(properties);
        }
    }

    private static class HttpPlugin implements AuthProviderPlugin {
        @Override
        public String type() {
            return "http";
        }

        @Override
        public AuthProvider create(AuthProperties properties) {
            return new HttpAuthProvider(properties);
        }
    }



    private static class RedisPlugin implements AuthProviderPlugin {
        @Override
        public String type() {
            return "redis";
        }

        @Override
        public AuthProvider create(AuthProperties properties) {
            return new RedisAuthProvider(properties);
        }
    }

    private static class MysqlPlugin implements AuthProviderPlugin {
        @Override
        public String type() {
            return "mysql";
        }

        @Override
        public AuthProvider create(AuthProperties properties) {
            return JdbcAuthProvider.mysql(properties);
        }
    }

    private static class PostgresqlPlugin implements AuthProviderPlugin {
        @Override
        public String type() {
            return "postgresql";
        }

        @Override
        public AuthProvider create(AuthProperties properties) {
            return JdbcAuthProvider.postgresql(properties);
        }
    }
}
