package com.jmqx.auth;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            // 链式模式下只保留配置中显式存在的插件，顺序完全以配置为准。
            List<AuthProvider> chain = chainTypes.stream()
                .map(plugins::get)
                .filter(Objects::nonNull)
                .map(plugin -> plugin.create(properties))
                .collect(Collectors.toList());
            return new ChainedAuthProvider(chain);
        }
        return resolvePlugin(plugins, normalizeType(properties.getType())).create(properties);
    }

    private static List<String> parseChain(String chainRaw) {
        if (chainRaw == null || chainRaw.isBlank()) {
            return List.of();
        }
        return Stream.of(chainRaw.split(","))
            .map(AuthProviderFactory::normalizeChainType)
            .filter(type -> !type.isBlank())
            .collect(Collectors.toList());
    }

    private static AuthProviderPlugin resolvePlugin(Map<String, AuthProviderPlugin> plugins, String type) {
        AuthProviderPlugin plugin = plugins.get(type);
        if (plugin != null) {
            return plugin;
        }
        // 未识别类型兜底为 allow_all，避免启动期因为配置错误直接崩溃。
        return plugins.get("allow_all");
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
