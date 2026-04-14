package com.jmqx.acl;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public final class AclAuthorizerFactory {
    private AclAuthorizerFactory() {
    }

    public static AclAuthorizer create(AclProperties properties) {
        Map<String, AclAuthorizerPlugin> plugins = new HashMap<>();
        registerBuiltins(plugins);
        loadExtensions(plugins);

        AclAuthorizer delegate = createDelegate(properties, plugins);
        if (properties.getCacheMillis() <= 0) {
            return delegate;
        }
        return new CachedAclAuthorizer(delegate, properties.getCacheMillis());
    }

    private static AclAuthorizer createDelegate(
            AclProperties properties,
            Map<String, AclAuthorizerPlugin> plugins
    ) {
        List<String> chainTypes = parseChain(properties.getChain());
        if (!chainTypes.isEmpty()) {
            List<AclAuthorizer> chain = chainTypes.stream()
                    .map(plugins::get)
                    .filter(Objects::nonNull)
                    .map(plugin -> plugin.create(properties))
                    .collect(Collectors.toList());
            return new ChainedAclAuthorizer(chain, properties.isDefaultAllow());
        }
        return plugins.get("allow_all").create(properties);
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
            plugins.put(normalizeChainType(plugin.type()), plugin);
        }
    }

    private static List<String> parseChain(String chainRaw) {
        if (chainRaw == null || chainRaw.isBlank()) {
            return List.of();
        }
        return Stream.of(chainRaw.split(","))
                .map(AclAuthorizerFactory::normalizeChainType)
                .filter(type -> !type.isBlank() && !"allow_all".equals(type))
                .collect(Collectors.toList());
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
