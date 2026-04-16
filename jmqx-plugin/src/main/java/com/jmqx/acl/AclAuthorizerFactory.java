package com.jmqx.acl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
            List<AclAuthorizer> chain = new ArrayList<>();
            for (String chainType : chainTypes) {
                AclAuthorizerPlugin plugin = plugins.get(chainType);
                if (plugin != null) {
                    chain.add(plugin.create(properties));
                }
            }
            return new ChainedAclAuthorizer(chain, properties.isDefaultAllow());
        }
        return plugins.get("allow_all").create(properties);
    }

    private static void registerBuiltins(Map<String, AclAuthorizerPlugin> plugins) {
        plugins.put("allow_all", new AllowAllPlugin());
        plugins.put("http", new HttpPlugin());
        plugins.put("redis", new RedisPlugin());
        plugins.put("file", new FilePlugin());
    }

    private static void loadExtensions(Map<String, AclAuthorizerPlugin> plugins) {
        ServiceLoader<AclAuthorizerPlugin> loader = ServiceLoader.load(AclAuthorizerPlugin.class);
        for (AclAuthorizerPlugin plugin : loader) {
            plugins.put(normalizeChainType(plugin.type()), plugin);
        }
    }

    private static List<String> parseChain(String chainRaw) {
        List<String> result = new ArrayList<>();
        if (chainRaw == null || chainRaw.isBlank()) {
            return result;
        }
        String[] values = chainRaw.split(",");
        for (String value : values) {
            String normalized = normalizeChainType(value);
            if (!normalized.isBlank() && !"allow_all".equals(normalized)) {
                result.add(normalized);
            }
        }
        return result;
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
    private static class AllowAllPlugin implements AclAuthorizerPlugin {
        @Override
        public String type() {
            return "allow_all";
        }

        @Override
        public AclAuthorizer create(AclProperties properties) {
            return new AllowAllAclAuthorizer();
        }
    }

    private static class HttpPlugin implements AclAuthorizerPlugin {
        @Override
        public String type() {
            return "http";
        }

        @Override
        public AclAuthorizer create(AclProperties properties) {
            return new HttpAclAuthorizer(properties);
        }
    }

    private static class RedisPlugin implements AclAuthorizerPlugin {
        @Override
        public String type() {
            return "redis";
        }

        @Override
        public AclAuthorizer create(AclProperties properties) {
            return new RedisAclAuthorizer(properties);
        }
    }

    private static class FilePlugin implements AclAuthorizerPlugin {
        @Override
        public String type() {
            return "file";
        }

        @Override
        public AclAuthorizer create(AclProperties properties) {
            return new FileAclAuthorizer(properties);
        }
    }
}
