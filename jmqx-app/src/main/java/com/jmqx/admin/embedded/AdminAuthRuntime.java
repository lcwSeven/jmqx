package com.jmqx.admin.embedded;

/**
 * 管理端账号运行时配置。
 *
 * @author liucaiwen
 * @since 2026-04-13
 */
public final class AdminAuthRuntime {

    private volatile Config current;

    public AdminAuthRuntime(Config initial) {
        this.current = initial == null ? Config.defaults() : initial.normalize();
    }

    public Config current() {
        return current;
    }

    public void update(Config config) {
        this.current = config == null ? Config.defaults() : config.normalize();
    }

    public boolean isAuthRequired() {
        return !current.username().isBlank();
    }

    public boolean matches(String username, String password) {
        Config config = current;
        return config.username().equals(username) && config.password().equals(password == null ? "" : password);
    }

    public record Config(
            String username,
            String password,
            String role
    ) {
        public Config normalize() {
            return new Config(
                    normalize(username, "admin"),
                    password == null ? "public" : password,
                    normalize(role, "super_admin")
            );
        }

        public static Config defaults() {
            return new Config("admin", "public", "super_admin");
        }

        private static String normalize(String value, String defaultValue) {
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            return value.trim();
        }
    }
}
