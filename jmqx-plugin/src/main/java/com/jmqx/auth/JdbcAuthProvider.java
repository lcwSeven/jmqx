package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 JDBC + HikariCP 的数据库认证。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public class JdbcAuthProvider implements AuthProvider {
    private static final Logger LOG = Logger.getLogger(JdbcAuthProvider.class.getName());

    private final String databaseType;
    private final String query;
    private final HikariDataSource dataSource;

    public JdbcAuthProvider(
            String databaseType,
            String driverClassName,
            String jdbcUrl,
            String username,
            String password,
            String query,
            int minIdle,
            int maxPoolSize,
            long connectionTimeoutMs,
            long idleTimeoutMs,
            long maxLifetimeMs
    ) {
        this.databaseType = databaseType;
        this.query = query;
        HikariConfig config = new HikariConfig();
        config.setPoolName("jmqx-auth-" + databaseType);
        config.setDriverClassName(driverClassName);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(Math.max(0, minIdle));
        config.setMaximumPoolSize(Math.max(1, maxPoolSize));
        config.setConnectionTimeout(Math.max(250L, connectionTimeoutMs));
        config.setIdleTimeout(Math.max(10_000L, idleTimeoutMs));
        config.setMaxLifetime(Math.max(30_000L, maxLifetimeMs));
        this.dataSource = new HikariDataSource(config);
    }

    public static JdbcAuthProvider mysql(AuthProperties properties) {
        return new JdbcAuthProvider(
                "mysql",
                "com.mysql.cj.jdbc.Driver",
                properties.getMysqlUrl(),
                properties.getMysqlUser(),
                properties.getMysqlPassword(),
                properties.getMysqlQuery(),
                properties.getMysqlPoolMinIdle(),
                properties.getMysqlPoolMaxSize(),
                properties.getMysqlPoolConnectionTimeoutMs(),
                properties.getMysqlPoolIdleTimeoutMs(),
                properties.getMysqlPoolMaxLifetimeMs()
        );
    }

    public static JdbcAuthProvider postgresql(AuthProperties properties) {
        return new JdbcAuthProvider(
                "postgresql",
                "org.postgresql.Driver",
                properties.getPostgresqlUrl(),
                properties.getPostgresqlUser(),
                properties.getPostgresqlPassword(),
                properties.getPostgresqlQuery(),
                properties.getPostgresqlPoolMinIdle(),
                properties.getPostgresqlPoolMaxSize(),
                properties.getPostgresqlPoolConnectionTimeoutMs(),
                properties.getPostgresqlPoolIdleTimeoutMs(),
                properties.getPostgresqlPoolMaxLifetimeMs()
        );
    }

    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        if (request.getUsername() == null || request.getUsername().isBlank()) {
            return AuthResult.deny();
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, request.getUsername());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return AuthResult.notFound();
                }
                String expected = rs.getString(1);
                return expected != null && expected.equals(request.getPassword()) ? AuthResult.allow() : AuthResult.deny();
            }
        } catch (SQLException exception) {
            LOG.log(Level.WARNING, databaseType + " auth failed: " + exception.getMessage(), exception);
            return AuthResult.deny();
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
