package com.jmqx.auth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class DbAuthProvider implements AuthProvider {
    private static final Logger LOG = Logger.getLogger(DbAuthProvider.class.getName());

    private final String driver;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String dbQuery;

    public DbAuthProvider(AuthProperties properties) {
        this.driver = properties.getDbDriver();
        this.dbUrl = properties.getDbUrl();
        this.dbUser = properties.getDbUser();
        this.dbPassword = properties.getDbPassword();
        this.dbQuery = properties.getDbQuery();

        if (driver != null && !driver.isBlank()) {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                LOG.log(Level.WARNING, "DB driver not found: " + driver, e);
            }
        }
    }

    @Override
    public boolean authenticate(AuthRequest request) {
        return authenticateDecision(request) == AuthDecision.ALLOW;
    }

    @Override
    public AuthDecision authenticateDecision(AuthRequest request) {
        if (request.getUsername() == null || request.getUsername().isBlank()) {
            return AuthDecision.DENY;
        }
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement stmt = connection.prepareStatement(dbQuery)) {
            stmt.setString(1, request.getUsername());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return AuthDecision.NOT_FOUND;
                }
                String expected = rs.getString(1);
                return expected != null && expected.equals(request.getPassword()) ? AuthDecision.ALLOW : AuthDecision.DENY;
            }
        } catch (SQLException e) {
            LOG.log(Level.WARNING, "DB auth failed: " + e.getMessage(), e);
            return AuthDecision.DENY;
        }
    }
}
