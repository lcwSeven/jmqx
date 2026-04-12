package com.jmqx.bridge;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class MysqlMessageBridge implements MessageBridge {
    private static final Logger LOG = Logger.getLogger(MysqlMessageBridge.class.getName());

    private final String url;
    private final String user;
    private final String password;
    private final String table;
    private final String insertSql;

    public MysqlMessageBridge(BridgeProperties properties) {
        this.url = properties.getMysqlUrl();
        this.user = properties.getMysqlUser();
        this.password = properties.getMysqlPassword();
        this.table = normalizeTableName(properties.getMysqlTable());
        this.insertSql = "INSERT INTO " + table
            + " (client_id, topic, payload, qos, retain, published_at) VALUES (?, ?, ?, ?, ?, ?)";

        String driver = properties.getMysqlDriver();
        if (driver != null && !driver.isBlank()) {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("mysql driver not found: " + driver, e);
            }
        }

        if (properties.isMysqlAutoCreateTable()) {
            createTableIfNeeded();
        }
    }

    @Override
    public void publish(BridgeMessage message) {
        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement statement = connection.prepareStatement(insertSql)) {
            statement.setString(1, message.clientId());
            statement.setString(2, message.topic());
            statement.setBytes(3, message.payload());
            statement.setInt(4, message.qos());
            statement.setBoolean(5, message.retain());
            statement.setLong(6, message.publishedAt());
            statement.executeUpdate();
        } catch (SQLException e) {
            LOG.warning("[BRIDGE][MYSQL] insert failed topic=" + message.topic() + ", error=" + e.getMessage());
        }
    }

    private void createTableIfNeeded() {
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " ("
            + "id BIGINT PRIMARY KEY AUTO_INCREMENT,"
            + "client_id VARCHAR(256) NULL,"
            + "topic VARCHAR(1024) NOT NULL,"
            + "payload LONGBLOB NOT NULL,"
            + "qos INT NOT NULL,"
            + "retain BOOLEAN NOT NULL,"
            + "published_at BIGINT NOT NULL,"
            + "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
            + "INDEX idx_topic(topic(255)),"
            + "INDEX idx_published_at(published_at)"
            + ")";
        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        } catch (SQLException e) {
            throw new IllegalStateException("create mysql bridge table failed", e);
        }
    }

    private static String normalizeTableName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            return "jmqx_bridge_message";
        }
        String normalized = tableName.trim().toLowerCase(Locale.ROOT);
        if (!normalized.matches("[a-z0-9_]+")) {
            return "jmqx_bridge_message";
        }
        return normalized;
    }
}
