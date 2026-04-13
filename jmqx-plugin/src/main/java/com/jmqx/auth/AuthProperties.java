package com.jmqx.auth;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AuthProperties {
    private String chain = "";
    private int cacheMillis = 60_000;

    private String httpUrl = "";
    private int httpTimeoutMs = 2000;

    private String filePath = "auth-users.txt";

    private static final String BUILT_IN_DATABASE_PATH = "data/auth-built-in-rocksdb";

    private String builtInDatabaseAccountType = "username";
    private String builtInDatabasePasswordHashAlgorithm = "sha256";
    private String builtInDatabaseSaltPosition = "suffix";

    private String redisHost = "127.0.0.1";
    private int redisPort = 6379;
    private String redisPassword = "";
    private int redisDb = 0;
    private String redisKeyPrefix = "jmqx:auth";
    private int redisTimeoutMs = 2000;

    private String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/jmqx";
    private String mysqlUser = "root";
    private String mysqlPassword = "";
    private String mysqlQuery = "SELECT password FROM mqtt_user WHERE username = ?";
    private int mysqlPoolMinIdle = 1;
    private int mysqlPoolMaxSize = 8;
    private long mysqlPoolConnectionTimeoutMs = 3000;
    private long mysqlPoolIdleTimeoutMs = 60_000;
    private long mysqlPoolMaxLifetimeMs = 600_000;

    private String postgresqlUrl = "jdbc:postgresql://127.0.0.1:5432/jmqx";
    private String postgresqlUser = "postgres";
    private String postgresqlPassword = "";
    private String postgresqlQuery = "SELECT password FROM mqtt_user WHERE username = ?";
    private int postgresqlPoolMinIdle = 1;
    private int postgresqlPoolMaxSize = 8;
    private long postgresqlPoolConnectionTimeoutMs = 3000;
    private long postgresqlPoolIdleTimeoutMs = 60_000;
    private long postgresqlPoolMaxLifetimeMs = 600_000;

    public String getChain() {
        return chain;
    }

    public void setChain(String chain) {
        this.chain = chain;
    }

    public int getCacheMillis() {
        return cacheMillis;
    }

    public void setCacheMillis(int cacheMillis) {
        this.cacheMillis = cacheMillis;
    }

    public String getHttpUrl() {
        return httpUrl;
    }

    public void setHttpUrl(String httpUrl) {
        this.httpUrl = httpUrl;
    }

    public int getHttpTimeoutMs() {
        return httpTimeoutMs;
    }

    public void setHttpTimeoutMs(int httpTimeoutMs) {
        this.httpTimeoutMs = httpTimeoutMs;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getBuiltInDatabasePath() {
        return BUILT_IN_DATABASE_PATH;
    }

    public String getBuiltInDatabaseAccountType() {
        return builtInDatabaseAccountType;
    }

    public void setBuiltInDatabaseAccountType(String builtInDatabaseAccountType) {
        this.builtInDatabaseAccountType = builtInDatabaseAccountType;
    }

    public String getBuiltInDatabasePasswordHashAlgorithm() {
        return builtInDatabasePasswordHashAlgorithm;
    }

    public void setBuiltInDatabasePasswordHashAlgorithm(String builtInDatabasePasswordHashAlgorithm) {
        this.builtInDatabasePasswordHashAlgorithm = builtInDatabasePasswordHashAlgorithm;
    }

    public String getBuiltInDatabaseSaltPosition() {
        return builtInDatabaseSaltPosition;
    }

    public void setBuiltInDatabaseSaltPosition(String builtInDatabaseSaltPosition) {
        this.builtInDatabaseSaltPosition = builtInDatabaseSaltPosition;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    public void setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
    }

    public int getRedisDb() {
        return redisDb;
    }

    public void setRedisDb(int redisDb) {
        this.redisDb = redisDb;
    }

    public String getRedisKeyPrefix() {
        return redisKeyPrefix;
    }

    public void setRedisKeyPrefix(String redisKeyPrefix) {
        this.redisKeyPrefix = redisKeyPrefix;
    }

    public int getRedisTimeoutMs() {
        return redisTimeoutMs;
    }

    public void setRedisTimeoutMs(int redisTimeoutMs) {
        this.redisTimeoutMs = redisTimeoutMs;
    }

    public String getMysqlUrl() {
        return mysqlUrl;
    }

    public void setMysqlUrl(String mysqlUrl) {
        this.mysqlUrl = mysqlUrl;
    }

    public String getMysqlUser() {
        return mysqlUser;
    }

    public void setMysqlUser(String mysqlUser) {
        this.mysqlUser = mysqlUser;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public String getMysqlQuery() {
        return mysqlQuery;
    }

    public void setMysqlQuery(String mysqlQuery) {
        this.mysqlQuery = mysqlQuery;
    }

    public int getMysqlPoolMinIdle() {
        return mysqlPoolMinIdle;
    }

    public void setMysqlPoolMinIdle(int mysqlPoolMinIdle) {
        this.mysqlPoolMinIdle = mysqlPoolMinIdle;
    }

    public int getMysqlPoolMaxSize() {
        return mysqlPoolMaxSize;
    }

    public void setMysqlPoolMaxSize(int mysqlPoolMaxSize) {
        this.mysqlPoolMaxSize = mysqlPoolMaxSize;
    }

    public long getMysqlPoolConnectionTimeoutMs() {
        return mysqlPoolConnectionTimeoutMs;
    }

    public void setMysqlPoolConnectionTimeoutMs(long mysqlPoolConnectionTimeoutMs) {
        this.mysqlPoolConnectionTimeoutMs = mysqlPoolConnectionTimeoutMs;
    }

    public long getMysqlPoolIdleTimeoutMs() {
        return mysqlPoolIdleTimeoutMs;
    }

    public void setMysqlPoolIdleTimeoutMs(long mysqlPoolIdleTimeoutMs) {
        this.mysqlPoolIdleTimeoutMs = mysqlPoolIdleTimeoutMs;
    }

    public long getMysqlPoolMaxLifetimeMs() {
        return mysqlPoolMaxLifetimeMs;
    }

    public void setMysqlPoolMaxLifetimeMs(long mysqlPoolMaxLifetimeMs) {
        this.mysqlPoolMaxLifetimeMs = mysqlPoolMaxLifetimeMs;
    }

    public String getPostgresqlUrl() {
        return postgresqlUrl;
    }

    public void setPostgresqlUrl(String postgresqlUrl) {
        this.postgresqlUrl = postgresqlUrl;
    }

    public String getPostgresqlUser() {
        return postgresqlUser;
    }

    public void setPostgresqlUser(String postgresqlUser) {
        this.postgresqlUser = postgresqlUser;
    }

    public String getPostgresqlPassword() {
        return postgresqlPassword;
    }

    public void setPostgresqlPassword(String postgresqlPassword) {
        this.postgresqlPassword = postgresqlPassword;
    }

    public String getPostgresqlQuery() {
        return postgresqlQuery;
    }

    public void setPostgresqlQuery(String postgresqlQuery) {
        this.postgresqlQuery = postgresqlQuery;
    }

    public int getPostgresqlPoolMinIdle() {
        return postgresqlPoolMinIdle;
    }

    public void setPostgresqlPoolMinIdle(int postgresqlPoolMinIdle) {
        this.postgresqlPoolMinIdle = postgresqlPoolMinIdle;
    }

    public int getPostgresqlPoolMaxSize() {
        return postgresqlPoolMaxSize;
    }

    public void setPostgresqlPoolMaxSize(int postgresqlPoolMaxSize) {
        this.postgresqlPoolMaxSize = postgresqlPoolMaxSize;
    }

    public long getPostgresqlPoolConnectionTimeoutMs() {
        return postgresqlPoolConnectionTimeoutMs;
    }

    public void setPostgresqlPoolConnectionTimeoutMs(long postgresqlPoolConnectionTimeoutMs) {
        this.postgresqlPoolConnectionTimeoutMs = postgresqlPoolConnectionTimeoutMs;
    }

    public long getPostgresqlPoolIdleTimeoutMs() {
        return postgresqlPoolIdleTimeoutMs;
    }

    public void setPostgresqlPoolIdleTimeoutMs(long postgresqlPoolIdleTimeoutMs) {
        this.postgresqlPoolIdleTimeoutMs = postgresqlPoolIdleTimeoutMs;
    }

    public long getPostgresqlPoolMaxLifetimeMs() {
        return postgresqlPoolMaxLifetimeMs;
    }

    public void setPostgresqlPoolMaxLifetimeMs(long postgresqlPoolMaxLifetimeMs) {
        this.postgresqlPoolMaxLifetimeMs = postgresqlPoolMaxLifetimeMs;
    }
}
