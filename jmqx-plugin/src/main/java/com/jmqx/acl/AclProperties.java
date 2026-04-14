package com.jmqx.acl;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class AclProperties {
    private String chain = "";
    private boolean defaultAllow = false;
    private int cacheMillis = 60_000;

    private String httpUrl = "";
    private int httpTimeoutMs = 2000;
    private String httpBodyTemplate = """
            {
              "clientId": "${clientId}",
              "username": "${username}",
              "topic": "${topic}",
              "action": "${action}"
            }
            """;

    private String redisHost = "127.0.0.1";
    private int redisPort = 6379;
    private String redisPassword = "";
    private int redisDb = 0;
    private String redisKeyPrefix = "jmqx:acl";
    private int redisTimeoutMs = 2000;

    private String filePath = "acl-rules.txt";

    public String getChain() {
        return chain;
    }

    public void setChain(String chain) {
        this.chain = chain;
    }

    public boolean isDefaultAllow() {
        return defaultAllow;
    }

    public void setDefaultAllow(boolean defaultAllow) {
        this.defaultAllow = defaultAllow;
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

    public String getHttpBodyTemplate() {
        return httpBodyTemplate;
    }

    public void setHttpBodyTemplate(String httpBodyTemplate) {
        this.httpBodyTemplate = httpBodyTemplate;
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

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
