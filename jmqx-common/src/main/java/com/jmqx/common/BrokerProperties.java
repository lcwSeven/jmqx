package com.jmqx.common;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class BrokerProperties {
    private String host = "localhost";
    private int port = 1883;
    private boolean mqttsEnabled = false;
    private String mqttsHost = "0.0.0.0";
    private int mqttsPort = 8883;
    private int bossThreads = 1;
    private int workerThreads = 0;
    private int readerIdleSeconds = 120;
    private int maxQos = 2;
    private int maxWillPayloadBytes = 1024 * 1024;
    private boolean websocketEnabled = true;
    private String websocketHost = "0.0.0.0";
    private int websocketPort = 8083;
    private String websocketPath = "/mqtt";
    private boolean wssEnabled = false;
    private String wssHost = "0.0.0.0";
    private int wssPort = 8084;
    private String wssPath = "/mqtt";
    private String tlsCertChainFile = "";
    private String tlsPrivateKeyFile = "";
    private String tlsPrivateKeyPassword = "";
    private boolean rateLimitClientIdEnabled = false;
    private int rateLimitClientIdPerSecond = 0;
    private boolean rateLimitIpEnabled = false;
    private int rateLimitIpPerSecond = 0;
    private String rateLimitPublishStrategy = "fixed_window";
    private boolean rateLimitConnectEnabled = false;
    private int rateLimitConnectGlobalPerSecond = 0;
    private int rateLimitConnectIpPerSecond = 0;
    private String rateLimitConnectStrategy = "fixed_window";
    private int rateLimitCleanupIntervalSeconds = 60;
    private int rateLimitIdleSeconds = 300;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isMqttsEnabled() {
        return mqttsEnabled;
    }

    public void setMqttsEnabled(boolean mqttsEnabled) {
        this.mqttsEnabled = mqttsEnabled;
    }

    public String getMqttsHost() {
        return mqttsHost;
    }

    public void setMqttsHost(String mqttsHost) {
        this.mqttsHost = mqttsHost;
    }

    public int getMqttsPort() {
        return mqttsPort;
    }

    public void setMqttsPort(int mqttsPort) {
        this.mqttsPort = mqttsPort;
    }

    public int getBossThreads() {
        return bossThreads;
    }

    public void setBossThreads(int bossThreads) {
        this.bossThreads = bossThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public int getReaderIdleSeconds() {
        return readerIdleSeconds;
    }

    public void setReaderIdleSeconds(int readerIdleSeconds) {
        this.readerIdleSeconds = readerIdleSeconds;
    }

    public int getMaxQos() {
        return maxQos;
    }

    public void setMaxQos(int maxQos) {
        this.maxQos = maxQos;
    }

    public int getMaxWillPayloadBytes() {
        return maxWillPayloadBytes;
    }

    public void setMaxWillPayloadBytes(int maxWillPayloadBytes) {
        this.maxWillPayloadBytes = maxWillPayloadBytes;
    }

    public boolean isWebsocketEnabled() {
        return websocketEnabled;
    }

    public void setWebsocketEnabled(boolean websocketEnabled) {
        this.websocketEnabled = websocketEnabled;
    }

    public String getWebsocketHost() {
        return websocketHost;
    }

    public void setWebsocketHost(String websocketHost) {
        this.websocketHost = websocketHost;
    }

    public int getWebsocketPort() {
        return websocketPort;
    }

    public void setWebsocketPort(int websocketPort) {
        this.websocketPort = websocketPort;
    }

    public String getWebsocketPath() {
        return websocketPath;
    }

    public void setWebsocketPath(String websocketPath) {
        this.websocketPath = websocketPath;
    }

    public boolean isWssEnabled() {
        return wssEnabled;
    }

    public void setWssEnabled(boolean wssEnabled) {
        this.wssEnabled = wssEnabled;
    }

    public String getWssHost() {
        return wssHost;
    }

    public void setWssHost(String wssHost) {
        this.wssHost = wssHost;
    }

    public int getWssPort() {
        return wssPort;
    }

    public void setWssPort(int wssPort) {
        this.wssPort = wssPort;
    }

    public String getWssPath() {
        return wssPath;
    }

    public void setWssPath(String wssPath) {
        this.wssPath = wssPath;
    }

    public String getTlsCertChainFile() {
        return tlsCertChainFile;
    }

    public void setTlsCertChainFile(String tlsCertChainFile) {
        this.tlsCertChainFile = tlsCertChainFile;
    }

    public String getTlsPrivateKeyFile() {
        return tlsPrivateKeyFile;
    }

    public void setTlsPrivateKeyFile(String tlsPrivateKeyFile) {
        this.tlsPrivateKeyFile = tlsPrivateKeyFile;
    }

    public String getTlsPrivateKeyPassword() {
        return tlsPrivateKeyPassword;
    }

    public void setTlsPrivateKeyPassword(String tlsPrivateKeyPassword) {
        this.tlsPrivateKeyPassword = tlsPrivateKeyPassword;
    }

    public boolean isRateLimitClientIdEnabled() {
        return rateLimitClientIdEnabled;
    }

    public void setRateLimitClientIdEnabled(boolean rateLimitClientIdEnabled) {
        this.rateLimitClientIdEnabled = rateLimitClientIdEnabled;
    }

    public int getRateLimitClientIdPerSecond() {
        return rateLimitClientIdPerSecond;
    }

    public void setRateLimitClientIdPerSecond(int rateLimitClientIdPerSecond) {
        this.rateLimitClientIdPerSecond = rateLimitClientIdPerSecond;
    }

    public boolean isRateLimitIpEnabled() {
        return rateLimitIpEnabled;
    }

    public void setRateLimitIpEnabled(boolean rateLimitIpEnabled) {
        this.rateLimitIpEnabled = rateLimitIpEnabled;
    }

    public int getRateLimitIpPerSecond() {
        return rateLimitIpPerSecond;
    }

    public void setRateLimitIpPerSecond(int rateLimitIpPerSecond) {
        this.rateLimitIpPerSecond = rateLimitIpPerSecond;
    }

    public String getRateLimitPublishStrategy() {
        return rateLimitPublishStrategy;
    }

    public void setRateLimitPublishStrategy(String rateLimitPublishStrategy) {
        this.rateLimitPublishStrategy = rateLimitPublishStrategy;
    }

    public boolean isRateLimitConnectEnabled() {
        return rateLimitConnectEnabled;
    }

    public void setRateLimitConnectEnabled(boolean rateLimitConnectEnabled) {
        this.rateLimitConnectEnabled = rateLimitConnectEnabled;
    }

    public int getRateLimitConnectGlobalPerSecond() {
        return rateLimitConnectGlobalPerSecond;
    }

    public void setRateLimitConnectGlobalPerSecond(int rateLimitConnectGlobalPerSecond) {
        this.rateLimitConnectGlobalPerSecond = rateLimitConnectGlobalPerSecond;
    }

    public int getRateLimitConnectIpPerSecond() {
        return rateLimitConnectIpPerSecond;
    }

    public void setRateLimitConnectIpPerSecond(int rateLimitConnectIpPerSecond) {
        this.rateLimitConnectIpPerSecond = rateLimitConnectIpPerSecond;
    }

    public String getRateLimitConnectStrategy() {
        return rateLimitConnectStrategy;
    }

    public void setRateLimitConnectStrategy(String rateLimitConnectStrategy) {
        this.rateLimitConnectStrategy = rateLimitConnectStrategy;
    }

    public int getRateLimitCleanupIntervalSeconds() {
        return rateLimitCleanupIntervalSeconds;
    }

    public void setRateLimitCleanupIntervalSeconds(int rateLimitCleanupIntervalSeconds) {
        this.rateLimitCleanupIntervalSeconds = rateLimitCleanupIntervalSeconds;
    }

    public int getRateLimitIdleSeconds() {
        return rateLimitIdleSeconds;
    }

    public void setRateLimitIdleSeconds(int rateLimitIdleSeconds) {
        this.rateLimitIdleSeconds = rateLimitIdleSeconds;
    }
}
