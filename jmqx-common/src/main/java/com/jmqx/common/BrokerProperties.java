package com.jmqx.common;

/**
 * @author liucaiwen
 * @date 2026/4/2
 */
public class BrokerProperties {
    private String host = "localhost";
    private int port = 1883;
    private int bossThreads = 1;
    private int workerThreads = 0;
    private int readerIdleSeconds = 120;
    private boolean websocketEnabled = true;
    private String websocketHost = "0.0.0.0";
    private int websocketPort = 8083;
    private String websocketPath = "/mqtt";

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
}
