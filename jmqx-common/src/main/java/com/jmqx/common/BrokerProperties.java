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
}
