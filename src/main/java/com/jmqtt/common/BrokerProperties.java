/**
 * @author liucaiwen
 * @date 2026/4/2
 */
package com.jmqtt.common;

public class BrokerProperties {
    private String host = "0.0.0.0";
    private int port = 1883;
    private int bossThreads = 1;
    private int workerThreads = 0;

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
}
