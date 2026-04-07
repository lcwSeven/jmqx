package com.jmqx.bridge;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class BridgeProperties {
    private boolean enabled = false;
    private String types = "";

    private boolean async = true;
    private int asyncQueueCapacity = 10000;
    private int asyncWorkerCount = 1;

    private String kafkaBootstrapServers = "127.0.0.1:9092";
    private String kafkaTopic = "jmqx-messages";
    private String kafkaAcks = "1";
    private String kafkaClientId = "jmqx-bridge";
    private String kafkaCompressionType = "none";

    private String rocketmqNameServer = "127.0.0.1:9876";
    private String rocketmqProducerGroup = "jmqx-bridge-group";
    private String rocketmqTopic = "JMQX_MESSAGES";
    private boolean rocketmqSyncSend = false;
    private int rocketmqTimeoutMs = 3000;

    private String mysqlDriver = "";
    private String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/jmqx";
    private String mysqlUser = "root";
    private String mysqlPassword = "";
    private String mysqlTable = "jmqx_bridge_message";
    private boolean mysqlAutoCreateTable = true;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getTypes() {
        return types;
    }

    public void setTypes(String types) {
        this.types = types;
    }

    public boolean isAsync() {
        return async;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public int getAsyncQueueCapacity() {
        return asyncQueueCapacity;
    }

    public void setAsyncQueueCapacity(int asyncQueueCapacity) {
        this.asyncQueueCapacity = asyncQueueCapacity;
    }

    public int getAsyncWorkerCount() {
        return asyncWorkerCount;
    }

    public void setAsyncWorkerCount(int asyncWorkerCount) {
        this.asyncWorkerCount = asyncWorkerCount;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getKafkaAcks() {
        return kafkaAcks;
    }

    public void setKafkaAcks(String kafkaAcks) {
        this.kafkaAcks = kafkaAcks;
    }

    public String getKafkaClientId() {
        return kafkaClientId;
    }

    public void setKafkaClientId(String kafkaClientId) {
        this.kafkaClientId = kafkaClientId;
    }

    public String getKafkaCompressionType() {
        return kafkaCompressionType;
    }

    public void setKafkaCompressionType(String kafkaCompressionType) {
        this.kafkaCompressionType = kafkaCompressionType;
    }

    public String getRocketmqNameServer() {
        return rocketmqNameServer;
    }

    public void setRocketmqNameServer(String rocketmqNameServer) {
        this.rocketmqNameServer = rocketmqNameServer;
    }

    public String getRocketmqProducerGroup() {
        return rocketmqProducerGroup;
    }

    public void setRocketmqProducerGroup(String rocketmqProducerGroup) {
        this.rocketmqProducerGroup = rocketmqProducerGroup;
    }

    public String getRocketmqTopic() {
        return rocketmqTopic;
    }

    public void setRocketmqTopic(String rocketmqTopic) {
        this.rocketmqTopic = rocketmqTopic;
    }

    public boolean isRocketmqSyncSend() {
        return rocketmqSyncSend;
    }

    public void setRocketmqSyncSend(boolean rocketmqSyncSend) {
        this.rocketmqSyncSend = rocketmqSyncSend;
    }

    public int getRocketmqTimeoutMs() {
        return rocketmqTimeoutMs;
    }

    public void setRocketmqTimeoutMs(int rocketmqTimeoutMs) {
        this.rocketmqTimeoutMs = rocketmqTimeoutMs;
    }

    public String getMysqlDriver() {
        return mysqlDriver;
    }

    public void setMysqlDriver(String mysqlDriver) {
        this.mysqlDriver = mysqlDriver;
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

    public String getMysqlTable() {
        return mysqlTable;
    }

    public void setMysqlTable(String mysqlTable) {
        this.mysqlTable = mysqlTable;
    }

    public boolean isMysqlAutoCreateTable() {
        return mysqlAutoCreateTable;
    }

    public void setMysqlAutoCreateTable(boolean mysqlAutoCreateTable) {
        this.mysqlAutoCreateTable = mysqlAutoCreateTable;
    }
}
