package com.jmqx.bridge;

/**
 * @author liucaiwen
 * @date 2026/4/7
 */
public class BridgeProperties {
    private boolean enabled = false;
    private String types = "";
    private String topicFilters = "";

    private boolean async = true;
    private int asyncQueueCapacity = 10000;
    private int asyncWorkerCount = 1;

    private String kafkaBootstrapServers = "127.0.0.1:9092";
    private String kafkaTopic = "jmqx-messages";
    private String kafkaSourceTopicFilters = "";
    private String kafkaAcks = "1";
    private String kafkaClientId = "jmqx-bridge";
    private String kafkaCompressionType = "none";

    private String rocketmqNameServer = "127.0.0.1:9876";
    private String rocketmqProducerGroup = "jmqx-bridge-group";
    private String rocketmqTopic = "JMQX_MESSAGES";
    private String rocketmqSourceTopicFilters = "";
    private boolean rocketmqSyncSend = false;
    private int rocketmqTimeoutMs = 3000;

    private String mysqlDriver = "";
    private String mysqlUrl = "jdbc:mysql://127.0.0.1:3306/jmqx";
    private String mysqlUser = "root";
    private String mysqlPassword = "";
    private String mysqlTable = "jmqx_bridge_message";
    private String mysqlSourceTopicFilters = "";
    private boolean mysqlAutoCreateTable = true;
    private int mysqlPoolMinIdle = 1;
    private int mysqlPoolMaxSize = 8;
    private long mysqlPoolConnectionTimeoutMs = 3000;
    private long mysqlPoolIdleTimeoutMs = 60_000;
    private long mysqlPoolMaxLifetimeMs = 600_000;

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

    public String getTopicFilters() {
        return topicFilters;
    }

    public void setTopicFilters(String topicFilters) {
        this.topicFilters = topicFilters;
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

    public String getKafkaSourceTopicFilters() {
        return kafkaSourceTopicFilters;
    }

    public void setKafkaSourceTopicFilters(String kafkaSourceTopicFilters) {
        this.kafkaSourceTopicFilters = kafkaSourceTopicFilters;
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

    public String getRocketmqSourceTopicFilters() {
        return rocketmqSourceTopicFilters;
    }

    public void setRocketmqSourceTopicFilters(String rocketmqSourceTopicFilters) {
        this.rocketmqSourceTopicFilters = rocketmqSourceTopicFilters;
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

    public String getMysqlSourceTopicFilters() {
        return mysqlSourceTopicFilters;
    }

    public void setMysqlSourceTopicFilters(String mysqlSourceTopicFilters) {
        this.mysqlSourceTopicFilters = mysqlSourceTopicFilters;
    }

    public boolean isMysqlAutoCreateTable() {
        return mysqlAutoCreateTable;
    }

    public void setMysqlAutoCreateTable(boolean mysqlAutoCreateTable) {
        this.mysqlAutoCreateTable = mysqlAutoCreateTable;
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
}
