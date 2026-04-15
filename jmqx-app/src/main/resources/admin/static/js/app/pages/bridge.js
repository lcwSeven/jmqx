import { fetchBridgeConfig, saveBridgeConfig } from "../../api.js";

export const bridgePageMethods = {
    async loadBridgeConfig() {
        const config = await fetchBridgeConfig(this.currentClusterId);
        this.bridgeConfig = this.normalizeBridgeConfig(config);
    },
    async saveBridgeConfig() {
        try {
            const payload = this.normalizeBridgeConfig(this.bridgeConfig);
            payload.types = this.toCommaList(payload.types);
            payload.topicFilters = this.toLineList(payload.topicFilters);
            payload.kafka.sourceTopicFilters = this.toLineList(payload.kafka.sourceTopicFilters);
            payload.rocketmq.sourceTopicFilters = this.toLineList(payload.rocketmq.sourceTopicFilters);
            payload.mysql.sourceTopicFilters = this.toLineList(payload.mysql.sourceTopicFilters);
            await saveBridgeConfig(this.currentClusterId, payload);
            this.bridgeConfig = this.normalizeBridgeConfig(payload);
            await this.loadAuditLogs();
            this.message = "桥接配置保存成功";
            this.error = "";
        } catch (e) {
            this.error = "保存桥接配置失败: " + e.message;
        }
    },
    normalizeBridgeConfig(config = {}) {
        const normalizeList = (value) => (Array.isArray(value) ? value : this.toCommaList(value || ""))
            .map(item => String(item || "").trim())
            .filter(Boolean);
        return {
            enabled: config.enabled === true,
            types: normalizeList(config.types),
            topicFilters: Array.isArray(config.topicFilters) ? config.topicFilters : this.toLineList(config.topicFilters || ""),
            asyncEnabled: config.asyncEnabled !== false,
            asyncQueueCapacity: Number(config.asyncQueueCapacity || 10000),
            asyncWorkerCount: Number(config.asyncWorkerCount || 1),
            kafka: {
                enabled: config.kafka?.enabled === true,
                bootstrapServers: config.kafka?.bootstrapServers || "127.0.0.1:9092",
                topic: config.kafka?.topic || "jmqx-messages",
                sourceTopicFilters: Array.isArray(config.kafka?.sourceTopicFilters) ? config.kafka.sourceTopicFilters : this.toLineList(config.kafka?.sourceTopicFilters || ""),
                acks: config.kafka?.acks || "1",
                clientId: config.kafka?.clientId || "jmqx-bridge",
                compressionType: config.kafka?.compressionType || "none"
            },
            rocketmq: {
                enabled: config.rocketmq?.enabled === true,
                nameServer: config.rocketmq?.nameServer || "127.0.0.1:9876",
                producerGroup: config.rocketmq?.producerGroup || "jmqx-bridge-group",
                topic: config.rocketmq?.topic || "JMQX_MESSAGES",
                sourceTopicFilters: Array.isArray(config.rocketmq?.sourceTopicFilters) ? config.rocketmq.sourceTopicFilters : this.toLineList(config.rocketmq?.sourceTopicFilters || ""),
                syncSend: config.rocketmq?.syncSend === true,
                timeoutMs: Number(config.rocketmq?.timeoutMs || 3000)
            },
            mysql: {
                enabled: config.mysql?.enabled === true,
                driver: config.mysql?.driver || "",
                url: config.mysql?.url || "jdbc:mysql://127.0.0.1:3306/jmqx",
                user: config.mysql?.user || "root",
                password: config.mysql?.password || "",
                table: config.mysql?.table || "jmqx_bridge_message",
                sourceTopicFilters: Array.isArray(config.mysql?.sourceTopicFilters) ? config.mysql.sourceTopicFilters : this.toLineList(config.mysql?.sourceTopicFilters || ""),
                autoCreateTable: config.mysql?.autoCreateTable !== false,
                poolMinIdle: Number(config.mysql?.poolMinIdle || 1),
                poolMaxSize: Number(config.mysql?.poolMaxSize || 8),
                poolConnectionTimeoutMs: Number(config.mysql?.poolConnectionTimeoutMs || 3000),
                poolIdleTimeoutMs: Number(config.mysql?.poolIdleTimeoutMs || 60000),
                poolMaxLifetimeMs: Number(config.mysql?.poolMaxLifetimeMs || 600000)
            }
        };
    },
    bridgePluginDisplayName(type) {
        if (type === "rocketmq") {
            return "RocketMQ";
        }
        if (type === "mysql") {
            return "MySQL";
        }
        return "Kafka";
    },
    describeBridgePlugin(type) {
        if (type === "rocketmq") {
            const config = this.bridgeConfig.rocketmq || {};
            return `${config.nameServer || "-"} -> ${config.topic || "-"}`;
        }
        if (type === "mysql") {
            const config = this.bridgeConfig.mysql || {};
            return `${config.url || "-"} -> ${config.table || "-"}`;
        }
        const config = this.bridgeConfig.kafka || {};
        return `${config.bootstrapServers || "-"} -> ${config.topic || "-"}`;
    },
    bridgeEntryEnabled(type) {
        if (type === "rocketmq") {
            return this.bridgeConfig.rocketmq?.enabled === true;
        }
        if (type === "mysql") {
            return this.bridgeConfig.mysql?.enabled === true;
        }
        return this.bridgeConfig.kafka?.enabled === true;
    },
    bridgeDatasourceLabel() {
        const hit = this.bridgeDatasourceOptions.find(item => item.key === this.bridgeDraft.datasource);
        return hit ? hit.label : "Kafka";
    },
    openBridgeCreate() {
        if (!this.availableBridgeDatasourceOptions.length) {
            this.error = "可用的桥接器类型已经全部创建";
            return;
        }
        this.bridgeCreateMode = true;
        this.bridgeEditingType = "";
        this.bridgeStep = 1;
        this.applyBridgeDraftFromType(this.availableBridgeDatasourceOptions[0].key);
    },
    cancelBridgeCreate() {
        this.bridgeCreateMode = false;
        this.bridgeEditingType = "";
        this.bridgeStep = 1;
    },
    selectBridgeDatasource(key) {
        this.applyBridgeDraftFromType(key);
    },
    previousBridgeStep() {
        if (this.bridgeStep > 1) {
            this.bridgeStep -= 1;
        }
    },
    nextBridgeStep() {
        if (this.bridgeStep < 2) {
            this.bridgeStep += 1;
        }
    },
    async saveBridgeGlobalConfig() {
        await this.saveBridgeConfig();
    },
    async toggleBridgeEntryEnabled(type, enabled) {
        if (type === "rocketmq") {
            this.bridgeConfig.rocketmq.enabled = enabled === true;
        } else if (type === "mysql") {
            this.bridgeConfig.mysql.enabled = enabled === true;
        } else {
            this.bridgeConfig.kafka.enabled = enabled === true;
        }
        this.bridgeConfig.enabled = this.bridgeConfig.kafka.enabled || this.bridgeConfig.rocketmq.enabled || this.bridgeConfig.mysql.enabled;
        await this.saveBridgeConfig();
    },
    openBridgeSettings(type) {
        const targetType = type || (this.bridgeEntries[0] ? this.bridgeEntries[0].type : "kafka");
        this.bridgeCreateMode = true;
        this.bridgeEditingType = targetType;
        this.bridgeStep = 2;
        this.applyBridgeDraftFromType(targetType);
    },
    async createBridgeAndSave() {
        try {
            const type = String(this.bridgeDraft.datasource || "").trim().toLowerCase();
            if (!type) {
                this.error = "请选择桥接器类型";
                return;
            }
            if (!this.bridgeEditingType && this.bridgeConfig.types.includes(type)) {
                this.error = "同一种桥接器已存在，请直接编辑该项配置";
                return;
            }
            this.applyBridgeDraftToBridgeConfig(type);
            const nextTypes = Array.isArray(this.bridgeConfig.types) ? [...this.bridgeConfig.types] : [];
            if (!nextTypes.includes(type)) {
                nextTypes.push(type);
            }
            this.bridgeConfig.types = nextTypes;
            this.bridgeConfig.enabled = this.bridgeConfig.kafka.enabled || this.bridgeConfig.rocketmq.enabled || this.bridgeConfig.mysql.enabled;
            await this.saveBridgeConfig();
            this.bridgeCreateMode = false;
            this.bridgeEditingType = "";
            this.bridgeStep = 1;
            this.message = "桥接器配置已保存";
            this.error = "";
        } catch (e) {
            this.error = "保存桥接器配置失败: " + e.message;
        }
    },
    async removeBridgeEntry(type) {
        try {
            this.bridgeConfig.types = (Array.isArray(this.bridgeConfig.types) ? this.bridgeConfig.types : [])
                .filter(item => item !== type);
            if (type === "rocketmq") {
                this.bridgeConfig.rocketmq.enabled = false;
            } else if (type === "mysql") {
                this.bridgeConfig.mysql.enabled = false;
            } else {
                this.bridgeConfig.kafka.enabled = false;
            }
            this.bridgeConfig.enabled = this.bridgeConfig.kafka.enabled || this.bridgeConfig.rocketmq.enabled || this.bridgeConfig.mysql.enabled;
            await this.saveBridgeConfig();
            this.message = "桥接器已移除";
            this.error = "";
        } catch (e) {
            this.error = "删除桥接器失败: " + e.message;
        }
    },
    applyBridgeDraftFromType(type) {
        const normalizedType = String(type || "kafka").trim().toLowerCase() || "kafka";
        const config = this.normalizeBridgeConfig(this.bridgeConfig);
        this.bridgeDraft = {
            datasource: normalizedType,
            kafka: { ...config.kafka },
            rocketmq: { ...config.rocketmq },
            mysql: { ...config.mysql }
        };
        if (!this.bridgeEditingType) {
            if (normalizedType === "rocketmq") {
                this.bridgeDraft.rocketmq.enabled = true;
            } else if (normalizedType === "mysql") {
                this.bridgeDraft.mysql.enabled = true;
            } else {
                this.bridgeDraft.kafka.enabled = true;
            }
        }
    },
    applyBridgeDraftToBridgeConfig(type) {
        const normalizedType = String(type || "kafka").trim().toLowerCase() || "kafka";
        const nextConfig = this.normalizeBridgeConfig({
            ...this.bridgeConfig,
            kafka: this.bridgeDraft.kafka,
            rocketmq: this.bridgeDraft.rocketmq,
            mysql: this.bridgeDraft.mysql
        });
        nextConfig.types = Array.isArray(this.bridgeConfig.types) ? [...this.bridgeConfig.types] : [];
        if (!nextConfig.types.includes(normalizedType)) {
            nextConfig.types.push(normalizedType);
        }
        nextConfig.enabled = nextConfig.kafka.enabled || nextConfig.rocketmq.enabled || nextConfig.mysql.enabled;
        this.bridgeConfig = nextConfig;
    }
};
