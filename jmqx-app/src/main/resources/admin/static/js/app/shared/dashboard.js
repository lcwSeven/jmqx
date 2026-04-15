export const dashboardMethods = {
    resolveDashboardWsUrl() {
        const protocol = window.location.protocol === "https:" ? "wss" : "ws";
        return protocol + "://" + window.location.hostname + ":8083/mqtt";
    },
    connectDashboardStream() {
        const mqttWsUrl = this.resolveDashboardWsUrl();
        if (!window.mqtt || !mqttWsUrl) {
            this.mqttStatus = "MQTT 库不可用";
            return;
        }
        this.disconnectDashboardStream();
        this.mqttStatus = "连接中";
        const clientId = "admin-" + Math.random().toString(16).slice(2, 10);
        const client = window.mqtt.connect(mqttWsUrl, {
            clientId,
            username: this.adminLoginForm.username || undefined,
            password: this.adminLoginForm.password || undefined,
            reconnectPeriod: 2000,
            clean: true,
            connectTimeout: 5000
        });
        this.mqttClient = client;
        client.on("connect", () => {
            this.mqttStatus = "已连接";
            this.resubscribeDashboardTopics();
        });
        client.on("reconnect", () => {
            this.mqttStatus = "重连中";
        });
        client.on("error", err => {
            this.mqttStatus = "连接异常";
            this.error = "Dashboard 实时通道异常: " + (err?.message || "unknown");
        });
        client.on("close", () => {
            this.mqttStatus = "已断开";
        });
        client.on("message", (topic, payload) => {
            this.onDashboardMessage(topic, payload);
        });
    },
    disconnectDashboardStream() {
        if (this.mqttClient) {
            try {
                this.mqttClient.end(true);
            } catch (e) {
                // ignore
            }
            this.mqttClient = null;
        }
        this.mqttStatus = "未连接";
    },
    resubscribeDashboardTopics() {
        if (!this.mqttClient || !this.mqttClient.connected) {
            return;
        }
        const prefix = "$SYS/dashboard/" + this.currentClusterId + "/";
        this.mqttClient.subscribe(prefix + "cluster/overview", { qos: 0 });
        this.mqttClient.subscribe(prefix + "client/connected", { qos: 0 });
        this.mqttClient.subscribe(prefix + "client/disconnected", { qos: 0 });
    },
    onDashboardMessage(topic, payload) {
        const text = payload ? payload.toString() : "";
        let data;
        try {
            data = text ? JSON.parse(text) : {};
        } catch (e) {
            return;
        }
        const prefix = "$SYS/dashboard/" + this.currentClusterId + "/";
        if (topic === prefix + "cluster/overview") {
            const nodeId = data.nodeId || "unknown";
            this.realtimeNodeMap[nodeId] = {
                nodeId,
                role: data.role || data.nodeRole || "UNKNOWN",
                nodeIp: data.nodeIp || "unknown",
                connectedClients: Number(data.connections || 0),
                inboundBytes: Number(data.inboundBytes || 0),
                outboundBytes: Number(data.outboundBytes || 0),
                connectAuthSuccess: Number(data.connectAuthSuccess || 0),
                connectAuthFailure: Number(data.connectAuthFailure || 0),
                connectAuthError: Number(data.connectAuthError || 0),
                connectAuthSlow: Number(data.connectAuthSlow || 0),
                connectAuthAvgMs: Number(data.connectAuthAvgMs || 0),
                connectAuthMaxMs: Number(data.connectAuthMaxMs || 0),
                publishAclAllow: Number(data.publishAclAllow || 0),
                publishAclDeny: Number(data.publishAclDeny || 0),
                publishAclError: Number(data.publishAclError || 0),
                publishAclSlow: Number(data.publishAclSlow || 0),
                publishAclAvgMs: Number(data.publishAclAvgMs || 0),
                publishAclMaxMs: Number(data.publishAclMaxMs || 0),
                lastReportTime: Number(data.timestamp || Date.now())
            };
            this.recalculateOverviewFromRealtimeNodes();
            return;
        }
        if (topic === prefix + "client/connected" || topic === prefix + "client/disconnected") {
            this.applyRealtimeClientEvent(data);
            if (this.refreshClientsTimer) {
                clearTimeout(this.refreshClientsTimer);
            }
            this.refreshClientsTimer = setTimeout(() => {
                this.queryClients();
                this.refreshClientsTimer = null;
            }, 500);
        }
    }
    ,
    recalculateOverviewFromRealtimeNodes() {
        const nodes = Object.values(this.realtimeNodeMap || {});
        this.overview.nodes = nodes;
        this.overview.totalConnections = nodes.reduce((acc, node) => acc + Number(node.connectedClients || 0), 0);
        this.overview.totalInboundBytes = nodes.reduce((acc, node) => acc + Number(node.inboundBytes || 0), 0);
        this.overview.totalOutboundBytes = nodes.reduce((acc, node) => acc + Number(node.outboundBytes || 0), 0);
        this.overview.totalConnectAuthFailure = nodes.reduce((acc, node) => acc + Number(node.connectAuthFailure || 0), 0);
        this.overview.totalConnectAuthError = nodes.reduce((acc, node) => acc + Number(node.connectAuthError || 0), 0);
        this.overview.totalPublishAclDeny = nodes.reduce((acc, node) => acc + Number(node.publishAclDeny || 0), 0);
        this.overview.totalPublishAclError = nodes.reduce((acc, node) => acc + Number(node.publishAclError || 0), 0);
        this.overview.maxConnectAuthMs = nodes.reduce((acc, node) => Math.max(acc, Number(node.connectAuthMaxMs || 0)), 0);
        this.overview.maxPublishAclMs = nodes.reduce((acc, node) => Math.max(acc, Number(node.publishAclMaxMs || 0)), 0);
    }
};
