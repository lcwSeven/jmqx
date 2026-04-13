import {
    fetchClientDetail,
    fetchClients,
    fetchClusterConfig,
    fetchOverview,
    fetchSecurityConfig,
    saveClusterConfig,
    saveSecurityConfig
} from "./api.js";

const { createApp } = Vue;

createApp({
    data() {
        return {
            activeMenu: "overview",
            message: "",
            error: "",
            currentClusterId: "default",
            mqttStatus: "未连接",
            mqttClient: null,
            realtimeNodeMap: {},
            refreshClientsTimer: null,
            overview: { totalConnections: 0, totalInboundBytes: 0, totalOutboundBytes: 0, nodes: [] },
            clients: { records: [], total: 0, pageNo: 1, pageSize: 20 },
            search: { clientId: "", userName: "", pageNo: 1, pageSize: 20 },
            selectedClient: null,
            securityConfig: { aclEnabled: true, aclChain: ["file"], authEnabled: true, authChain: ["file"], cacheTtlMs: 60000 },
            clusterConfig: { coreNodes: ["core-1:9801"], replicantNodes: [], coreAcceptClientConnections: true, sharedSubscriptionMaxMembersPerGroup: 10000 },
            authCreateMode: false,
            authStep: 1,
            authMethodOptions: [
                { key: "password", label: "Password-Base" }
            ],
            authDatasourceOptions: [
                { key: "builtin", label: "内置数据库", icon: "🗄️" },
                { key: "mysql", label: "MySQL", icon: "🐬" },
                { key: "redis", label: "Redis", icon: "🧱" },
                { key: "http", label: "HTTP 服务", icon: "🌐" }
            ],
            authDraft: {
                method: "password",
                datasource: "builtin",
                cacheTtlMs: 60000,
                filePath: "conf/auth-users.txt",
                httpUrl: "http://127.0.0.1:8080/auth/verify",
                redisHost: "127.0.0.1",
                redisPort: 6379,
                dbUrl: "jdbc:mysql://127.0.0.1:3306/jmqx",
                dbUser: "root",
                dbPassword: ""
            }
        };
    },
    computed: {
        activeMenuLabel() {
            const labels = {
                overview: "集群概览",
                clients: this.selectedClient ? "客户端详情" : "客户端列表",
                acl: "ACL 鉴权",
                auth: this.authCreateMode ? "创建认证" : "连接鉴权",
                cluster: "集群配置"
            };
            return labels[this.activeMenu] || "JMQX Admin";
        },
        activeMenuDescription() {
            const descriptions = {
                overview: "实时查看节点负载、流量和运行态变化。",
                clients: "搜索在线客户端，快速定位连接和订阅信息。",
                acl: "维护主题访问策略与鉴权缓存设置。",
                auth: "统一管理客户端接入认证与数据源配置。",
                cluster: "调整节点角色、共享订阅容量和集群行为。"
            };
            return descriptions[this.activeMenu] || "";
        },
        authStatusText() {
            if (!this.hasAuthRecord()) {
                return "未配置";
            }
            return this.securityConfig.authEnabled ? "已启用" : "已停用";
        },
        authStatusClass() {
            if (!this.hasAuthRecord()) {
                return "is-idle";
            }
            return this.securityConfig.authEnabled ? "is-up" : "is-down";
        },
        aclStatusText() {
            return this.securityConfig.aclEnabled ? "已启用" : "已停用";
        },
        aclStatusClass() {
            return this.securityConfig.aclEnabled ? "is-up" : "is-down";
        },
        totalNodes() {
            return Array.isArray(this.overview.nodes) ? this.overview.nodes.length : 0;
        },
        clientsSummaryText() {
            const total = Number(this.clients.total || 0);
            const keyword = [this.search.clientId, this.search.userName].filter(Boolean).join(" / ");
            return keyword ? `共 ${total} 条结果，筛选条件：${keyword}` : `共 ${total} 个在线客户端`;
        }
    },
    async mounted() {
        await this.reloadCurrentClusterData();
        this.connectDashboardStream();
    },
    beforeUnmount() {
        this.disconnectDashboardStream();
        if (this.refreshClientsTimer) {
            clearTimeout(this.refreshClientsTimer);
            this.refreshClientsTimer = null;
        }
    },
    methods: {
        clearTips() {
            this.message = "";
            this.error = "";
        },
        setMenu(menu) {
            this.activeMenu = menu;
            this.selectedClient = null;
            if (menu !== "auth") {
                this.authCreateMode = false;
                this.authStep = 1;
            }
            this.clearTips();
        },
        async reloadCurrentClusterData() {
            try {
                this.clearTips();
                await Promise.all([
                    this.refreshOverview(),
                    this.queryClients(),
                    this.loadSecurityConfig(),
                    this.loadClusterConfig()
                ]);
            } catch (e) {
                this.error = "加载集群数据失败: " + e.message;
            }
        },
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
            client.on("error", (err) => {
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
                    lastReportTime: Number(data.timestamp || Date.now())
                };
                const nodes = Object.values(this.realtimeNodeMap);
                this.overview.nodes = nodes;
                this.overview.totalConnections = nodes.reduce((acc, node) => acc + (node.connectedClients || 0), 0);
                this.overview.totalInboundBytes = nodes.reduce((acc, node) => acc + (node.inboundBytes || 0), 0);
                this.overview.totalOutboundBytes = nodes.reduce((acc, node) => acc + (node.outboundBytes || 0), 0);
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
        },
        applyRealtimeClientEvent(event) {
            if (!event || !event.clientId) {
                return;
            }
            if (!Array.isArray(this.clients.records)) {
                this.clients.records = [];
            }
            const idx = this.clients.records.findIndex(item => item.clientId === event.clientId);
            if (event.event === "connected") {
                const row = {
                    clientId: event.clientId,
                    nodeId: event.nodeId || "",
                    clientIp: event.clientIp || "",
                    keepAliveSeconds: Number(event.keepAliveSeconds || 0),
                    connectionType: event.connectionType || "",
                    username: event.username || "",
                    connectedAt: Number(event.timestamp || Date.now())
                };
                if (idx >= 0) {
                    this.clients.records.splice(idx, 1, row);
                } else {
                    this.clients.records.unshift(row);
                }
                this.clients.total = Math.max(Number(this.clients.total || 0), this.clients.records.length);
                return;
            }
            if (event.event === "graceful" || event.event === "unexpected" || event.event === "disconnected") {
                if (idx >= 0) {
                    this.clients.records.splice(idx, 1);
                }
                if (this.clients.total > 0) {
                    this.clients.total -= 1;
                }
            }
        },
        async refreshOverview() {
            this.overview = await fetchOverview(this.currentClusterId);
        },
        async queryClients() {
            this.clients = await fetchClients(this.currentClusterId, this.search);
        },
        async viewClient(clientId) {
            try {
                this.selectedClient = await fetchClientDetail(this.currentClusterId, clientId);
            } catch (e) {
                this.error = "加载客户端详情失败: " + e.message;
            }
        },
        async loadSecurityConfig() {
            this.securityConfig = await fetchSecurityConfig(this.currentClusterId);
        },
        async saveSecurityConfig() {
            try {
                const payload = {
                    ...this.securityConfig,
                    aclChain: this.toCommaList(this.securityConfig.aclChain),
                    authChain: this.toCommaList(this.securityConfig.authChain)
                };
                await saveSecurityConfig(this.currentClusterId, payload);
                this.message = "安全配置保存成功";
                this.error = "";
            } catch (e) {
                this.error = "保存安全配置失败: " + e.message;
            }
        },
        async saveAclConfig() {
            await this.saveSecurityConfig();
        },
        async saveAuthConfig() {
            await this.saveSecurityConfig();
        },
        openAuthCreate() {
            this.authCreateMode = true;
            this.authStep = 1;
            this.authDraft.method = "password";
            this.authDraft.cacheTtlMs = Number(this.securityConfig.cacheTtlMs || 60000);
            const primary = Array.isArray(this.securityConfig.authChain)
                ? this.securityConfig.authChain[0]
                : this.securityConfig.authChain;
            this.authDraft.datasource = this.mapPluginToDatasource(primary);
        },
        cancelAuthCreate() {
            this.authCreateMode = false;
            this.authStep = 1;
        },
        selectAuthMethod(key) {
            this.authDraft.method = key;
        },
        selectAuthDatasource(key) {
            this.authDraft.datasource = key;
        },
        previousAuthStep() {
            if (this.authStep > 1) {
                this.authStep -= 1;
            }
        },
        nextAuthStep() {
            if (this.authStep < 3) {
                this.authStep += 1;
            }
        },
        authStepDone(step) {
            return this.authStep > step;
        },
        authMethodLabel() {
            const hit = this.authMethodOptions.find(item => item.key === this.authDraft.method);
            return hit ? hit.label : "Password-Base";
        },
        authDatasourceLabel() {
            const hit = this.authDatasourceOptions.find(item => item.key === this.authDraft.datasource);
            return hit ? hit.label : "内置数据库";
        },
        mapDatasourceToPlugin(datasource) {
            if (datasource === "http") {
                return "http";
            }
            if (datasource === "redis") {
                return "redis";
            }
            if (datasource === "builtin") {
                return "file";
            }
            return "db";
        },
        mapPluginToDatasource(plugin) {
            if (plugin === "http") {
                return "http";
            }
            if (plugin === "redis") {
                return "redis";
            }
            if (plugin === "file") {
                return "builtin";
            }
            return "mysql";
        },
        authPluginDisplayName(plugin) {
            if (plugin === "file") {
                return "内置数据库";
            }
            if (plugin === "http") {
                return "HTTP 服务";
            }
            if (plugin === "redis") {
                return "Redis";
            }
            if (plugin === "db") {
                return "MySQL";
            }
            return plugin || "-";
        },
        primaryAuthPlugin() {
            const chain = Array.isArray(this.securityConfig.authChain)
                ? this.securityConfig.authChain
                : [];
            if (!chain.length || !chain[0]) {
                return "";
            }
            return String(chain[0]).trim().toLowerCase();
        },
        hasAuthRecord() {
            const plugin = this.primaryAuthPlugin();
            return plugin !== "" && plugin !== "allow_all";
        },
        async createAuthAndSave() {
            try {
                const plugin = this.mapDatasourceToPlugin(this.authDraft.datasource);
                this.securityConfig.authEnabled = true;
                this.securityConfig.authChain = [plugin];
                this.securityConfig.cacheTtlMs = Number(this.authDraft.cacheTtlMs || 60000);
                await this.saveAuthConfig();
                this.authCreateMode = false;
                this.authStep = 1;
                this.message = "创建认证成功";
            } catch (e) {
                this.error = "创建认证失败: " + e.message;
            }
        },
        async toggleAuthEnabled() {
            await this.saveAuthConfig();
        },
        openAuthSettings() {
            this.openAuthCreate();
            this.authStep = 3;
        },
        async loadClusterConfig() {
            this.clusterConfig = await fetchClusterConfig(this.currentClusterId);
        },
        async saveClusterConfig() {
            try {
                const payload = {
                    ...this.clusterConfig,
                    coreNodes: this.toLineList(this.clusterConfig.coreNodes),
                    replicantNodes: this.toLineList(this.clusterConfig.replicantNodes)
                };
                await saveClusterConfig(this.currentClusterId, payload);
                this.message = "集群配置保存成功";
                this.error = "";
            } catch (e) {
                this.error = "保存集群配置失败: " + e.message;
            }
        },
        toCommaList(val) {
            if (Array.isArray(val)) {
                return val;
            }
            return String(val || "").split(",").map(s => s.trim()).filter(Boolean);
        },
        toLineList(val) {
            if (Array.isArray(val)) {
                return val;
            }
            return String(val || "").split("\n").map(s => s.trim()).filter(Boolean);
        },
        joinComma(val) {
            return Array.isArray(val) ? val.join(", ") : val;
        },
        joinLine(val) {
            return Array.isArray(val) ? val.join("\n") : val;
        },
        formatDateTime(value) {
            if (value === null || value === undefined || value === "") {
                return "-";
            }
            const timestamp = Number(value);
            if (!Number.isFinite(timestamp) || timestamp <= 0) {
                return "-";
            }
            return new Date(timestamp).toLocaleString(undefined, { hour12: false });
        },
        formatNumber(value) {
            const num = Number(value || 0);
            return Number.isFinite(num) ? num.toLocaleString() : "0";
        },
        nodeHealthLabel(node) {
            const ts = Number(node?.lastReportTime || 0);
            if (!ts) {
                return "未知";
            }
            return Date.now() - ts <= 15000 ? "在线" : "延迟";
        },
        nodeHealthClass(node) {
            const ts = Number(node?.lastReportTime || 0);
            if (!ts) {
                return "is-idle";
            }
            return Date.now() - ts <= 15000 ? "is-up" : "is-warn";
        }
    },
    template: `
      <div class="layout">
        <aside class="sidebar">
          <div class="logo-wrap">
            <div class="logo">JMQX Admin</div>
            <div class="logo-sub">Cluster Console</div>
          </div>
          <div class="menu-title">监控</div>
          <button class="menu-item" :class="{active: activeMenu==='overview'}" @click="setMenu('overview')">集群概览</button>
          <button class="menu-item" :class="{active: activeMenu==='clients'}" @click="setMenu('clients')">客户端列表</button>
          <div class="menu-title">安全策略</div>
          <button class="menu-item" :class="{active: activeMenu==='acl'}" @click="setMenu('acl')">ACL 鉴权</button>
          <button class="menu-item" :class="{active: activeMenu==='auth'}" @click="setMenu('auth')">连接鉴权</button>
          <div class="menu-title">系统配置</div>
          <button class="menu-item" :class="{active: activeMenu==='cluster'}" @click="setMenu('cluster')">集群配置</button>
          <div class="sidebar-footer">
            <div class="sidebar-foot-label">Cluster</div>
            <div class="sidebar-foot-value">{{ currentClusterId }}</div>
            <div class="sidebar-foot-label">Realtime</div>
            <div class="sidebar-status">{{ mqttStatus }}</div>
          </div>
        </aside>
        <main class="content">
          <section class="hero panel">
            <div>
              <div class="eyebrow">JMQX Control Surface</div>
              <h1 class="hero-title">{{ activeMenuLabel }}</h1>
              <p class="hero-desc">{{ activeMenuDescription }}</p>
            </div>
            <div class="hero-metrics">
              <div class="hero-pill">
                <span class="hero-pill-label">Cluster</span>
                <strong>{{ currentClusterId }}</strong>
              </div>
              <div class="hero-pill">
                <span class="hero-pill-label">Dashboard</span>
                <strong>{{ mqttStatus }}</strong>
              </div>
              <div class="hero-pill">
                <span class="hero-pill-label">Nodes</span>
                <strong>{{ totalNodes }}</strong>
              </div>
            </div>
          </section>

          <div v-if="message" class="success">{{ message }}</div>
          <div v-if="error" class="error">{{ error }}</div>

          <section class="panel" v-if="activeMenu==='overview'">
            <div class="section-head">
              <div>
                <h2 class="title">集群概览</h2>
                <div class="section-subtitle">总览卡片聚合全节点实时数据，下面保留逐节点明细。</div>
              </div>
              <button class="btn" @click="refreshOverview">刷新</button>
            </div>
            <div class="stats">
              <div class="stat">
                <div class="label">总连接数</div>
                <div class="value">{{ formatNumber(overview.totalConnections) }}</div>
              </div>
              <div class="stat">
                <div class="label">总入流量 (Bytes)</div>
                <div class="value">{{ formatNumber(overview.totalInboundBytes) }}</div>
              </div>
              <div class="stat">
                <div class="label">总出流量 (Bytes)</div>
                <div class="value">{{ formatNumber(overview.totalOutboundBytes) }}</div>
              </div>
            </div>
            <div class="node-grid" v-if="overview.nodes && overview.nodes.length">
              <article class="node-card" v-for="node in overview.nodes" :key="node.nodeId">
                <div class="node-card-head">
                  <div>
                    <div class="node-name">{{ node.nodeId }}</div>
                    <div class="node-meta">{{ node.role || '-' }} · {{ node.nodeIp }}</div>
                  </div>
                  <span class="status-badge" :class="nodeHealthClass(node)">{{ nodeHealthLabel(node) }}</span>
                </div>
                <div class="node-card-stats">
                  <div>
                    <span>连接数</span>
                    <strong>{{ formatNumber(node.connectedClients) }}</strong>
                  </div>
                  <div>
                    <span>入流量</span>
                    <strong>{{ formatNumber(node.inboundBytes) }}</strong>
                  </div>
                  <div>
                    <span>出流量</span>
                    <strong>{{ formatNumber(node.outboundBytes) }}</strong>
                  </div>
                </div>
                <div class="node-card-foot">最后上报 {{ formatDateTime(node.lastReportTime) }}</div>
              </article>
            </div>
            <table class="data-table">
              <thead>
              <tr>
                <th>节点 ID</th>
                <th>节点角色</th>
                <th>节点 IP</th>
                <th>连接数</th>
                <th>入流量</th>
                <th>出流量</th>
                <th>最后上报时间</th>
              </tr>
              </thead>
              <tbody>
              <tr v-for="node in overview.nodes" :key="node.nodeId">
                <td>{{ node.nodeId }}</td>
                <td>{{ node.role || '-' }}</td>
                <td>{{ node.nodeIp }}</td>
                <td>{{ formatNumber(node.connectedClients) }}</td>
                <td>{{ formatNumber(node.inboundBytes) }}</td>
                <td>{{ formatNumber(node.outboundBytes) }}</td>
                <td>{{ formatDateTime(node.lastReportTime) }}</td>
              </tr>
              </tbody>
            </table>
          </section>

          <section class="panel" v-if="activeMenu==='clients' && !selectedClient">
            <div class="section-head">
              <div>
                <h2 class="title">客户端列表</h2>
                <div class="section-subtitle">{{ clientsSummaryText }}</div>
              </div>
            </div>
            <div class="toolbar">
              <input v-model="search.clientId" placeholder="客户端 ID"/>
              <input v-model="search.userName" placeholder="用户名"/>
              <button class="btn" @click="queryClients">查询</button>
              <button class="btn secondary" @click="search={clientId:'',userName:'',pageNo:1,pageSize:20};queryClients()">重置</button>
            </div>
            <div class="chips toolbar-chips">
              <span class="chip">页面容量 {{ clients.pageSize }}</span>
              <span class="chip">实时刷新已启用</span>
              <span class="chip">点击行查看详情</span>
            </div>
            <table class="data-table">
              <thead>
              <tr>
                <th>客户端 ID</th>
                <th>节点</th>
                <th>IP</th>
                <th>Keepalive</th>
                <th>连接方式</th>
                <th>用户名</th>
                <th>上线时间</th>
              </tr>
              </thead>
              <tbody>
              <tr class="click-row" v-for="c in clients.records" :key="c.clientId" @click="viewClient(c.clientId)">
                <td>{{ c.clientId }}</td>
                <td>{{ c.nodeId }}</td>
                <td>{{ c.clientIp }}</td>
                <td>{{ c.keepAliveSeconds }}</td>
                <td>{{ c.connectionType }}</td>
                <td>{{ c.username || '-' }}</td>
                <td>{{ formatDateTime(c.connectedAt) }}</td>
              </tr>
              </tbody>
            </table>
            <div class="hint">总记录: {{ clients.total }}，列表会在连接事件后自动刷新。</div>
          </section>

          <section class="panel" v-if="activeMenu==='clients' && selectedClient">
            <div class="section-head">
              <div>
                <h2 class="title">客户端详情</h2>
                <div class="section-subtitle">查看连接属性、会话信息和当前订阅主题。</div>
              </div>
            </div>
            <div class="toolbar">
              <button class="btn secondary" @click="selectedClient=null">返回列表</button>
            </div>
            <table class="data-table detail-table">
              <tbody>
              <tr><th>客户端 ID</th><td>{{ selectedClient.session.clientId }}</td></tr>
              <tr><th>节点</th><td>{{ selectedClient.session.nodeId }}</td></tr>
              <tr><th>IP</th><td>{{ selectedClient.session.clientIp }}</td></tr>
              <tr><th>Keepalive</th><td>{{ selectedClient.session.keepAliveSeconds }}</td></tr>
              <tr><th>连接方式</th><td>{{ selectedClient.session.connectionType }}</td></tr>
              <tr><th>用户名</th><td>{{ selectedClient.session.username || '-' }}</td></tr>
              <tr><th>上线时间</th><td>{{ formatDateTime(selectedClient.session.connectedAt) }}</td></tr>
              </tbody>
            </table>
            <div class="hint">订阅主题</div>
            <div class="chips">
              <span class="chip" v-for="topic in selectedClient.subscribedTopics" :key="topic">{{ topic }}</span>
              <span class="hint" v-if="selectedClient.subscribedTopics.length===0">暂无订阅主题</span>
            </div>
          </section>

          <section class="panel" v-if="activeMenu==='acl'">
            <div class="section-head">
              <div>
                <h2 class="title">ACL 鉴权配置</h2>
                <div class="section-subtitle">控制主题发布与订阅的访问链路。</div>
              </div>
              <span class="status-badge" :class="aclStatusClass">{{ aclStatusText }}</span>
            </div>
            <div class="form-grid">
              <label class="field checkbox-field">
                <input type="checkbox" v-model="securityConfig.aclEnabled"/>
                <span>启用 ACL 鉴权</span>
              </label>
              <label class="field">
                <span class="field-label">ACL 链（逗号分隔，按顺序执行）</span>
                <input :value="joinComma(securityConfig.aclChain)" @input="securityConfig.aclChain=$event.target.value"/>
              </label>
              <label class="field">
                <span class="field-label">鉴权缓存时间（毫秒）</span>
                <input type="number" min="0" v-model.number="securityConfig.cacheTtlMs"/>
              </label>
            </div>
            <div class="actions">
              <button class="btn" @click="saveAclConfig">保存 ACL 配置</button>
            </div>
          </section>

          <section class="panel auth-surface" v-if="activeMenu==='auth' && !authCreateMode">
            <div class="auth-list-toolbar">
              <div>
                <h2 class="title auth-title">客户端认证</h2>
                <div class="section-subtitle">统一管理接入认证、状态和数据源。</div>
              </div>
              <button class="btn auth-create-btn" @click="openAuthCreate">+ 创建</button>
            </div>
            <table class="data-table auth-table">
              <thead>
              <tr>
                <th>数据源及认证方式</th>
                <th>数据源状态</th>
                <th>是否启用</th>
                <th>操作</th>
              </tr>
              </thead>
              <tbody>
              <tr v-if="hasAuthRecord()">
                <td>
                  <div class="auth-main-cell">
                    <div class="auth-main-name">{{ authPluginDisplayName(primaryAuthPlugin()) }}</div>
                    <div class="auth-main-sub">{{ authMethodLabel() }}</div>
                  </div>
                </td>
                <td>
                  <span class="auth-status" :class="authStatusClass">
                    {{ authStatusText }}
                  </span>
                </td>
                <td>
                  <label class="switch">
                    <input type="checkbox" v-model="securityConfig.authEnabled" @change="toggleAuthEnabled"/>
                    <span class="slider"></span>
                  </label>
                </td>
                <td class="auth-actions">
                  <button class="btn secondary">用户管理</button>
                  <button class="btn secondary" @click="openAuthSettings">设置</button>
                  <button class="btn secondary">更多</button>
                </td>
              </tr>
              <tr v-else>
                <td colspan="4" class="hint">暂无认证配置（当前为未配置或 allow_all）</td>
              </tr>
              </tbody>
            </table>
          </section>

          <section class="panel auth-surface" v-if="activeMenu==='auth' && authCreateMode">
            <div class="auth-create-header">
              <button class="btn secondary auth-back-btn" @click="cancelAuthCreate">返回</button>
              <span class="auth-sep">|</span>
              <h2 class="title auth-title">创建认证</h2>
            </div>

            <div class="auth-stepper">
              <div class="step-item" :class="{active: authStep===1, done: authStepDone(1)}">
                <span class="step-circle">{{ authStepDone(1) ? '✓' : '1' }}</span>
                <span class="step-text">认证方式</span>
              </div>
              <div class="step-line"></div>
              <div class="step-item" :class="{active: authStep===2, done: authStepDone(2)}">
                <span class="step-circle">{{ authStepDone(2) ? '✓' : '2' }}</span>
                <span class="step-text">数据源</span>
              </div>
              <div class="step-line"></div>
              <div class="step-item" :class="{active: authStep===3, done: authStepDone(3)}">
                <span class="step-circle">3</span>
                <span class="step-text">配置参数</span>
              </div>
            </div>

            <div v-if="authStep===1">
              <div class="auth-hint">使用客户端用户名、Client ID 与密码进行认证</div>
              <div class="auth-option-grid">
                <button
                    v-for="item in authMethodOptions"
                    :key="item.key"
                    class="auth-option-card"
                    :class="{selected: authDraft.method===item.key}"
                    @click="selectAuthMethod(item.key)">
                  {{ item.label }}
                </button>
              </div>
            </div>

            <div v-if="authStep===2">
              <div class="auth-hint">选择存储认证数据的数据源</div>
              <div class="auth-option-grid datasource-grid">
                <button
                    v-for="item in authDatasourceOptions"
                    :key="item.key"
                    class="auth-option-card"
                    :class="{selected: authDraft.datasource===item.key}"
                    @click="selectAuthDatasource(item.key)">
                  <span class="ds-icon">{{ item.icon }}</span>{{ item.label }}
                </button>
              </div>
            </div>

            <div v-if="authStep===3">
              <div class="auth-hint">当前组合：{{ authMethodLabel() }} / {{ authDatasourceLabel() }}</div>
              <div class="form-grid auth-config-grid">
                <label class="field">
                  <span class="field-label">缓存时间（毫秒）</span>
                  <input type="number" min="0" v-model.number="authDraft.cacheTtlMs"/>
                </label>
                <label class="field" v-if="authDraft.datasource==='builtin'">
                  <span class="field-label">用户文件路径</span>
                  <input v-model="authDraft.filePath" placeholder="conf/auth-users.txt"/>
                </label>
                <label class="field" v-if="authDraft.datasource==='http'">
                  <span class="field-label">HTTP 认证地址</span>
                  <input v-model="authDraft.httpUrl" placeholder="http://host:port/auth/verify"/>
                </label>
                <label class="field" v-if="authDraft.datasource==='redis'">
                  <span class="field-label">Redis Host</span>
                  <input v-model="authDraft.redisHost" placeholder="127.0.0.1"/>
                </label>
                <label class="field" v-if="authDraft.datasource==='redis'">
                  <span class="field-label">Redis Port</span>
                  <input type="number" min="1" v-model.number="authDraft.redisPort"/>
                </label>
                <label class="field" v-if="authDraft.datasource!=='builtin' && authDraft.datasource!=='http' && authDraft.datasource!=='redis'">
                  <span class="field-label">数据库连接串</span>
                  <input v-model="authDraft.dbUrl" placeholder="jdbc:..."/>
                </label>
                <label class="field" v-if="authDraft.datasource!=='builtin' && authDraft.datasource!=='http' && authDraft.datasource!=='redis'">
                  <span class="field-label">数据库用户名</span>
                  <input v-model="authDraft.dbUser" placeholder="root"/>
                </label>
                <label class="field" v-if="authDraft.datasource!=='builtin' && authDraft.datasource!=='http' && authDraft.datasource!=='redis'">
                  <span class="field-label">数据库密码</span>
                  <input type="password" v-model="authDraft.dbPassword"/>
                </label>
              </div>
            </div>

            <div class="auth-footer-actions">
              <button class="btn secondary" @click="cancelAuthCreate">取消</button>
              <button class="btn secondary" v-if="authStep>1" @click="previousAuthStep">上一步</button>
              <button class="btn" v-if="authStep<3" @click="nextAuthStep">下一步</button>
              <button class="btn auth-create-btn" v-if="authStep===3" @click="createAuthAndSave">创建</button>
            </div>
          </section>

          <section class="panel" v-if="activeMenu==='cluster'">
            <div class="section-head">
              <div>
                <h2 class="title">集群配置</h2>
                <div class="section-subtitle">调整节点列表、接入策略和共享订阅容量。</div>
              </div>
            </div>
            <div class="hint">Core 节点（每行一个 host:port）</div>
            <textarea :value="joinLine(clusterConfig.coreNodes)" @input="clusterConfig.coreNodes=$event.target.value"></textarea>
            <div class="hint">Replicant 节点（每行一个 host:port）</div>
            <textarea :value="joinLine(clusterConfig.replicantNodes)" @input="clusterConfig.replicantNodes=$event.target.value"></textarea>
            <div class="toolbar">
              <label><input type="checkbox" v-model="clusterConfig.coreAcceptClientConnections"/> Core 可接入客户端</label>
            </div>
            <div class="hint">共享订阅每组最大成员数</div>
            <input type="number" min="1" v-model.number="clusterConfig.sharedSubscriptionMaxMembersPerGroup"/>
            <div style="margin-top: 10px">
              <button class="btn" @click="saveClusterConfig">保存</button>
            </div>
          </section>
        </main>
      </div>
    `
}).mount("#app");
