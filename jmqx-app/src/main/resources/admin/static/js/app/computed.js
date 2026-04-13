export const adminComputed = {
    activeMenuLabel() {
        const labels = {
            overview: "集群概览",
            clients: this.selectedClient ? "客户端详情" : "客户端列表",
            acl: "ACL 鉴权",
            auth: this.authCreateMode ? "创建认证" : "连接鉴权",
            "built-in-users": "内置数据库用户管理",
            cluster: "集群配置",
            audit: "操作审计"
        };
        return labels[this.activeMenu] || "JMQX Admin";
    },
    activeMenuDescription() {
        const descriptions = {
            overview: "实时查看节点负载、流量和运行态变化。",
            clients: "搜索在线客户端，快速定位连接和订阅信息。",
            acl: "维护主题访问策略与鉴权缓存设置。",
            auth: "统一管理客户端接入认证与数据源配置。",
            "built-in-users": "添加、导入和删除内置数据库认证用户。",
            cluster: "调整节点角色、共享订阅容量和集群行为。",
            audit: "查看配置变更来源、时间与前后快照。"
        };
        return descriptions[this.activeMenu] || "";
    },
    builtInUserIdMatchHint() {
        return this.builtInUsers.accountType === "clientId"
            ? "客户端会使用 clientId + password 与内置数据库中的 userId + password 比较。"
            : "客户端会使用 username + password 与内置数据库中的 userId + password 比较。";
    },
    builtInAccountFieldLabel() {
        return this.builtInUsers.accountType === "clientId" ? "clientId" : "username";
    },
    adminRoleLabel() {
        return this.adminSession?.superAdmin ? "超级管理员" : (this.adminSession?.role || "未登录");
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
    authEntries() {
        const chain = Array.isArray(this.securityConfig.authChain) ? this.securityConfig.authChain : [];
        return chain
            .map((plugin, index) => {
                const key = String(plugin || "").trim().toLowerCase();
                if (!key) {
                    return null;
                }
                return {
                    plugin: key,
                    index,
                    displayName: this.authPluginDisplayName(key),
                    summary: this.describeAuthPlugin(key)
                };
            })
            .filter(Boolean);
    },
    availableAuthDatasourceOptions() {
        const used = new Set(this.authEntries.map(entry => entry.plugin));
        return this.authDatasourceOptions.filter(option => !used.has(this.mapDatasourceToPlugin(option.key)));
    },
    authDraftDatasourceOptions() {
        if (this.authEditingPlugin) {
            return this.authDatasourceOptions.filter(option => this.mapDatasourceToPlugin(option.key) === this.authEditingPlugin);
        }
        return this.availableAuthDatasourceOptions;
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
    },
    auditSummaryText() {
        return `最近 ${Array.isArray(this.auditLogs) ? this.auditLogs.length : 0} 条审计记录`;
    },
    filteredAuditLogs() {
        if (!Array.isArray(this.auditLogs)) {
            return [];
        }
        if (this.auditFilter === "all") {
            return this.auditLogs;
        }
        return this.auditLogs.filter(entry => entry && entry.action === this.auditFilter);
    },
    auditActions() {
        const values = new Set(["all"]);
        if (Array.isArray(this.auditLogs)) {
            this.auditLogs.forEach(entry => {
                if (entry && entry.action) {
                    values.add(entry.action);
                }
            });
        }
        return Array.from(values);
    }
};
