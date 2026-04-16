export const adminComputed = {
    activeMenuLabel() {
        const labels = {
            overview: "menu.overview",
            clients: this.selectedClient ? "menu.clientDetails" : "menu.clients",
            blacklist: "menu.blacklist",
            acl: this.aclCreateMode ? "menu.aclCreate" : "menu.acl",
            auth: this.authCreateMode ? "menu.authCreate" : "menu.auth",
            "built-in-users": "menu.builtInUsers",
            bridge: "menu.bridge"
        };
        return this.tr(labels[this.activeMenu] || "app.brand.console");
    },
    activeMenuDescription() {
        const descriptions = {
            overview: "description.overview",
            clients: "description.clients",
            blacklist: "description.blacklist",
            acl: "description.acl",
            auth: "description.auth",
            "built-in-users": "description.builtInUsers",
            bridge: "description.bridge"
        };
        return this.tr(descriptions[this.activeMenu] || "");
    },
    builtInUserIdMatchHint() {
        const hint = this.builtInUsers.accountType === "clientId"
            ? "hint.builtInUser.clientId"
            : "hint.builtInUser.username";
        return this.tr(hint);
    },
    builtInAccountFieldLabel() {
        return this.builtInUsers.accountType === "clientId" ? "clientId" : "username";
    },
    adminRoleLabel() {
        return this.adminSession?.superAdmin ? this.tr("admin.role.superAdmin") : (this.adminSession?.role || this.tr("admin.role.unknown"));
    },
    mqttStatusLabel() {
        return this.tr(this.mqttStatus);
    },
    currentClusterDisplayName() {
        const current = Array.isArray(this.clusters)
            ? this.clusters.find(cluster => cluster && cluster.clusterId === this.currentClusterId)
            : null;
        return current?.displayName || this.currentClusterId || "-";
    },
    bridgeEntries() {
        const types = Array.isArray(this.bridgeConfig?.types) ? this.bridgeConfig.types : [];
        return types
            .map((type, index) => {
                const key = String(type || "").trim().toLowerCase();
                if (!key) {
                    return null;
                }
                return {
                    type: key,
                    index,
                    displayName: this.bridgePluginDisplayName(key),
                    summary: this.describeBridgePlugin(key)
                };
            })
            .filter(Boolean);
    },
    availableBridgeDatasourceOptions() {
        const used = new Set(this.bridgeEntries.map(entry => entry.type));
        return this.bridgeDatasourceOptions.filter(option => !used.has(option.key));
    },
    bridgeDraftDatasourceOptions() {
        if (this.bridgeEditingType) {
            return this.bridgeDatasourceOptions.filter(option => option.key === this.bridgeEditingType);
        }
        return this.availableBridgeDatasourceOptions;
    },
    authStatusText() {
        if (!this.hasAuthRecord()) {
            return this.tr("status.notConfigured");
        }
        return this.securityConfig.authEnabled ? this.tr("status.enabled") : this.tr("status.disabled");
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
        if (!this.aclEntries.length) {
            return this.tr("status.notConfigured");
        }
        return this.securityConfig.aclEnabled ? this.tr("status.enabled") : this.tr("status.disabled");
    },
    aclStatusClass() {
        if (!this.aclEntries.length) {
            return "is-idle";
        }
        return this.securityConfig.aclEnabled ? "is-up" : "is-down";
    },
    aclEntries() {
        const chain = Array.isArray(this.securityConfig.aclChain) ? this.securityConfig.aclChain : [];
        return chain
            .map((plugin, index) => {
                const key = String(plugin || "").trim().toLowerCase();
                if (!key) {
                    return null;
                }
                return {
                    plugin: key,
                    index,
                    displayName: this.aclPluginDisplayName(key),
                    summary: this.describeAclPlugin(key)
                };
            })
            .filter(Boolean);
    },
    availableAclDatasourceOptions() {
        const used = new Set(this.aclEntries.map(entry => entry.plugin));
        return this.aclDatasourceOptions.filter(option => !used.has(this.mapAclDatasourceToPlugin(option.key)));
    },
    aclDraftDatasourceOptions() {
        if (this.aclEditingPlugin) {
            return this.aclDatasourceOptions.filter(option => this.mapAclDatasourceToPlugin(option.key) === this.aclEditingPlugin);
        }
        return this.availableAclDatasourceOptions;
    },
    totalNodes() {
        return Array.isArray(this.overview.nodes) ? this.overview.nodes.length : 0;
    },
    clientsSummaryText() {
        const total = Number(this.clients.total || 0);
        const keyword = [this.search.clientId, this.search.userName].filter(Boolean).join(" / ");
        return keyword
            ? this.tr("summary.clients.filtered", { total, keyword })
            : this.tr("summary.clients.total", { total });
    },
    blacklistSummaryText() {
        return this.tr("summary.blacklist.total", {
            count: Array.isArray(this.blacklistEntries) ? this.blacklistEntries.length : 0
        });
    },
    auditSummaryText() {
        return this.tr("summary.audit.recent", {
            count: Array.isArray(this.auditLogs) ? this.auditLogs.length : 0
        });
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
