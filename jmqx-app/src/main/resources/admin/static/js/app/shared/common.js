import { changeAdminPassword, clearStoredAdminAuth, fetchAdminSession, fetchClusters, storeAdminAuth } from "../../api.js";
import { createInitialState } from "../state.js";

export const commonMethods = {
    clearTips() {
        this.message = "";
        this.error = "";
    },
    setMenu(menu) {
        this.activeMenu = menu;
        this.selectedClient = null;
        if (menu !== "acl") {
            this.aclCreateMode = false;
            this.aclStep = 1;
        }
        if (menu !== "auth") {
            this.authCreateMode = false;
            this.authStep = 1;
        }
        if (menu !== "bridge") {
            this.bridgeCreateMode = false;
            this.bridgeEditingType = "";
            this.bridgeStep = 1;
        }
        if (menu === "blacklist" && typeof this.loadBlacklistEntries === "function") {
            this.loadBlacklistEntries();
        }
        if (menu === "bridge" && typeof this.loadBridgeConfig === "function") {
            this.loadBridgeConfig();
        }
        this.clearTips();
    },
    resetAdminWorkspace() {
        const initial = createInitialState();
        this.activeMenu = initial.activeMenu;
        this.clusters = initial.clusters;
        this.currentClusterId = initial.currentClusterId;
        this.clusterSelectionTouched = initial.clusterSelectionTouched;
        this.mqttStatus = initial.mqttStatus;
        this.realtimeNodeMap = {};
        this.overview = initial.overview;
        this.clients = initial.clients;
        this.search = initial.search;
        this.selectedClient = null;
        this.blacklistEntries = initial.blacklistEntries;
        this.blacklistForm = initial.blacklistForm;
        this.builtInUsers = initial.builtInUsers;
        this.builtInUserForm = initial.builtInUserForm;
        this.builtInUserImportText = "";
        this.builtInUserImportFile = null;
        this.builtInUserDialogs = initial.builtInUserDialogs;
        this.auditLogs = [];
        this.auditFilter = initial.auditFilter;
        this.expandedAuditIds = [];
        this.securityConfig = initial.securityConfig;
        this.bridgeConfig = initial.bridgeConfig;
        this.bridgeCreateMode = false;
        this.bridgeEditingType = "";
        this.bridgeStep = 1;
        this.bridgeDraft = initial.bridgeDraft;
        this.clusterConfig = initial.clusterConfig;
        this.aclCreateMode = false;
        this.aclEditingPlugin = "";
        this.aclStep = 1;
        this.aclDraft = initial.aclDraft;
        this.authCreateMode = false;
        this.authEditingPlugin = "";
        this.authStep = 1;
        this.authDraft = initial.authDraft;
        this.adminSession = initial.adminSession;
        this.adminDialogs = initial.adminDialogs;
        this.adminPasswordForm = initial.adminPasswordForm;
    },
    async reloadCurrentClusterData() {
        this.clearTips();
        try {
            await this.loadClusters();
        } catch (e) {
            if (e?.status === 401) {
                this.adminAuthRequired = true;
                this.adminAuthenticated = false;
                this.disconnectDashboardStream();
                this.resetAdminWorkspace();
                this.error = "请输入内嵌管理后台账号密码";
                return;
            }
            throw e;
        }
        const tasks = [
            { label: "登录会话", run: () => this.loadAdminSession() },
            { label: "集群概览", run: () => this.refreshOverview() },
            { label: "客户端列表", run: () => this.queryClients() },
            { label: "黑名单", optional: true, run: () => this.loadBlacklistEntries() },
            { label: "安全配置", run: () => this.loadSecurityConfig() },
            { label: "桥接配置", run: () => this.loadBridgeConfig() }
        ];
        const results = await Promise.allSettled(tasks.map(task => task.run()));
        const failures = [];
        results.forEach((result, index) => {
            if (result.status === "rejected") {
                failures.push({
                    label: tasks[index].label,
                    optional: Boolean(tasks[index].optional),
                    status: result.reason?.status || 0,
                    message: result.reason?.message || "request failed"
                });
            }
        });
        if (failures.some(item => item.status === 401)) {
            this.adminAuthRequired = true;
            this.adminAuthenticated = false;
            this.disconnectDashboardStream();
            this.resetAdminWorkspace();
            this.error = "请输入内嵌管理后台账号密码";
            return;
        }
        this.adminAuthRequired = false;
        this.adminAuthenticated = true;
        const blockingFailures = failures.filter(item => !item.optional);
        if (blockingFailures.length > 0) {
            this.error = "加载集群数据失败: " + blockingFailures.map(item => item.label + "（" + item.message + "）").join("；");
        }
        const optionalFailures = failures.filter(item => item.optional);
        if (optionalFailures.length > 0) {
            this.message = optionalFailures.map(item => item.label + "暂不可用，已跳过加载").join("；");
        }
    },
    async loadClusters() {
        const clusters = await fetchClusters();
        this.clusters = Array.isArray(clusters) ? clusters : [];
        this.currentClusterId = this.resolvePreferredClusterId(this.clusters, this.currentClusterId, this.clusterSelectionTouched);
    },
    resolvePreferredClusterId(clusters, currentClusterId, preserveSelection) {
        const records = Array.isArray(clusters) ? clusters.filter(Boolean) : [];
        if (records.length === 0) {
            return currentClusterId || "default";
        }
        if (preserveSelection && currentClusterId && records.some(cluster => cluster.clusterId === currentClusterId)) {
            return currentClusterId;
        }
        const preferred = records.find(cluster => cluster.clusterId && cluster.clusterId !== "default");
        if (preferred?.clusterId) {
            return preferred.clusterId;
        }
        return records[0]?.clusterId || currentClusterId || "default";
    },
    async switchCluster(clusterId) {
        const nextClusterId = String(clusterId || "").trim();
        if (!nextClusterId || nextClusterId === this.currentClusterId) {
            return;
        }
        this.clearTips();
        this.clusterSelectionTouched = true;
        this.currentClusterId = nextClusterId;
        this.realtimeNodeMap = {};
        this.selectedClient = null;
        this.disconnectDashboardStream();
        try {
            await this.reloadCurrentClusterData();
            if (this.adminAuthenticated) {
                this.connectDashboardStream();
                this.message = `已切换到集群 ${this.currentClusterId}`;
            }
        } catch (e) {
            this.error = "切换集群失败: " + (e?.message || "request failed");
        }
    },
    async loadAdminSession() {
        const session = await fetchAdminSession();
        this.adminSession = {
            authenticated: session?.authenticated === true,
            username: String(session?.username || ""),
            role: String(session?.role || ""),
            superAdmin: session?.superAdmin === true,
            permissions: Array.isArray(session?.permissions) ? session.permissions : []
        };
    },
    async loginAdminPanel() {
        this.clearTips();
        this.disconnectDashboardStream();
        storeAdminAuth(this.adminLoginForm.username, this.adminLoginForm.password);
        try {
            await this.reloadCurrentClusterData();
            if (this.adminAuthenticated) {
                this.connectDashboardStream();
                this.message = "管理后台登录成功";
            }
        } catch (e) {
            this.error = "登录失败: " + (e?.message || "request failed");
        }
    },
    openAdminPasswordDialog() {
        this.clearTips();
        this.adminPasswordForm.currentPassword = "";
        this.adminPasswordForm.newPassword = "";
        this.adminPasswordForm.confirmPassword = "";
        this.adminDialogs.password = true;
    },
    closeAdminPasswordDialog() {
        this.adminDialogs.password = false;
    },
    async submitAdminPasswordChange() {
        this.clearTips();
        if (!this.adminPasswordForm.currentPassword) {
            this.error = "请输入当前密码";
            return;
        }
        if (!this.adminPasswordForm.newPassword) {
            this.error = "请输入新密码";
            return;
        }
        if (this.adminPasswordForm.newPassword !== this.adminPasswordForm.confirmPassword) {
            this.error = "两次输入的新密码不一致";
            return;
        }
        await changeAdminPassword({
            currentPassword: this.adminPasswordForm.currentPassword,
            newPassword: this.adminPasswordForm.newPassword,
            confirmPassword: this.adminPasswordForm.confirmPassword
        });
        this.adminLoginForm.password = this.adminPasswordForm.newPassword;
        storeAdminAuth(this.adminLoginForm.username, this.adminLoginForm.password);
        this.adminDialogs.password = false;
        this.adminPasswordForm.currentPassword = "";
        this.adminPasswordForm.newPassword = "";
        this.adminPasswordForm.confirmPassword = "";
        this.disconnectDashboardStream();
        this.connectDashboardStream();
        this.message = "管理后台密码已更新";
    },
    handleAdminMenuCommand(command) {
        if (command === "change-password") {
            this.openAdminPasswordDialog();
            return;
        }
        if (command === "logout") {
            this.logoutAdminPanel();
        }
    },
    logoutAdminPanel() {
        this.disconnectDashboardStream();
        clearStoredAdminAuth();
        this.adminAuthenticated = false;
        this.adminAuthRequired = true;
        this.resetAdminWorkspace();
        this.adminLoginForm.password = "";
        this.message = "";
        this.error = "已退出登录";
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
    }
};
