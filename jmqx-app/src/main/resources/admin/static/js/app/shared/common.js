export const commonMethods = {
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
        this.clearTips();
        const tasks = [
            { label: "集群概览", run: () => this.refreshOverview() },
            { label: "客户端列表", run: () => this.queryClients() },
            { label: "安全配置", run: () => this.loadSecurityConfig() },
            { label: "集群配置", run: () => this.loadClusterConfig() },
            { label: "操作审计", optional: true, run: () => this.loadAuditLogs() }
        ];
        const results = await Promise.allSettled(tasks.map(task => task.run()));
        const failures = [];
        results.forEach((result, index) => {
            if (result.status === "rejected") {
                failures.push({
                    label: tasks[index].label,
                    optional: Boolean(tasks[index].optional),
                    message: result.reason?.message || "request failed"
                });
            }
        });
        const blockingFailures = failures.filter(item => !item.optional);
        if (blockingFailures.length > 0) {
            this.error = "加载集群数据失败: " + blockingFailures.map(item => item.label + "（" + item.message + "）").join("；");
        }
        const optionalFailures = failures.filter(item => item.optional);
        if (optionalFailures.length > 0) {
            this.message = optionalFailures.map(item => item.label + "暂不可用，已跳过加载").join("；");
        }
        if (optionalFailures.some(item => item.label === "操作审计")) {
            this.auditLogs = [];
            this.expandedAuditIds = [];
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
    }
};
