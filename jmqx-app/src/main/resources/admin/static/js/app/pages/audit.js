import { fetchAuditLogs } from "../../api.js";

export const auditPageMethods = {
    async loadAuditLogs() {
        this.auditLogs = await fetchAuditLogs(this.currentClusterId, 50);
        this.expandedAuditIds = [];
    },
    formatJsonPreview(value) {
        if (!value) {
            return "-";
        }
        try {
            return JSON.stringify(JSON.parse(value), null, 2);
        } catch (e) {
            return value;
        }
    },
    isAuditExpanded(entryId) {
        return this.expandedAuditIds.includes(entryId);
    },
    toggleAudit(entryId) {
        if (!entryId) {
            return;
        }
        if (this.isAuditExpanded(entryId)) {
            this.expandedAuditIds = this.expandedAuditIds.filter(id => id !== entryId);
            return;
        }
        this.expandedAuditIds = [...this.expandedAuditIds, entryId];
    },
    async copyAudit(entry) {
        if (!entry) {
            return;
        }
        const text = [
            `action: ${entry.action || ""}`,
            `source: ${entry.source || ""}`,
            `timestamp: ${this.formatDateTime(entry.timestamp)}`,
            "before:",
            this.formatJsonPreview(entry.beforeJson),
            "after:",
            this.formatJsonPreview(entry.afterJson)
        ].join("\n");
        try {
            if (navigator?.clipboard?.writeText) {
                await navigator.clipboard.writeText(text);
                this.message = "审计快照已复制";
                this.error = "";
                return;
            }
        } catch (e) {
            // fallback below
        }
        this.error = "当前环境不支持复制，请手动复制内容";
    }
};
