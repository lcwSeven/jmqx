import { fetchOverview } from "../../api.js";

export const overviewPageMethods = {
    async refreshOverview() {
        this.overview = await fetchOverview(this.currentClusterId);
    },
    formatLatencyMs(value) {
        const ms = Number(value || 0);
        return ms > 0 ? `${this.formatNumber(ms)} ms` : "-";
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
    },
    nodeSecurityRiskClass(node) {
        if (Number(node?.connectAuthError || 0) > 0 || Number(node?.publishAclError || 0) > 0) {
            return "is-down";
        }
        if (Number(node?.connectAuthFailure || 0) > 0 || Number(node?.publishAclDeny || 0) > 0) {
            return "is-warn";
        }
        return "is-up";
    },
    nodeSecurityRiskLabel(node) {
        if (Number(node?.connectAuthError || 0) > 0 || Number(node?.publishAclError || 0) > 0) {
            return "有异常";
        }
        if (Number(node?.connectAuthFailure || 0) > 0 || Number(node?.publishAclDeny || 0) > 0) {
            return "有拒绝";
        }
        return "稳定";
    }
};
