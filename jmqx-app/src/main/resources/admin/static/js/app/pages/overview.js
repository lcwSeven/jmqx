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
            return this.tr("overview.nodeHealth.unknown");
        }
        return Date.now() - ts <= 15000 ? this.tr("overview.nodeHealth.online") : this.tr("overview.nodeHealth.stale");
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
            return this.tr("overview.nodeRisk.errors");
        }
        if (Number(node?.connectAuthFailure || 0) > 0 || Number(node?.publishAclDeny || 0) > 0) {
            return this.tr("overview.nodeRisk.denied");
        }
        return this.tr("overview.nodeRisk.stable");
    }
};
