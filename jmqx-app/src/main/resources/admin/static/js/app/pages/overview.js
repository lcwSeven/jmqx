import { fetchOverview } from "../../api.js";

export const overviewPageMethods = {
    async refreshOverview() {
        this.overview = await fetchOverview(this.currentClusterId);
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
};
