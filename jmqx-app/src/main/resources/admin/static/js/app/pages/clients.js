import { fetchClientDetail, fetchClients } from "../../api.js";

export const clientsPageMethods = {
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
    async queryClients() {
        this.clients = await fetchClients(this.currentClusterId, this.search);
    },
    async viewClient(clientId) {
        try {
            this.selectedClient = await fetchClientDetail(this.currentClusterId, clientId);
        } catch (e) {
            this.error = "加载客户端详情失败: " + e.message;
        }
    }
};
