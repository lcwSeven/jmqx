import { kickClient } from "../../api.js";
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
    },
    removeClientFromVisibleList(clientId) {
        if (!clientId) {
            return;
        }
        if (Array.isArray(this.clients.records)) {
            this.clients.records = this.clients.records.filter(item => item?.clientId !== clientId);
        }
        if (this.selectedClient?.session?.clientId === clientId) {
            this.selectedClient = null;
        }
        if (Number(this.clients.total || 0) > 0) {
            this.clients.total = Math.max(0, Number(this.clients.total || 0) - 1);
        }
    },
    async kickClient(clientId) {
        if (!clientId) {
            return;
        }
        const messageBox = globalThis.ElementPlus?.ElMessageBox;
        if (messageBox?.confirm) {
            try {
                await messageBox.confirm(
                    `确认踢掉客户端 ${clientId} 吗？`,
                    "踢下线客户端",
                    {
                        confirmButtonText: "确认踢下线",
                        cancelButtonText: "取消",
                        type: "warning"
                    }
                );
            } catch (e) {
                return;
            }
        }
        try {
            await kickClient(this.currentClusterId, clientId);
            this.removeClientFromVisibleList(clientId);
            this.message = `客户端 ${clientId} 已发起踢下线`;
            this.error = "";
        } catch (e) {
            this.error = "踢客户端失败: " + e.message;
        }
    },
    async blockClientByClientId(clientId) {
        try {
            const created = await this.upsertBlacklistEntry("clientId", clientId);
            if (created) {
                this.removeClientFromVisibleList(clientId);
            }
        } catch (e) {
            this.error = "加入 clientId 黑名单失败: " + e.message;
        }
    },
    async blockClientByIp(clientIp, clientId) {
        try {
            const created = await this.upsertBlacklistEntry("ip", clientIp);
            if (created && Array.isArray(this.clients.records)) {
                this.clients.records = this.clients.records.filter(item => item?.clientIp !== clientIp);
                if (this.selectedClient?.session?.clientIp === clientIp || this.selectedClient?.session?.clientId === clientId) {
                    this.selectedClient = null;
                }
            }
        } catch (e) {
            this.error = "加入 IP 黑名单失败: " + e.message;
        }
    }
};
