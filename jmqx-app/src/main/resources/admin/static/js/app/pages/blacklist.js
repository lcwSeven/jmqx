import { createBlacklistEntry, deleteBlacklistEntry, fetchBlacklistEntries } from "../../api.js";

export const blacklistPageMethods = {
    async loadBlacklistEntries() {
        const data = await fetchBlacklistEntries(this.currentClusterId);
        this.blacklistEntries = Array.isArray(data?.records) ? data.records : [];
    },
    async upsertBlacklistEntry(type, value) {
        const normalizedType = type === "ip" ? "ip" : "clientId";
        const normalizedValue = String(value || "").trim();
        if (!normalizedValue) {
            this.error = normalizedType === "ip" ? "请输入要拉黑的 IP" : "请输入要拉黑的 clientId";
            return false;
        }
        const data = await createBlacklistEntry(this.currentClusterId, {
            type: normalizedType,
            value: normalizedValue
        });
        this.blacklistEntries = Array.isArray(data?.records) ? data.records : [];
        this.blacklistForm.type = normalizedType;
        this.blacklistForm.value = "";
        this.message = normalizedType === "ip"
            ? `IP ${normalizedValue} 已加入黑名单`
            : `clientId ${normalizedValue} 已加入黑名单`;
        this.error = "";
        return true;
    },
    async submitBlacklistEntry() {
        try {
            const created = await this.upsertBlacklistEntry(this.blacklistForm.type, this.blacklistForm.value);
            if (created) {
                await this.queryClients();
            }
        } catch (e) {
            this.error = "新增黑名单失败: " + e.message;
        }
    },
    async removeBlacklistEntry(entry) {
        if (!entry?.type || !entry?.value) {
            return;
        }
        const messageBox = globalThis.ElementPlus?.ElMessageBox;
        if (messageBox?.confirm) {
            await messageBox.confirm(
                `确认从黑名单中移除 ${entry.type === "ip" ? "IP" : "clientId"} ${entry.value} 吗？`,
                "移除黑名单",
                {
                    confirmButtonText: "确认移除",
                    cancelButtonText: "取消",
                    type: "warning"
                }
            );
        }
        try {
            const data = await deleteBlacklistEntry(this.currentClusterId, entry.type, entry.value);
            this.blacklistEntries = Array.isArray(data?.records) ? data.records : [];
            this.message = `${entry.type === "ip" ? "IP" : "clientId"} ${entry.value} 已移出黑名单`;
            this.error = "";
        } catch (e) {
            if (e?.message === "cancel") {
                return;
            }
            this.error = "移除黑名单失败: " + e.message;
        }
    },
    blacklistTypeLabel(type) {
        return type === "ip" ? "IP" : "clientId";
    }
};
