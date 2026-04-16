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
            this.error = normalizedType === "ip" ? this.tr("blacklist.message.missingIp") : this.tr("blacklist.message.missingClientId");
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
            ? this.tr("blacklist.message.createdIp", { value: normalizedValue })
            : this.tr("blacklist.message.createdClientId", { value: normalizedValue });
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
            this.error = this.tr("blacklist.message.createFailed", { message: e.message });
        }
    },
    async removeBlacklistEntry(entry) {
        if (!entry?.type || !entry?.value) {
            return;
        }
        const messageBox = globalThis.ElementPlus?.ElMessageBox;
        if (messageBox?.confirm) {
            await messageBox.confirm(
                this.tr("blacklist.message.removeConfirm", {
                    type: entry.type === "ip" ? this.tr("blacklist.type.ip") : this.tr("blacklist.type.clientId"),
                    value: entry.value
                }),
                this.tr("blacklist.message.removeTitle"),
                {
                    confirmButtonText: this.tr("blacklist.message.removeConfirmButton"),
                    cancelButtonText: this.tr("common.cancel"),
                    type: "warning"
                }
            );
        }
        try {
            const data = await deleteBlacklistEntry(this.currentClusterId, entry.type, entry.value);
            this.blacklistEntries = Array.isArray(data?.records) ? data.records : [];
            this.message = this.tr("blacklist.message.removed", {
                type: entry.type === "ip" ? this.tr("blacklist.type.ip") : this.tr("blacklist.type.clientId"),
                value: entry.value
            });
            this.error = "";
        } catch (e) {
            if (e?.message === "cancel") {
                return;
            }
            this.error = this.tr("blacklist.message.removeFailed", { message: e.message });
        }
    },
    blacklistTypeLabel(type) {
        return type === "ip" ? this.tr("blacklist.type.ip") : this.tr("blacklist.type.clientId");
    }
};
