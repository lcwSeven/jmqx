import { createClientTrace, deleteClientTrace, fetchClientTraces } from "../../api.js";

function toDateTimeLocalValue(timestamp) {
    const date = new Date(timestamp);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    const hours = String(date.getHours()).padStart(2, "0");
    const minutes = String(date.getMinutes()).padStart(2, "0");
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

export const tracePageMethods = {
    defaultTraceStartAt() {
        return toDateTimeLocalValue(Date.now() + 5 * 60 * 1000);
    },
    async loadClientTraces() {
        const data = await fetchClientTraces(this.currentClusterId);
        this.clientTraces = Array.isArray(data?.records) ? data.records : [];
        if (!this.clientTraceForm.startAt) {
            this.clientTraceForm.startAt = this.defaultTraceStartAt();
        }
    },
    openClientTraceForClient(clientId) {
        this.activeMenu = "trace";
        this.clientTraceForm.clientId = clientId || "";
        this.clientTraceForm.startAt = this.defaultTraceStartAt();
        this.clientTraceForm.durationMinutes = 5;
        this.loadClientTraces();
    },
    async submitClientTrace() {
        const clientId = String(this.clientTraceForm.clientId || "").trim();
        const startAt = new Date(this.clientTraceForm.startAt || "").getTime();
        const durationMinutes = Number(this.clientTraceForm.durationMinutes || 0);
        if (!clientId) {
            this.error = "请输入 clientId";
            return;
        }
        if (!Number.isFinite(startAt)) {
            this.error = "请选择未来追踪开始时间";
            return;
        }
        if (!Number.isFinite(durationMinutes) || durationMinutes <= 0 || durationMinutes > 30) {
            this.error = "追踪时长必须在 1 到 30 分钟之间";
            return;
        }
        try {
            const data = await createClientTrace(this.currentClusterId, {
                clientId,
                startAt,
                durationMinutes
            });
            this.clientTraces = Array.isArray(data?.records) ? data.records : [];
            this.clientTraceForm.startAt = this.defaultTraceStartAt();
            this.message = `已为客户端 ${clientId} 创建日志追踪任务`;
            this.error = "";
        } catch (e) {
            this.error = "创建日志追踪失败: " + e.message;
        }
    },
    async removeClientTrace(task) {
        if (!task?.id) {
            return;
        }
        const messageBox = globalThis.ElementPlus?.ElMessageBox;
        if (messageBox?.confirm) {
            try {
                await messageBox.confirm(
                    `确认删除客户端 ${task.clientId} 的日志追踪任务吗？`,
                    "删除日志追踪任务",
                    {
                        confirmButtonText: "确认删除",
                        cancelButtonText: "取消",
                        type: "warning"
                    }
                );
            } catch (e) {
                return;
            }
        }
        try {
            const data = await deleteClientTrace(this.currentClusterId, task.id);
            this.clientTraces = Array.isArray(data?.records) ? data.records : [];
            this.message = `日志追踪任务 ${task.id} 已删除`;
            this.error = "";
        } catch (e) {
            this.error = "删除日志追踪失败: " + e.message;
        }
    },
    clientTraceStatusLabel(status) {
        if (status === "active") {
            return "进行中";
        }
        if (status === "expired") {
            return "已结束";
        }
        return "待开始";
    }
};
