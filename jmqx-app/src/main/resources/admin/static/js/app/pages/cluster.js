import { fetchClusterConfig, saveClusterConfig } from "../../api.js";

export const clusterPageMethods = {
    async loadClusterConfig() {
        this.clusterConfig = await fetchClusterConfig(this.currentClusterId);
    },
    async saveClusterConfig() {
        try {
            const payload = {
                ...this.clusterConfig,
                coreNodes: this.toLineList(this.clusterConfig.coreNodes),
                replicantNodes: this.toLineList(this.clusterConfig.replicantNodes)
            };
            await saveClusterConfig(this.currentClusterId, payload);
            await this.loadAuditLogs();
            this.message = "集群部署配置已保存，重启或重部署后生效";
            this.error = "";
        } catch (e) {
            this.error = "保存集群部署配置失败: " + e.message;
        }
    }
};
