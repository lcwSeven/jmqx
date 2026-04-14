import { deleteAllBuiltInUsers, fetchSecurityConfig, saveSecurityConfig } from "../../api.js";

export const securityPageMethods = {
    async loadSecurityConfig() {
        const config = await fetchSecurityConfig(this.currentClusterId);
        this.securityConfig = this.normalizeSecurityConfig(config);
    },
    async saveSecurityConfig() {
        try {
            const payload = this.normalizeSecurityConfig(this.securityConfig);
            payload.aclChain = this.toCommaList(payload.aclChain);
            payload.authChain = this.toCommaList(payload.authChain);
            await saveSecurityConfig(this.currentClusterId, payload);
            this.securityConfig = payload;
            await this.loadAuditLogs();
            this.message = "安全配置保存成功";
            this.error = "";
        } catch (e) {
            this.error = "保存安全配置失败: " + e.message;
        }
    },
    async saveAclConfig() {
        await this.saveSecurityConfig();
    },
    openAclCreate() {
        if (!this.availableAclDatasourceOptions.length) {
            this.error = "可用的 ACL 数据源已经全部加入当前 ACL 链";
            return;
        }
        this.aclCreateMode = true;
        this.aclEditingPlugin = "";
        this.aclStep = 1;
        this.aclDraft.method = "topic";
        this.aclDraft.cacheTtlMs = Number(this.securityConfig.cacheTtlMs || 60000);
        const fallbackDatasource = this.availableAclDatasourceOptions.length
            ? this.availableAclDatasourceOptions[0].key
            : this.mapAclPluginToDatasource(this.primaryAclPlugin() || "file");
        this.applyAclDraftFromPlugin(this.mapAclDatasourceToPlugin(fallbackDatasource || "file"));
    },
    cancelAclCreate() {
        this.aclCreateMode = false;
        this.aclEditingPlugin = "";
        this.aclStep = 1;
    },
    selectAclMethod(key) {
        this.aclDraft.method = key;
    },
    selectAclDatasource(key) {
        this.aclDraft.datasource = key;
    },
    previousAclStep() {
        if (this.aclStep > 1) {
            this.aclStep -= 1;
        }
    },
    nextAclStep() {
        if (this.aclStep < 3) {
            this.aclStep += 1;
        }
    },
    aclMethodLabel() {
        const hit = this.aclMethodOptions.find(item => item.key === this.aclDraft.method);
        return hit ? hit.label : "Topic ACL";
    },
    aclDatasourceLabel() {
        const hit = this.aclDatasourceOptions.find(item => item.key === this.aclDraft.datasource);
        return hit ? hit.label : "文件";
    },
    mapAclDatasourceToPlugin(datasource) {
        if (datasource === "http") {
            return "http";
        }
        if (datasource === "redis") {
            return "redis";
        }
        return "file";
    },
    mapAclPluginToDatasource(plugin) {
        if (plugin === "http") {
            return "http";
        }
        if (plugin === "redis") {
            return "redis";
        }
        return "file";
    },
    aclPluginDisplayName(plugin) {
        if (plugin === "http") {
            return "HTTP 服务";
        }
        if (plugin === "redis") {
            return "Redis";
        }
        return "文件";
    },
    describeAclPlugin(plugin) {
        if (plugin === "http") {
            const config = this.securityConfig.aclHttp || {};
            return `${config.url || "-"} · ${config.timeoutMs || 2000}ms`;
        }
        if (plugin === "redis") {
            const redis = this.securityConfig.aclRedis || {};
            return `${redis.host || "127.0.0.1"}:${redis.port || 6379}/${redis.db || 0}`;
        }
        return this.securityConfig.aclFile?.path || "acl-rules.txt";
    },
    primaryAclPlugin() {
        const chain = Array.isArray(this.securityConfig.aclChain)
            ? this.securityConfig.aclChain
            : [];
        if (!chain.length || !chain[0]) {
            return "";
        }
        return String(chain[0]).trim().toLowerCase();
    },
    hasAclRecord() {
        return Array.isArray(this.securityConfig.aclChain) && this.securityConfig.aclChain.length > 0;
    },
    async createAclAndSave() {
        try {
            const plugin = this.mapAclDatasourceToPlugin(this.aclDraft.datasource);
            if (!this.aclEditingPlugin && this.securityConfig.aclChain.includes(plugin)) {
                this.error = "同一种 ACL 数据源已存在，请直接编辑该项配置";
                return;
            }
            this.securityConfig.aclEnabled = true;
            this.securityConfig.cacheTtlMs = Number(this.aclDraft.cacheTtlMs || 60000);
            this.applyAclDraftToSecurityConfig(plugin);
            const currentChain = Array.isArray(this.securityConfig.aclChain) ? [...this.securityConfig.aclChain] : [];
            if (!currentChain.includes(plugin)) {
                currentChain.push(plugin);
            }
            this.securityConfig.aclChain = currentChain;
            await this.saveAclConfig();
            this.aclCreateMode = false;
            this.aclEditingPlugin = "";
            this.aclStep = 1;
            this.message = "ACL 鉴权配置已保存";
            this.error = "";
        } catch (e) {
            this.error = "保存 ACL 配置失败: " + e.message;
        }
    },
    async toggleAclEnabled() {
        await this.saveAclConfig();
    },
    openAclSettings(plugin) {
        const targetPlugin = plugin || this.primaryAclPlugin() || "file";
        this.aclCreateMode = true;
        this.aclEditingPlugin = targetPlugin;
        this.aclStep = 3;
        this.applyAclDraftFromPlugin(targetPlugin);
    },
    async moveAclEntryUp(plugin) {
        const previousChain = Array.isArray(this.securityConfig.aclChain) ? [...this.securityConfig.aclChain] : [];
        const chain = [...previousChain];
        const index = chain.indexOf(plugin);
        if (index <= 0) {
            return;
        }
        [chain[index - 1], chain[index]] = [chain[index], chain[index - 1]];
        this.securityConfig.aclChain = chain;
        this.error = "";
        await this.saveAclConfig();
        if (this.error) {
            this.securityConfig.aclChain = previousChain;
            return;
        }
        this.message = "ACL 链顺序已更新";
    },
    async moveAclEntryDown(plugin) {
        const previousChain = Array.isArray(this.securityConfig.aclChain) ? [...this.securityConfig.aclChain] : [];
        const chain = [...previousChain];
        const index = chain.indexOf(plugin);
        if (index < 0 || index >= chain.length - 1) {
            return;
        }
        [chain[index], chain[index + 1]] = [chain[index + 1], chain[index]];
        this.securityConfig.aclChain = chain;
        this.error = "";
        await this.saveAclConfig();
        if (this.error) {
            this.securityConfig.aclChain = previousChain;
            return;
        }
        this.message = "ACL 链顺序已更新";
    },
    async removeAclEntry(plugin) {
        try {
            this.securityConfig.aclChain = (Array.isArray(this.securityConfig.aclChain) ? this.securityConfig.aclChain : [])
                .filter(item => item !== plugin);
            if (!this.securityConfig.aclChain.length) {
                this.securityConfig.aclEnabled = false;
            }
            await this.saveAclConfig();
            this.message = "ACL 数据源已移除";
            this.error = "";
        } catch (e) {
            this.error = "删除 ACL 数据源失败: " + e.message;
        }
    },
    applyAclDraftFromPlugin(plugin) {
        const normalizedPlugin = plugin || "file";
        this.aclDraft.datasource = this.mapAclPluginToDatasource(normalizedPlugin);
        this.aclDraft.cacheTtlMs = Number(this.securityConfig.cacheTtlMs || 60000);
        this.aclDraft.defaultAllow = this.securityConfig.aclDefaultAllow === true;
        this.aclDraft.filePath = this.securityConfig.aclFile?.path || "acl-rules.txt";
        this.aclDraft.httpUrl = this.securityConfig.aclHttp?.url || "http://127.0.0.1:8080/acl/check";
        this.aclDraft.httpTimeoutMs = Number(this.securityConfig.aclHttp?.timeoutMs || 2000);
        this.aclDraft.httpBodyTemplate = this.securityConfig.aclHttp?.bodyTemplate
            || '{\n  "clientId": "${clientId}",\n  "username": "${username}",\n  "topic": "${topic}",\n  "action": "${action}"\n}';
        this.aclDraft.redisHost = this.securityConfig.aclRedis?.host || "127.0.0.1";
        this.aclDraft.redisPort = Number(this.securityConfig.aclRedis?.port || 6379);
        this.aclDraft.redisPassword = this.securityConfig.aclRedis?.password || "";
        this.aclDraft.redisDb = Number(this.securityConfig.aclRedis?.db || 0);
        this.aclDraft.redisKeyPrefix = this.securityConfig.aclRedis?.keyPrefix || "jmqx:acl";
        this.aclDraft.redisTimeoutMs = Number(this.securityConfig.aclRedis?.timeoutMs || 2000);
    },
    applyAclDraftToSecurityConfig(plugin) {
        this.securityConfig.aclDefaultAllow = this.aclDraft.defaultAllow === true;
        if (plugin === "http") {
            this.securityConfig.aclHttp = {
                url: this.aclDraft.httpUrl || "",
                timeoutMs: Number(this.aclDraft.httpTimeoutMs || 2000),
                bodyTemplate: this.aclDraft.httpBodyTemplate
                    || '{\n  "clientId": "${clientId}",\n  "username": "${username}",\n  "topic": "${topic}",\n  "action": "${action}"\n}'
            };
            return;
        }
        if (plugin === "redis") {
            this.securityConfig.aclRedis = {
                host: this.aclDraft.redisHost || "127.0.0.1",
                port: Number(this.aclDraft.redisPort || 6379),
                password: this.aclDraft.redisPassword || "",
                db: Number(this.aclDraft.redisDb || 0),
                keyPrefix: this.aclDraft.redisKeyPrefix || "jmqx:acl",
                timeoutMs: Number(this.aclDraft.redisTimeoutMs || 2000)
            };
            return;
        }
        this.securityConfig.aclFile = {
            path: this.aclDraft.filePath || "acl-rules.txt"
        };
    },
    async saveAuthConfig() {
        await this.saveSecurityConfig();
    },
    openAuthCreate() {
        this.authCreateMode = true;
        this.authEditingPlugin = "";
        this.authStep = 1;
        this.authDraft.method = "password";
        this.authDraft.cacheTtlMs = Number(this.securityConfig.cacheTtlMs || 60000);
        const primary = this.primaryAuthPlugin();
        const fallbackDatasource = this.availableAuthDatasourceOptions.length
            ? this.availableAuthDatasourceOptions[0].key
            : this.mapPluginToDatasource(primary);
        this.applyAuthDraftFromPlugin(this.mapDatasourceToPlugin(fallbackDatasource || "file"));
    },
    cancelAuthCreate() {
        this.authCreateMode = false;
        this.authEditingPlugin = "";
        this.authStep = 1;
    },
    selectAuthMethod(key) {
        this.authDraft.method = key;
    },
    selectAuthDatasource(key) {
        this.authDraft.datasource = key;
    },
    previousAuthStep() {
        if (this.authStep > 1) {
            this.authStep -= 1;
        }
    },
    nextAuthStep() {
        if (this.authStep < 3) {
            this.authStep += 1;
        }
    },
    authStepDone(step) {
        return this.authStep > step;
    },
    authMethodLabel() {
        const hit = this.authMethodOptions.find(item => item.key === this.authDraft.method);
        return hit ? hit.label : "Password-Base";
    },
    authDatasourceLabel() {
        const hit = this.authDatasourceOptions.find(item => item.key === this.authDraft.datasource);
        return hit ? hit.label : "文件";
    },
    parseHttpHeaders(text) {
        const rows = String(text || "")
            .split(/\r?\n/)
            .map(line => line.trim())
            .filter(Boolean)
            .map(line => {
                const split = line.indexOf(":");
                if (split <= 0) {
                    return null;
                }
                return {
                    key: line.substring(0, split).trim(),
                    value: line.substring(split + 1).trim()
                };
            })
            .filter(Boolean);
        return rows.length ? rows : [{ key: "content-type", value: "application/json" }];
    },
    serializeHttpHeaders(headers) {
        if (!Array.isArray(headers)) {
            return "content-type: application/json";
        }
        const rows = headers
            .map(header => ({
                key: String(header?.key || "").trim(),
                value: String(header?.value || "").trim()
            }))
            .filter(header => header.key);
        return rows.length
            ? rows.map(header => `${header.key}: ${header.value}`).join("\n")
            : "content-type: application/json";
    },
    addHttpHeaderRow() {
        this.authDraft.httpHeaders.push({ key: "", value: "" });
    },
    removeHttpHeaderRow(index) {
        if (!Array.isArray(this.authDraft.httpHeaders) || this.authDraft.httpHeaders.length <= 1) {
            this.authDraft.httpHeaders = [{ key: "content-type", value: "application/json" }];
            return;
        }
        this.authDraft.httpHeaders.splice(index, 1);
    },
    mapDatasourceToPlugin(datasource) {
        if (datasource === "built_in_database") {
            return "built_in_database";
        }
        if (datasource === "http") {
            return "http";
        }
        if (datasource === "redis") {
            return "redis";
        }
        if (datasource === "file") {
            return "file";
        }
        if (datasource === "postgresql") {
            return "postgresql";
        }
        return "mysql";
    },
    mapPluginToDatasource(plugin) {
        if (!plugin) {
            return "file";
        }
        if (plugin === "built_in_database") {
            return "built_in_database";
        }
        if (plugin === "http") {
            return "http";
        }
        if (plugin === "redis") {
            return "redis";
        }
        return plugin === "postgresql" ? "postgresql" : "mysql";
    },
    authPluginDisplayName(plugin) {
        if (plugin === "built_in_database") {
            return "内置数据库";
        }
        if (plugin === "file") {
            return "文件";
        }
        if (plugin === "http") {
            return "HTTP 服务";
        }
        if (plugin === "redis") {
            return "Redis";
        }
        if (plugin === "mysql") {
            return "MySQL";
        }
        if (plugin === "postgresql") {
            return "PostgreSQL";
        }
        return plugin || "-";
    },
    describeAuthPlugin(plugin) {
        if (plugin === "built_in_database") {
            const config = this.securityConfig.authBuiltInDatabase || {};
            return `${config.accountType || "username"} · ${config.passwordHashAlgorithm || "plain"} · ${config.saltPosition || "disable"}`;
        }
        if (plugin === "file") {
            return this.securityConfig.authFile?.path || "auth-users.txt";
        }
        if (plugin === "http") {
            return this.securityConfig.authHttp?.url || "-";
        }
        if (plugin === "redis") {
            const redis = this.securityConfig.authRedis || {};
            return `${redis.host || "127.0.0.1"}:${redis.port || 6379}/${redis.db || 0}`;
        }
        if (plugin === "mysql") {
            return this.securityConfig.authMysql?.url || "-";
        }
        if (plugin === "postgresql") {
            return this.securityConfig.authPostgresql?.url || "-";
        }
        return plugin || "-";
    },
    primaryAuthPlugin() {
        const chain = Array.isArray(this.securityConfig.authChain)
            ? this.securityConfig.authChain
            : [];
        if (!chain.length || !chain[0]) {
            return "";
        }
        return String(chain[0]).trim().toLowerCase();
    },
    hasAuthRecord() {
        return Array.isArray(this.securityConfig.authChain) && this.securityConfig.authChain.length > 0;
    },
    async createAuthAndSave() {
        try {
            const plugin = this.mapDatasourceToPlugin(this.authDraft.datasource);
            if (!this.authEditingPlugin && this.securityConfig.authChain.includes(plugin)) {
                this.error = "同一种鉴权方式已存在，请直接编辑该项配置";
                return;
            }
            this.securityConfig.authEnabled = true;
            this.securityConfig.cacheTtlMs = Number(this.authDraft.cacheTtlMs || 60000);
            this.applyAuthDraftToSecurityConfig(plugin);
            const currentChain = Array.isArray(this.securityConfig.authChain) ? [...this.securityConfig.authChain] : [];
            if (!currentChain.includes(plugin)) {
                currentChain.push(plugin);
            }
            this.securityConfig.authChain = currentChain;
            await this.saveAuthConfig();
            this.authCreateMode = false;
            this.authEditingPlugin = "";
            this.authStep = 1;
            this.message = "连接鉴权配置已保存";
        } catch (e) {
            this.error = "创建认证失败: " + e.message;
        }
    },
    async toggleAuthEnabled() {
        await this.saveAuthConfig();
    },
    openAuthSettings(plugin) {
        const targetPlugin = plugin || this.primaryAuthPlugin();
        this.authCreateMode = true;
        this.authEditingPlugin = targetPlugin;
        this.authStep = 3;
        this.applyAuthDraftFromPlugin(targetPlugin);
    },
    moveAuthEntryUp(plugin) {
        const chain = Array.isArray(this.securityConfig.authChain) ? [...this.securityConfig.authChain] : [];
        const index = chain.indexOf(plugin);
        if (index <= 0) {
            return;
        }
        [chain[index - 1], chain[index]] = [chain[index], chain[index - 1]];
        this.securityConfig.authChain = chain;
    },
    moveAuthEntryDown(plugin) {
        const chain = Array.isArray(this.securityConfig.authChain) ? [...this.securityConfig.authChain] : [];
        const index = chain.indexOf(plugin);
        if (index < 0 || index >= chain.length - 1) {
            return;
        }
        [chain[index], chain[index + 1]] = [chain[index + 1], chain[index]];
        this.securityConfig.authChain = chain;
    },
    async confirmDangerAction(title, message) {
        const messageBox = globalThis.ElementPlus?.ElMessageBox;
        if (messageBox?.confirm) {
            try {
                await messageBox.confirm(message, title, {
                    confirmButtonText: "确认删除",
                    cancelButtonText: "取消",
                    type: "warning",
                    confirmButtonClass: "el-button--danger"
                });
                return true;
            } catch (e) {
                return false;
            }
        }
        return globalThis.confirm(message);
    },
    async removeAuthEntry(plugin) {
        try {
            if (plugin === "built_in_database") {
                const confirmed = await this.confirmDangerAction(
                    "删除内置数据库鉴权",
                    "删除后会同时清空内置数据库中的全部用户数据，且无法恢复。是否继续？"
                );
                if (!confirmed) {
                    return;
                }
                await deleteAllBuiltInUsers(this.currentClusterId);
            }
            const chain = (Array.isArray(this.securityConfig.authChain) ? this.securityConfig.authChain : [])
                .filter(item => item !== plugin);
            this.securityConfig.authChain = chain;
            if (!chain.length) {
                this.securityConfig.authEnabled = false;
            }
            await this.saveAuthConfig();
            if (this.activeMenu === "built-in-users") {
                this.activeMenu = "auth";
            }
            this.message = plugin === "built_in_database"
                ? "已删除内置数据库鉴权，并清空全部内置数据库用户"
                : "认证方式已移除";
            this.error = "";
        } catch (e) {
            this.error = "删除认证方式失败: " + e.message;
        }
    },
    applyAuthDraftFromPlugin(plugin) {
        const normalizedPlugin = plugin || "file";
        this.authDraft.datasource = this.mapPluginToDatasource(normalizedPlugin);
        this.authDraft.cacheTtlMs = Number(this.securityConfig.cacheTtlMs || 60000);
        this.authDraft.filePath = this.securityConfig.authFile?.path || "auth-users.txt";
        this.authDraft.builtInDatabaseAccountType = this.securityConfig.authBuiltInDatabase?.accountType || "username";
        this.authDraft.builtInDatabasePasswordHashAlgorithm = this.securityConfig.authBuiltInDatabase?.passwordHashAlgorithm || "sha256";
        this.authDraft.builtInDatabaseSaltPosition = this.securityConfig.authBuiltInDatabase?.saltPosition || "suffix";
        this.authDraft.httpMethod = this.securityConfig.authHttp?.method || "POST";
        this.authDraft.httpUrl = this.securityConfig.authHttp?.url || "http://127.0.0.1:8080/auth/check";
        this.authDraft.httpHeaders = this.parseHttpHeaders(this.securityConfig.authHttp?.headersText || "content-type: application/json");
        this.authDraft.httpTlsEnabled = this.securityConfig.authHttp?.tlsEnabled === true;
        this.authDraft.httpBodyTemplate = this.securityConfig.authHttp?.bodyTemplate || '{\n  "username": "${username}",\n  "password": "${password}"\n}';
        this.authDraft.httpPoolSize = Number(this.securityConfig.authHttp?.poolSize || 4);
        this.authDraft.httpRateLimitPerSecond = Number(this.securityConfig.authHttp?.rateLimitPerSecond || 0);
        this.authDraft.httpRequestTimeoutMs = Number(this.securityConfig.authHttp?.requestTimeoutMs || 2000);
        this.authDraft.httpConnectTimeoutMs = Number(this.securityConfig.authHttp?.connectTimeoutMs || 1500);
        this.authDraft.httpPipelineCount = Number(this.securityConfig.authHttp?.pipelineCount || 2);
        this.authDraft.redisHost = this.securityConfig.authRedis?.host || "127.0.0.1";
        this.authDraft.redisPort = Number(this.securityConfig.authRedis?.port || 6379);
        this.authDraft.redisPassword = this.securityConfig.authRedis?.password || "";
        this.authDraft.redisDb = Number(this.securityConfig.authRedis?.db || 0);
        this.authDraft.redisKeyPrefix = this.securityConfig.authRedis?.keyPrefix || "jmqx:auth";
        this.authDraft.redisTimeoutMs = Number(this.securityConfig.authRedis?.timeoutMs || 2000);
        this.authDraft.mysqlUrl = this.securityConfig.authMysql?.url || "jdbc:mysql://127.0.0.1:3306/jmqx";
        this.authDraft.mysqlUser = this.securityConfig.authMysql?.user || "root";
        this.authDraft.mysqlPassword = this.securityConfig.authMysql?.password || "";
        this.authDraft.mysqlQuery = this.securityConfig.authMysql?.query || "SELECT password FROM mqtt_user WHERE username = ?";
        this.authDraft.mysqlPoolMinIdle = Number(this.securityConfig.authMysql?.poolMinIdle || 1);
        this.authDraft.mysqlPoolMaxSize = Number(this.securityConfig.authMysql?.poolMaxSize || 8);
        this.authDraft.mysqlPoolConnectionTimeoutMs = Number(this.securityConfig.authMysql?.poolConnectionTimeoutMs || 3000);
        this.authDraft.mysqlPoolIdleTimeoutMs = Number(this.securityConfig.authMysql?.poolIdleTimeoutMs || 60000);
        this.authDraft.mysqlPoolMaxLifetimeMs = Number(this.securityConfig.authMysql?.poolMaxLifetimeMs || 600000);
        this.authDraft.postgresqlUrl = this.securityConfig.authPostgresql?.url || "jdbc:postgresql://127.0.0.1:5432/jmqx";
        this.authDraft.postgresqlUser = this.securityConfig.authPostgresql?.user || "postgres";
        this.authDraft.postgresqlPassword = this.securityConfig.authPostgresql?.password || "";
        this.authDraft.postgresqlQuery = this.securityConfig.authPostgresql?.query || "SELECT password FROM mqtt_user WHERE username = ?";
        this.authDraft.postgresqlPoolMinIdle = Number(this.securityConfig.authPostgresql?.poolMinIdle || 1);
        this.authDraft.postgresqlPoolMaxSize = Number(this.securityConfig.authPostgresql?.poolMaxSize || 8);
        this.authDraft.postgresqlPoolConnectionTimeoutMs = Number(this.securityConfig.authPostgresql?.poolConnectionTimeoutMs || 3000);
        this.authDraft.postgresqlPoolIdleTimeoutMs = Number(this.securityConfig.authPostgresql?.poolIdleTimeoutMs || 60000);
        this.authDraft.postgresqlPoolMaxLifetimeMs = Number(this.securityConfig.authPostgresql?.poolMaxLifetimeMs || 600000);
    },
    applyAuthDraftToSecurityConfig(plugin) {
        if (plugin === "built_in_database") {
            this.securityConfig.authBuiltInDatabase = {
                accountType: this.authDraft.builtInDatabaseAccountType || "username",
                passwordHashAlgorithm: this.authDraft.builtInDatabasePasswordHashAlgorithm || "sha256",
                saltPosition: this.authDraft.builtInDatabaseSaltPosition || "suffix"
            };
            return;
        }
        if (plugin === "file") {
            this.securityConfig.authFile = {
                path: this.authDraft.filePath || "auth-users.txt"
            };
            return;
        }
        if (plugin === "http") {
            this.securityConfig.authHttp = {
                method: this.authDraft.httpMethod || "POST",
                url: this.authDraft.httpUrl || "",
                headersText: this.serializeHttpHeaders(this.authDraft.httpHeaders),
                tlsEnabled: this.authDraft.httpTlsEnabled === true,
                bodyTemplate: this.authDraft.httpBodyTemplate || "",
                poolSize: Number(this.authDraft.httpPoolSize || 4),
                rateLimitPerSecond: Number(this.authDraft.httpRateLimitPerSecond || 0),
                requestTimeoutMs: Number(this.authDraft.httpRequestTimeoutMs || 2000),
                connectTimeoutMs: Number(this.authDraft.httpConnectTimeoutMs || 1500),
                pipelineCount: Number(this.authDraft.httpPipelineCount || 2)
            };
            return;
        }
        if (plugin === "redis") {
            this.securityConfig.authRedis = {
                host: this.authDraft.redisHost || "127.0.0.1",
                port: Number(this.authDraft.redisPort || 6379),
                password: this.authDraft.redisPassword || "",
                db: Number(this.authDraft.redisDb || 0),
                keyPrefix: this.authDraft.redisKeyPrefix || "jmqx:auth",
                timeoutMs: Number(this.authDraft.redisTimeoutMs || 2000)
            };
            return;
        }
        if (plugin === "postgresql") {
            this.securityConfig.authPostgresql = {
                url: this.authDraft.postgresqlUrl || "",
                user: this.authDraft.postgresqlUser || "",
                password: this.authDraft.postgresqlPassword || "",
                query: this.authDraft.postgresqlQuery || "",
                poolMinIdle: Number(this.authDraft.postgresqlPoolMinIdle || 1),
                poolMaxSize: Number(this.authDraft.postgresqlPoolMaxSize || 8),
                poolConnectionTimeoutMs: Number(this.authDraft.postgresqlPoolConnectionTimeoutMs || 3000),
                poolIdleTimeoutMs: Number(this.authDraft.postgresqlPoolIdleTimeoutMs || 60000),
                poolMaxLifetimeMs: Number(this.authDraft.postgresqlPoolMaxLifetimeMs || 600000)
            };
            return;
        }
        this.securityConfig.authMysql = {
            url: this.authDraft.mysqlUrl || "",
            user: this.authDraft.mysqlUser || "",
            password: this.authDraft.mysqlPassword || "",
            query: this.authDraft.mysqlQuery || "",
            poolMinIdle: Number(this.authDraft.mysqlPoolMinIdle || 1),
            poolMaxSize: Number(this.authDraft.mysqlPoolMaxSize || 8),
            poolConnectionTimeoutMs: Number(this.authDraft.mysqlPoolConnectionTimeoutMs || 3000),
            poolIdleTimeoutMs: Number(this.authDraft.mysqlPoolIdleTimeoutMs || 60000),
            poolMaxLifetimeMs: Number(this.authDraft.mysqlPoolMaxLifetimeMs || 600000)
        };
    },
    normalizeSecurityConfig(config = {}) {
        const normalizedAclChain = (Array.isArray(config.aclChain)
            ? config.aclChain
            : this.toCommaList(config.aclChain || ""))
            .map(item => String(item || "").trim().toLowerCase())
            .filter((item, index, array) => item && item !== "allow_all" && array.indexOf(item) === index);
        const normalizedAuthChain = (Array.isArray(config.authChain)
            ? config.authChain
            : this.toCommaList(config.authChain || ""))
            .map(item => String(item || "").trim().toLowerCase())
            .filter(item => item && item !== "allow_all");
        return {
            aclEnabled: config.aclEnabled === true && normalizedAclChain.length > 0,
            aclChain: normalizedAclChain,
            aclDefaultAllow: config.aclDefaultAllow === true,
            aclHttp: {
                url: config.aclHttp?.url || "http://127.0.0.1:8080/acl/check",
                timeoutMs: Number(config.aclHttp?.timeoutMs || 2000),
                bodyTemplate: config.aclHttp?.bodyTemplate || '{\n  "clientId": "${clientId}",\n  "username": "${username}",\n  "topic": "${topic}",\n  "action": "${action}"\n}'
            },
            aclFile: {
                path: config.aclFile?.path || "acl-rules.txt"
            },
            aclRedis: {
                host: config.aclRedis?.host || "127.0.0.1",
                port: Number(config.aclRedis?.port || 6379),
                password: config.aclRedis?.password || "",
                db: Number(config.aclRedis?.db || 0),
                keyPrefix: config.aclRedis?.keyPrefix || "jmqx:acl",
                timeoutMs: Number(config.aclRedis?.timeoutMs || 2000)
            },
            authEnabled: config.authEnabled === true && normalizedAuthChain.length > 0,
            authChain: normalizedAuthChain,
            cacheTtlMs: Number(config.cacheTtlMs || 60000),
            authHttp: {
                method: config.authHttp?.method || "POST",
                url: config.authHttp?.url || "http://127.0.0.1:8080/auth/check",
                headersText: config.authHttp?.headersText || "content-type: application/json",
                tlsEnabled: config.authHttp?.tlsEnabled === true,
                bodyTemplate: config.authHttp?.bodyTemplate || '{\n  "username": "${username}",\n  "password": "${password}"\n}',
                poolSize: Number(config.authHttp?.poolSize || 4),
                rateLimitPerSecond: Number(config.authHttp?.rateLimitPerSecond || 0),
                requestTimeoutMs: Number(config.authHttp?.requestTimeoutMs || config.authHttp?.timeoutMs || 2000),
                connectTimeoutMs: Number(config.authHttp?.connectTimeoutMs || 1500),
                pipelineCount: Number(config.authHttp?.pipelineCount || 2)
            },
            authFile: {
                path: config.authFile?.path || "auth-users.txt"
            },
            authBuiltInDatabase: {
                accountType: config.authBuiltInDatabase?.accountType || "username",
                passwordHashAlgorithm: config.authBuiltInDatabase?.passwordHashAlgorithm || "sha256",
                saltPosition: config.authBuiltInDatabase?.saltPosition || "suffix"
            },
            authRedis: {
                host: config.authRedis?.host || "127.0.0.1",
                port: Number(config.authRedis?.port || 6379),
                password: config.authRedis?.password || "",
                db: Number(config.authRedis?.db || 0),
                keyPrefix: config.authRedis?.keyPrefix || "jmqx:auth",
                timeoutMs: Number(config.authRedis?.timeoutMs || 2000)
            },
            authMysql: {
                url: config.authMysql?.url || "jdbc:mysql://127.0.0.1:3306/jmqx",
                user: config.authMysql?.user || "root",
                password: config.authMysql?.password || "",
                query: config.authMysql?.query || "SELECT password FROM mqtt_user WHERE username = ?",
                poolMinIdle: Number(config.authMysql?.poolMinIdle || 1),
                poolMaxSize: Number(config.authMysql?.poolMaxSize || 8),
                poolConnectionTimeoutMs: Number(config.authMysql?.poolConnectionTimeoutMs || 3000),
                poolIdleTimeoutMs: Number(config.authMysql?.poolIdleTimeoutMs || 60000),
                poolMaxLifetimeMs: Number(config.authMysql?.poolMaxLifetimeMs || 600000)
            },
            authPostgresql: {
                url: config.authPostgresql?.url || "jdbc:postgresql://127.0.0.1:5432/jmqx",
                user: config.authPostgresql?.user || "postgres",
                password: config.authPostgresql?.password || "",
                query: config.authPostgresql?.query || "SELECT password FROM mqtt_user WHERE username = ?",
                poolMinIdle: Number(config.authPostgresql?.poolMinIdle || 1),
                poolMaxSize: Number(config.authPostgresql?.poolMaxSize || 8),
                poolConnectionTimeoutMs: Number(config.authPostgresql?.poolConnectionTimeoutMs || 3000),
                poolIdleTimeoutMs: Number(config.authPostgresql?.poolIdleTimeoutMs || 60000),
                poolMaxLifetimeMs: Number(config.authPostgresql?.poolMaxLifetimeMs || 600000)
            }
        };
    },
    primaryPluginFromChain(chain) {
        const values = Array.isArray(chain) ? chain : this.toCommaList(chain || "");
        if (!values.length) {
            return "";
        }
        return String(values[0] || "").trim().toLowerCase();
    }
};
