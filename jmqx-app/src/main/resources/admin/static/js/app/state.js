export function createInitialState() {
    return {
        activeMenu: "overview",
        message: "",
        error: "",
        adminAuthRequired: false,
        adminAuthenticated: false,
        adminSession: {
            authenticated: false,
            username: "",
            role: "",
            superAdmin: false,
            permissions: []
        },
        adminLoginForm: {
            username: "",
            password: ""
        },
        adminDialogs: {
            password: false
        },
        adminPasswordForm: {
            currentPassword: "",
            newPassword: "",
            confirmPassword: ""
        },
        currentClusterId: "default",
        mqttStatus: "未连接",
        mqttClient: null,
        realtimeNodeMap: {},
        refreshClientsTimer: null,
        overview: { totalConnections: 0, totalInboundBytes: 0, totalOutboundBytes: 0, nodes: [] },
        clients: { records: [], total: 0, pageNo: 1, pageSize: 20 },
        search: { clientId: "", userName: "", pageNo: 1, pageSize: 20 },
        selectedClient: null,
        clientTraces: [],
        clientTraceForm: {
            clientId: "",
            startAt: "",
            durationMinutes: 5
        },
        blacklistEntries: [],
        blacklistForm: {
            type: "clientId",
            value: ""
        },
        builtInUsers: {
            accountType: "username",
            passwordHashAlgorithm: "sha256",
            saltPosition: "suffix",
            records: []
        },
        builtInUserForm: {
            userId: "",
            password: "",
            superuser: false
        },
        builtInUserImportText: "",
        builtInUserImportFile: null,
        builtInUserDialogs: {
            create: false,
            import: false
        },
        auditLogs: [],
        auditFilter: "all",
        expandedAuditIds: [],
        securityConfig: {
            aclEnabled: true,
            aclChain: ["file"],
            authEnabled: false,
            authChain: [],
            cacheTtlMs: 60000,
            authHttp: {
                method: "POST",
                url: "http://127.0.0.1:8080/auth/check",
                headersText: "content-type: application/json",
                tlsEnabled: false,
                bodyTemplate: '{\n  "username": "${username}",\n  "password": "${password}"\n}',
                poolSize: 4,
                rateLimitPerSecond: 0,
                requestTimeoutMs: 2000,
                connectTimeoutMs: 1500,
                pipelineCount: 2
            },
            authFile: { path: "auth-users.txt" },
            authBuiltInDatabase: {
                accountType: "username",
                passwordHashAlgorithm: "sha256",
                saltPosition: "suffix"
            },
            authRedis: { host: "127.0.0.1", port: 6379, password: "", db: 0, keyPrefix: "jmqx:auth", timeoutMs: 2000 },
            authMysql: {
                url: "jdbc:mysql://127.0.0.1:3306/jmqx",
                user: "root",
                password: "",
                query: "SELECT password FROM mqtt_user WHERE username = ?",
                poolMinIdle: 1,
                poolMaxSize: 8,
                poolConnectionTimeoutMs: 3000,
                poolIdleTimeoutMs: 60000,
                poolMaxLifetimeMs: 600000
            },
            authPostgresql: {
                url: "jdbc:postgresql://127.0.0.1:5432/jmqx",
                user: "postgres",
                password: "",
                query: "SELECT password FROM mqtt_user WHERE username = ?",
                poolMinIdle: 1,
                poolMaxSize: 8,
                poolConnectionTimeoutMs: 3000,
                poolIdleTimeoutMs: 60000,
                poolMaxLifetimeMs: 600000
            }
        },
        clusterConfig: { coreNodes: ["core-1:9801"], replicantNodes: [], coreAcceptClientConnections: true, sharedSubscriptionMaxMembersPerGroup: 10000 },
        authCreateMode: false,
        authEditingPlugin: "",
        authStep: 1,
        authMethodOptions: [
            { key: "password", label: "Password-Base" }
        ],
        authDatasourceOptions: [
            { key: "built_in_database", label: "内置数据库", icon: "🗄️" },
            { key: "file", label: "文件", icon: "📄" },
            { key: "mysql", label: "MySQL", icon: "🐬" },
            { key: "postgresql", label: "PostgreSQL", icon: "🐘" },
            { key: "redis", label: "Redis", icon: "🧱" },
            { key: "http", label: "HTTP 服务", icon: "🌐" }
        ],
        authDraft: {
            method: "password",
            datasource: "file",
            cacheTtlMs: 60000,
            filePath: "auth-users.txt",
            builtInDatabaseAccountType: "username",
            builtInDatabasePasswordHashAlgorithm: "sha256",
            builtInDatabaseSaltPosition: "suffix",
            httpMethod: "POST",
            httpUrl: "http://127.0.0.1:8080/auth/check",
            httpHeaders: [{ key: "content-type", value: "application/json" }],
            httpTlsEnabled: false,
            httpBodyTemplate: '{\n  "username": "${username}",\n  "password": "${password}"\n}',
            httpPoolSize: 4,
            httpRateLimitPerSecond: 0,
            httpRequestTimeoutMs: 2000,
            httpConnectTimeoutMs: 1500,
            httpPipelineCount: 2,
            redisHost: "127.0.0.1",
            redisPort: 6379,
            redisPassword: "",
            redisDb: 0,
            redisKeyPrefix: "jmqx:auth",
            redisTimeoutMs: 2000,
            mysqlUrl: "jdbc:mysql://127.0.0.1:3306/jmqx",
            mysqlUser: "root",
            mysqlPassword: "",
            mysqlQuery: "SELECT password FROM mqtt_user WHERE username = ?",
            mysqlPoolMinIdle: 1,
            mysqlPoolMaxSize: 8,
            mysqlPoolConnectionTimeoutMs: 3000,
            mysqlPoolIdleTimeoutMs: 60000,
            mysqlPoolMaxLifetimeMs: 600000,
            postgresqlUrl: "jdbc:postgresql://127.0.0.1:5432/jmqx",
            postgresqlUser: "postgres",
            postgresqlPassword: "",
            postgresqlQuery: "SELECT password FROM mqtt_user WHERE username = ?",
            postgresqlPoolMinIdle: 1,
            postgresqlPoolMaxSize: 8,
            postgresqlPoolConnectionTimeoutMs: 3000,
            postgresqlPoolIdleTimeoutMs: 60000,
            postgresqlPoolMaxLifetimeMs: 600000
        }
    };
}
