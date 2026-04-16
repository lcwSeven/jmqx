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
        clusters: [],
        currentClusterId: "default",
        clusterSelectionTouched: false,
        mqttStatus: "未连接",
        mqttClient: null,
        realtimeNodeMap: {},
        refreshClientsTimer: null,
        overview: {
            totalConnections: 0,
            totalInboundBytes: 0,
            totalOutboundBytes: 0,
            totalConnectAuthFailure: 0,
            totalConnectAuthError: 0,
            totalPublishAclDeny: 0,
            totalPublishAclError: 0,
            maxConnectAuthMs: 0,
            maxPublishAclMs: 0,
            nodes: []
        },
        clients: { records: [], total: 0, pageNo: 1, pageSize: 20 },
        search: { clientId: "", userName: "", pageNo: 1, pageSize: 20 },
        selectedClient: null,
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
            aclEnabled: false,
            aclChain: [],
            aclDefaultAllow: false,
            aclHttp: {
                url: "http://127.0.0.1:8080/acl/check",
                timeoutMs: 2000,
                bodyTemplate: '{\n  "clientId": "${clientId}",\n  "username": "${username}",\n  "topic": "${topic}",\n  "action": "${action}"\n}'
            },
            aclFile: {
                path: "acl-rules.txt"
            },
            aclRedis: {
                host: "127.0.0.1",
                port: 6379,
                password: "",
                db: 0,
                keyPrefix: "jmqx:acl",
                timeoutMs: 2000
            },
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
        bridgeConfig: {
            enabled: false,
            types: [],
            topicFilters: [],
            asyncEnabled: true,
            asyncQueueCapacity: 10000,
            asyncWorkerCount: 1,
            kafka: {
                enabled: false,
                bootstrapServers: "127.0.0.1:9092",
                topic: "jmqx-messages",
                sourceTopicFilters: [],
                acks: "1",
                clientId: "jmqx-bridge",
                compressionType: "none"
            },
            rocketmq: {
                enabled: false,
                nameServer: "127.0.0.1:9876",
                producerGroup: "jmqx-bridge-group",
                topic: "JMQX_MESSAGES",
                sourceTopicFilters: [],
                syncSend: false,
                timeoutMs: 3000
            },
            mysql: {
                enabled: false,
                driver: "",
                url: "jdbc:mysql://127.0.0.1:3306/jmqx",
                user: "root",
                password: "",
                table: "jmqx_bridge_message",
                sourceTopicFilters: [],
                autoCreateTable: true,
                poolMinIdle: 1,
                poolMaxSize: 8,
                poolConnectionTimeoutMs: 3000,
                poolIdleTimeoutMs: 60000,
                poolMaxLifetimeMs: 600000
            }
        },
        bridgeTypeOptions: [
            { key: "kafka", label: "Kafka" },
            { key: "rocketmq", label: "RocketMQ" },
            { key: "mysql", label: "MySQL" }
        ],
        bridgeCreateMode: false,
        bridgeEditingType: "",
        bridgeStep: 1,
        bridgeDatasourceOptions: [
            { key: "kafka", label: "Kafka", icon: "🟠" },
            { key: "rocketmq", label: "RocketMQ", icon: "🚀" },
            { key: "mysql", label: "MySQL", icon: "🐬" }
        ],
        bridgeDraft: {
            datasource: "kafka",
            kafka: {
                enabled: true,
                bootstrapServers: "127.0.0.1:9092",
                topic: "jmqx-messages",
                sourceTopicFilters: [],
                acks: "1",
                clientId: "jmqx-bridge",
                compressionType: "none"
            },
            rocketmq: {
                enabled: true,
                nameServer: "127.0.0.1:9876",
                producerGroup: "jmqx-bridge-group",
                topic: "JMQX_MESSAGES",
                sourceTopicFilters: [],
                syncSend: false,
                timeoutMs: 3000
            },
            mysql: {
                enabled: true,
                driver: "",
                url: "jdbc:mysql://127.0.0.1:3306/jmqx",
                user: "root",
                password: "",
                table: "jmqx_bridge_message",
                sourceTopicFilters: [],
                autoCreateTable: true,
                poolMinIdle: 1,
                poolMaxSize: 8,
                poolConnectionTimeoutMs: 3000,
                poolIdleTimeoutMs: 60000,
                poolMaxLifetimeMs: 600000
            }
        },
        aclCreateMode: false,
        aclEditingPlugin: "",
        aclStep: 1,
        aclMethodOptions: [
            { key: "topic", label: "Topic ACL" }
        ],
        aclDatasourceOptions: [
            { key: "file", label: "文件", icon: "📄" },
            { key: "redis", label: "Redis", icon: "🧱" },
            { key: "http", label: "HTTP 服务", icon: "🌐" }
        ],
        aclDraft: {
            method: "topic",
            datasource: "file",
            cacheTtlMs: 60000,
            defaultAllow: false,
            filePath: "acl-rules.txt",
            httpUrl: "http://127.0.0.1:8080/acl/check",
            httpTimeoutMs: 2000,
            httpBodyTemplate: '{\n  "clientId": "${clientId}",\n  "username": "${username}",\n  "topic": "${topic}",\n  "action": "${action}"\n}',
            redisHost: "127.0.0.1",
            redisPort: 6379,
            redisPassword: "",
            redisDb: 0,
            redisKeyPrefix: "jmqx:acl",
            redisTimeoutMs: 2000
        },
        authCreateMode: false,
        authEditingPlugin: "",
        authStep: 1,
        authMethodOptions: [
            { key: "password", label: "Password-Base" }
        ],
        authDatasourceOptions: [
            { key: "built_in_database", label: "内置数据库", icon: "🗄️" },
            { key: "mysql", label: "MySQL", icon: "🐬" },
            { key: "postgresql", label: "PostgreSQL", icon: "🐘" },
            { key: "redis", label: "Redis", icon: "🧱" },
            { key: "http", label: "HTTP 服务", icon: "🌐" }
        ],
        authDraft: {
            method: "password",
            datasource: "built_in_database",
            cacheTtlMs: 60000,
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
