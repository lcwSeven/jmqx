import { createInitialState } from "./state.js";
import { adminComputed } from "./computed.js";
import { commonMethods } from "./shared/common.js";
import { dashboardMethods } from "./shared/dashboard.js";
import { overviewPageMethods } from "./pages/overview.js";
import { clientsPageMethods } from "./pages/clients.js";
import { securityPageMethods } from "./pages/security.js";
import { builtInUsersPageMethods } from "./pages/built-in-users.js";
import { clusterPageMethods } from "./pages/cluster.js";
import { auditPageMethods } from "./pages/audit.js";
import { adminTemplate } from "./template.js";
import { getStoredAdminAuth } from "../api.js";

const { createApp } = Vue;

export function createAdminApp() {
    const app = createApp({
        data: createInitialState,
        computed: adminComputed,
        async mounted() {
            const auth = getStoredAdminAuth();
            this.adminLoginForm.username = auth.username || "";
            this.adminLoginForm.password = auth.password || "";
            try {
                await this.reloadCurrentClusterData();
            } finally {
                if (this.adminAuthenticated) {
                    this.connectDashboardStream();
                } else {
                    this.disconnectDashboardStream();
                }
            }
        },
        beforeUnmount() {
            this.disconnectDashboardStream();
            if (this.refreshClientsTimer) {
                clearTimeout(this.refreshClientsTimer);
                this.refreshClientsTimer = null;
            }
        },
        methods: {
            ...commonMethods,
            ...dashboardMethods,
            ...overviewPageMethods,
            ...clientsPageMethods,
            ...securityPageMethods,
            ...builtInUsersPageMethods,
            ...clusterPageMethods,
            ...auditPageMethods
        },
        template: adminTemplate
    });
    app.use(ElementPlus, { size: "large" });
    return app;
}
