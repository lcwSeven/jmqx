import {
    createBuiltInUser,
    deleteBuiltInUser,
    fetchBuiltInUsers,
    importBuiltInUsers
} from "../../api.js";

export const builtInUsersPageMethods = {
    normalizeBuiltInUserRecords(records) {
        if (!Array.isArray(records)) {
            return [];
        }
        return records.map(record => ({
            ...record,
            userId: record?.userId || record?.principal || ""
        }));
    },
    async openBuiltInUserManagement() {
        this.activeMenu = "built-in-users";
        this.clearTips();
        await this.loadBuiltInUsers();
    },
    openBuiltInUserCreateDialog() {
        this.builtInUserForm = { userId: "", password: "", superuser: false };
        this.builtInUserDialogs.create = true;
    },
    closeBuiltInUserCreateDialog() {
        this.builtInUserDialogs.create = false;
    },
    openBuiltInUserImportDialog() {
        this.builtInUserImportText = "";
        this.builtInUserImportFile = null;
        this.builtInUserDialogs.import = true;
    },
    closeBuiltInUserImportDialog() {
        this.builtInUserDialogs.import = false;
    },
    handleBuiltInCsvChange(uploadFile) {
        this.builtInUserImportFile = uploadFile?.raw || null;
    },
    removeBuiltInCsvFile() {
        this.builtInUserImportFile = null;
    },
    async loadBuiltInUsers() {
        const data = await fetchBuiltInUsers(this.currentClusterId);
        this.builtInUsers = {
            accountType: data?.accountType || "username",
            passwordHashAlgorithm: data?.passwordHashAlgorithm || "sha256",
            saltPosition: data?.saltPosition || "suffix",
            records: this.normalizeBuiltInUserRecords(data?.records)
        };
    },
    async createBuiltInUserRecord() {
        try {
            await createBuiltInUser(this.currentClusterId, this.builtInUserForm);
            this.builtInUserForm = { userId: "", password: "", superuser: false };
            this.builtInUserDialogs.create = false;
            await this.loadBuiltInUsers();
            this.message = this.tr("builtInUsers.message.created");
            this.error = "";
        } catch (e) {
            this.error = this.tr("builtInUsers.message.createFailed", { message: e.message });
        }
    },
    async importBuiltInUserRecords() {
        try {
            if (!this.builtInUserImportFile) {
                this.error = this.tr("builtInUsers.message.selectCsv");
                return;
            }
            const text = await this.readBuiltInCsvFile(this.builtInUserImportFile);
            const lines = this.parseBuiltInCsvLines(text);
            if (!lines.length) {
                this.error = this.tr("builtInUsers.message.emptyCsv");
                return;
            }
            const result = await importBuiltInUsers(this.currentClusterId, lines);
            this.builtInUserImportText = "";
            this.builtInUserImportFile = null;
            this.builtInUserDialogs.import = false;
            this.builtInUsers = {
                accountType: result?.data?.accountType || this.builtInUsers.accountType,
                passwordHashAlgorithm: result?.data?.passwordHashAlgorithm || this.builtInUsers.passwordHashAlgorithm,
                saltPosition: result?.data?.saltPosition || this.builtInUsers.saltPosition,
                records: this.normalizeBuiltInUserRecords(result?.data?.records)
            };
            this.message = this.tr("builtInUsers.message.imported", {
                count: Number(result?.imported || 0)
            });
            this.error = "";
        } catch (e) {
            this.error = this.tr("builtInUsers.message.importFailed", { message: e.message });
        }
    },
    readBuiltInCsvFile(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(String(reader.result || ""));
            reader.onerror = () => reject(new Error(this.tr("builtInUsers.message.readCsvFailed")));
            reader.readAsText(file, "UTF-8");
        });
    },
    parseBuiltInCsvLines(text) {
        const rows = String(text || "")
            .split(/\r?\n/)
            .map(line => line.trim())
            .filter(Boolean);
        const lines = [];
        rows.forEach((row, index) => {
            const normalized = row.toLowerCase();
            if (index === 0 && (
                normalized === "username,password"
                || normalized === "clientid,password"
                || normalized === "client_id,password"
                || normalized === "username,password,superuser"
                || normalized === "clientid,password,superuser"
                || normalized === "client_id,password,superuser"
            )) {
                return;
            }
            const parts = row.split(",").map(item => item.trim());
            if (parts.length < 2 || !parts[0] || !parts[1]) {
                return;
            }
            const superuser = parts.length >= 3 ? String(parts[2]).toLowerCase() === "true" : false;
            lines.push(parts[0] + "," + parts[1] + "," + superuser);
        });
        return lines;
    },
    async deleteBuiltInUserRecord(userId) {
        try {
            await deleteBuiltInUser(this.currentClusterId, userId);
            await this.loadBuiltInUsers();
            this.message = this.tr("builtInUsers.message.deleted");
            this.error = "";
        } catch (e) {
            this.error = this.tr("builtInUsers.message.deleteFailed", { message: e.message });
        }
    }
};
