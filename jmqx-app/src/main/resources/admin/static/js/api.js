const API_BASE = "api/v1";
const ADMIN_AUTH_STORAGE_KEY = "jmqx_admin_auth";

function resolveCandidates(url) {
    const candidates = [url];
    if (typeof url === "string" && url && !url.startsWith("/")) {
        candidates.push("/" + url.replace(/^\/+/, ""));
    }
    return [...new Set(candidates)];
}

function normalizeErrorText(text) {
    if (!text) {
        return "request failed";
    }
    try {
        const parsed = JSON.parse(text);
        return parsed?.error || text;
    } catch (e) {
        return text;
    }
}

export async function request(url, options = {}) {
    const candidates = resolveCandidates(url);
    let lastError = null;
    for (let index = 0; index < candidates.length; index++) {
        const candidate = candidates[index];
        const authHeaders = getAdminAuthHeader();
        const mergedHeaders = { "Content-Type": "application/json", ...authHeaders, ...(options.headers || {}) };
        const response = await fetch(candidate, {
            ...options
            ,
            headers: mergedHeaders
        });
        if (response.ok) {
            const text = await response.text();
            return text ? JSON.parse(text) : null;
        }
        const errorText = normalizeErrorText(await response.text());
        lastError = new Error(candidate + " -> HTTP " + response.status + ": " + errorText);
        lastError.status = response.status;
        if (response.status !== 404 || index === candidates.length - 1) {
            throw lastError;
        }
    }
    throw lastError || new Error("request failed");
}

export function getStoredAdminAuth() {
    try {
        const raw = window.sessionStorage.getItem(ADMIN_AUTH_STORAGE_KEY);
        if (!raw) {
            return { username: "", password: "" };
        }
        const parsed = JSON.parse(raw);
        return {
            username: String(parsed?.username || ""),
            password: String(parsed?.password || "")
        };
    } catch (e) {
        return { username: "", password: "" };
    }
}

export function storeAdminAuth(username, password) {
    window.sessionStorage.setItem(ADMIN_AUTH_STORAGE_KEY, JSON.stringify({
        username: String(username || ""),
        password: String(password || "")
    }));
}

export function clearStoredAdminAuth() {
    window.sessionStorage.removeItem(ADMIN_AUTH_STORAGE_KEY);
}

function getAdminAuthHeader() {
    const auth = getStoredAdminAuth();
    if (!auth.username) {
        return {};
    }
    return {
        Authorization: "Basic " + window.btoa(`${auth.username}:${auth.password || ""}`)
    };
}

export function fetchClusters() {
    return request(API_BASE + "/clusters");
}

export function fetchAdminSession() {
    return request(API_BASE + "/admin/session");
}

export function changeAdminPassword(payload) {
    return request(API_BASE + "/admin/password", {
        method: "PUT",
        body: JSON.stringify(payload)
    });
}

export function createCluster(payload) {
    return request(API_BASE + "/clusters", {
        method: "POST",
        body: JSON.stringify(payload)
    });
}

export function fetchOverview(clusterId) {
    return request(API_BASE + "/cluster/overview?clusterId=" + encodeURIComponent(clusterId));
}

export function fetchClients(clusterId, search) {
    const query = new URLSearchParams({
        clusterId: clusterId || "default",
        clientId: search.clientId || "",
        userName: search.userName || "",
        pageNo: String(search.pageNo || 1),
        pageSize: String(search.pageSize || 20)
    });
    return request(API_BASE + "/clients?" + query.toString());
}

export function fetchClientDetail(clusterId, clientId) {
    return request(API_BASE + "/clients/" + encodeURIComponent(clientId) + "?clusterId=" + encodeURIComponent(clusterId));
}

export function fetchSecurityConfig(clusterId) {
    return request(API_BASE + "/security/config?clusterId=" + encodeURIComponent(clusterId));
}

export function saveSecurityConfig(clusterId, payload) {
    return request(API_BASE + "/security/config?clusterId=" + encodeURIComponent(clusterId), {
        method: "PUT",
        body: JSON.stringify(payload)
    });
}

export function fetchClusterConfig(clusterId) {
    return request(API_BASE + "/cluster/config?clusterId=" + encodeURIComponent(clusterId));
}

export function saveClusterConfig(clusterId, payload) {
    return request(API_BASE + "/cluster/config?clusterId=" + encodeURIComponent(clusterId), {
        method: "PUT",
        body: JSON.stringify(payload)
    });
}

export function fetchClusterFullConfig(clusterId) {
    return request(API_BASE + "/cluster/full-config?clusterId=" + encodeURIComponent(clusterId));
}

export function fetchAuditLogs(clusterId, limit = 20) {
    return request(API_BASE + "/audit/logs?clusterId=" + encodeURIComponent(clusterId) + "&limit=" + encodeURIComponent(limit));
}

export function fetchBuiltInUsers(clusterId) {
    return request(API_BASE + "/auth/built-in/users?clusterId=" + encodeURIComponent(clusterId));
}

export function createBuiltInUser(clusterId, payload) {
    return request(API_BASE + "/auth/built-in/users?clusterId=" + encodeURIComponent(clusterId), {
        method: "POST",
        body: JSON.stringify(payload)
    });
}

export function importBuiltInUsers(clusterId, lines) {
    return request(API_BASE + "/auth/built-in/users/import?clusterId=" + encodeURIComponent(clusterId), {
        method: "POST",
        body: JSON.stringify({ lines })
    });
}

export function deleteBuiltInUser(clusterId, userId) {
    return request(API_BASE + "/auth/built-in/users/" + encodeURIComponent(userId) + "?clusterId=" + encodeURIComponent(clusterId), {
        method: "DELETE"
    });
}

export function deleteAllBuiltInUsers(clusterId) {
    return request(API_BASE + "/auth/built-in/users?clusterId=" + encodeURIComponent(clusterId), {
        method: "DELETE"
    });
}
