const API_BASE = "api/v1";

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
        const response = await fetch(candidate, {
            headers: { "Content-Type": "application/json" },
            ...options
        });
        if (response.ok) {
            const text = await response.text();
            return text ? JSON.parse(text) : null;
        }
        const errorText = normalizeErrorText(await response.text());
        lastError = new Error(candidate + " -> HTTP " + response.status + ": " + errorText);
        if (response.status !== 404 || index === candidates.length - 1) {
            throw lastError;
        }
    }
    throw lastError || new Error("request failed");
}

export function fetchClusters() {
    return request(API_BASE + "/clusters");
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
