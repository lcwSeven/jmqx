export async function request(url, options = {}) {
    const response = await fetch(url, {
        headers: { "Content-Type": "application/json" },
        ...options
    });
    if (!response.ok) {
        throw new Error((await response.text()) || "request failed");
    }
    const text = await response.text();
    return text ? JSON.parse(text) : null;
}

export function fetchClusters() {
    return request("/api/v1/clusters");
}

export function createCluster(payload) {
    return request("/api/v1/clusters", {
        method: "POST",
        body: JSON.stringify(payload)
    });
}

export function fetchOverview(clusterId) {
    return request("/api/v1/cluster/overview?clusterId=" + encodeURIComponent(clusterId));
}

export function fetchClients(clusterId, search) {
    const query = new URLSearchParams({
        clusterId: clusterId || "default",
        clientId: search.clientId || "",
        userName: search.userName || "",
        pageNo: String(search.pageNo || 1),
        pageSize: String(search.pageSize || 20)
    });
    return request("/api/v1/clients?" + query.toString());
}

export function fetchClientDetail(clusterId, clientId) {
    return request("/api/v1/clients/" + encodeURIComponent(clientId) + "?clusterId=" + encodeURIComponent(clusterId));
}

export function fetchSecurityConfig(clusterId) {
    return request("/api/v1/security/config?clusterId=" + encodeURIComponent(clusterId));
}

export function saveSecurityConfig(clusterId, payload) {
    return request("/api/v1/security/config?clusterId=" + encodeURIComponent(clusterId), {
        method: "PUT",
        body: JSON.stringify(payload)
    });
}

export function fetchClusterConfig(clusterId) {
    return request("/api/v1/cluster/config?clusterId=" + encodeURIComponent(clusterId));
}

export function saveClusterConfig(clusterId, payload) {
    return request("/api/v1/cluster/config?clusterId=" + encodeURIComponent(clusterId), {
        method: "PUT",
        body: JSON.stringify(payload)
    });
}

export function fetchClusterFullConfig(clusterId) {
    return request("/api/v1/cluster/full-config?clusterId=" + encodeURIComponent(clusterId));
}

export function fetchAuditLogs(clusterId, limit = 20) {
    return request("/api/v1/audit/logs?clusterId=" + encodeURIComponent(clusterId) + "&limit=" + encodeURIComponent(limit));
}
