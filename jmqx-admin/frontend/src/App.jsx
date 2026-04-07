import { useEffect, useState } from "react";

const apiBase = import.meta.env.VITE_ADMIN_API_BASE || "http://127.0.0.1:18083/api/admin";

export default function App() {
  const [menu, setMenu] = useState("cluster");
  const [nodes, setNodes] = useState([]);
  const [cluster, setCluster] = useState({
    totalNodes: 0,
    onlineNodes: 0,
    totalConnections: 0,
    nodes: []
  });
  const [activeNodeId, setActiveNodeId] = useState("");
  const [status, setStatus] = useState({
    connections: 0,
    authType: "-",
    authCacheMillis: 0,
    aclType: "-",
    aclCacheMillis: 0
  });
  const [clients, setClients] = useState([]);
  const [form, setForm] = useState({
    authType: "",
    authCacheMillis: "",
    aclType: "",
    aclCacheMillis: ""
  });
  const [clientQueryInput, setClientQueryInput] = useState("");
  const [usernameQueryInput, setUsernameQueryInput] = useState("");
  const [clientQuery, setClientQuery] = useState("");
  const [usernameQuery, setUsernameQuery] = useState("");
  const [detail, setDetail] = useState(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [nodeForm, setNodeForm] = useState({ name: "", baseUrl: "" });
  const [authAdvancedJson, setAuthAdvancedJson] = useState("");
  const [aclAdvancedJson, setAclAdvancedJson] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    void refreshNodes();
    void refreshCluster();
    void refreshStatus();
    void refreshClients();
    const timer = setInterval(() => {
      void refreshNodes();
      void refreshCluster();
      void refreshStatus();
      void refreshClients();
    }, 3000);
    return () => clearInterval(timer);
  }, [clientQuery, usernameQuery, activeNodeId]);

  async function refreshNodes() {
    try {
      const resp = await fetch(`${apiBase}/nodes`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      const data = await resp.json();
      setNodes(data);
      if (!activeNodeId && data.length > 0) {
        setActiveNodeId(data[0].nodeId || "");
      }
      setError("");
    } catch (e) {
      setError(`加载节点失败: ${String(e)}`);
    }
  }

  async function refreshCluster() {
    try {
      const resp = await fetch(`${apiBase}/cluster/status`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      setCluster(await resp.json());
      setError("");
    } catch (e) {
      setError(`加载集群状态失败: ${String(e)}`);
    }
  }

  async function refreshStatus() {
    try {
      const query = new URLSearchParams();
      if (activeNodeId) query.set("nodeId", activeNodeId);
      const suffix = query.toString() ? `?${query}` : "";
      const resp = await fetch(`${apiBase}/status${suffix}`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      setStatus(await resp.json());
      setError("");
    } catch (e) {
      setError(`加载状态失败: ${String(e)}`);
    }
  }

  async function refreshClients() {
    try {
      const query = new URLSearchParams();
      if (activeNodeId) query.set("nodeId", activeNodeId);
      if (clientQuery.trim()) query.set("clientId", clientQuery.trim());
      if (usernameQuery.trim()) query.set("username", usernameQuery.trim());
      const suffix = query.toString() ? `?${query}` : "";
      const resp = await fetch(`${apiBase}/clients${suffix}`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      setClients(await resp.json());
      setError("");
    } catch (e) {
      setError(`加载客户端失败: ${String(e)}`);
    }
  }

  async function openClientDetail(nodeId, clientId) {
    try {
      setDetail({ nodeId, clientId, subscriptions: [] });
      setDetailLoading(true);
      const query = new URLSearchParams();
      if (nodeId) query.set("nodeId", nodeId);
      const suffix = query.toString() ? `?${query}` : "";
      const resp = await fetch(`${apiBase}/clients/${encodeURIComponent(clientId)}${suffix}`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      setDetail(await resp.json());
      setError("");
    } catch (e) {
      setError(`加载客户端详情失败: ${String(e)}`);
    } finally {
      setDetailLoading(false);
    }
  }

  function applyClientQuery() {
    setClientQuery(clientQueryInput.trim());
    setUsernameQuery(usernameQueryInput.trim());
  }

  async function submit(payload) {
    try {
      const query = new URLSearchParams();
      if (activeNodeId) query.set("nodeId", activeNodeId);
      const suffix = query.toString() ? `?${query}` : "";
      const resp = await fetch(`${apiBase}/config${suffix}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      setStatus(await resp.json());
      setError("");
    } catch (e) {
      setError(`保存失败: ${String(e)}`);
    }
  }

  async function addNode() {
    try {
      if (!nodeForm.baseUrl.trim()) {
        setError("节点地址不能为空");
        return;
      }
      const resp = await fetch(`${apiBase}/nodes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: nodeForm.name.trim(),
          baseUrl: nodeForm.baseUrl.trim()
        })
      });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      setNodeForm({ name: "", baseUrl: "" });
      await refreshNodes();
      await refreshCluster();
      setError("");
    } catch (e) {
      setError(`新增节点失败: ${String(e)}`);
    }
  }

  async function removeNode(nodeId) {
    try {
      const resp = await fetch(`${apiBase}/nodes/${encodeURIComponent(nodeId)}`, {
        method: "DELETE"
      });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      if (activeNodeId === nodeId) {
        setActiveNodeId("");
      }
      await refreshNodes();
      await refreshCluster();
      setError("");
    } catch (e) {
      setError(`删除节点失败: ${String(e)}`);
    }
  }

  async function submitAuthConfig() {
    const payload = {};
    if (form.authType) payload.authType = form.authType;
    if (form.authCacheMillis !== "") payload.authCacheMillis = Number(form.authCacheMillis);
    await submit(payload);
  }

  async function submitAclConfig() {
    const payload = {};
    if (form.aclType) payload.aclType = form.aclType;
    if (form.aclCacheMillis !== "") payload.aclCacheMillis = Number(form.aclCacheMillis);
    await submit(payload);
  }

  async function submitAuthAdvanced() {
    try {
      const payload = authAdvancedJson.trim() ? JSON.parse(authAdvancedJson) : {};
      await submit(payload);
    } catch (e) {
      setError(`Auth 高级配置 JSON 解析失败: ${String(e)}`);
    }
  }

  async function submitAclAdvanced() {
    try {
      const payload = aclAdvancedJson.trim() ? JSON.parse(aclAdvancedJson) : {};
      await submit(payload);
    } catch (e) {
      setError(`ACL 高级配置 JSON 解析失败: ${String(e)}`);
    }
  }

  function formatTime(epochMillis) {
    if (!epochMillis || Number.isNaN(epochMillis)) {
      return "-";
    }
    return new Date(epochMillis).toLocaleString();
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">JMQX Admin</div>
        <button
          className={`menu-item ${menu === "cluster" ? "active" : ""}`}
          onClick={() => setMenu("cluster")}
        >
          集群节点
        </button>
        <button
          className={`menu-item ${menu === "clients" ? "active" : ""}`}
          onClick={() => setMenu("clients")}
        >
          客户端列表
        </button>
        <div className="menu-group-title">安全设置</div>
        <button
          className={`submenu-item ${menu === "acl-auth" ? "active" : ""}`}
          onClick={() => setMenu("acl-auth")}
        >
          ACL 鉴权
        </button>
        <button
          className={`submenu-item ${menu === "conn-auth" ? "active" : ""}`}
          onClick={() => setMenu("conn-auth")}
        >
          连接鉴权
        </button>
      </aside>

      <main className="content">
        {menu === "cluster" ? (
          <>
            <h1>集群节点</h1>
            <div className="card">
              <div>节点总数: <strong>{cluster.totalNodes}</strong></div>
              <div>在线节点: <strong>{cluster.onlineNodes}</strong></div>
              <div>总连接数: <strong>{cluster.totalConnections}</strong></div>
            </div>

            <div className="card filters-row">
              <div className="filter-item">
                <label>节点名称</label>
                <input
                  type="text"
                  value={nodeForm.name}
                  onChange={(e) => setNodeForm({ ...nodeForm, name: e.target.value })}
                  placeholder="例如 node-shanghai-1"
                />
              </div>
              <div className="filter-item">
                <label>节点 API 地址</label>
                <input
                  type="text"
                  value={nodeForm.baseUrl}
                  onChange={(e) => setNodeForm({ ...nodeForm, baseUrl: e.target.value })}
                  placeholder="例如 http://127.0.0.1:28083/api/admin"
                />
              </div>
              <div className="filter-actions">
                <button className="primary-btn" onClick={() => void addNode()}>新增节点</button>
              </div>
            </div>

            <div className="card table-card">
              <table className="client-table">
                <thead>
                  <tr>
                    <th>节点 ID</th>
                    <th>节点名称</th>
                    <th>API 地址</th>
                    <th>在线</th>
                    <th>连接数</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {cluster.nodes?.length ? (
                    cluster.nodes.map((item) => (
                      <tr key={item.nodeId || item.baseUrl}>
                        <td>{item.nodeId || "-"}</td>
                        <td>{item.nodeName || "-"}</td>
                        <td>{item.baseUrl || "-"}</td>
                        <td>{item.online ? "在线" : "离线"}</td>
                        <td>{item.connections || 0}</td>
                        <td>
                          <button className="secondary-btn" onClick={() => setActiveNodeId(item.nodeId || "")}>设为当前</button>
                          <button className="secondary-btn" onClick={() => void removeNode(item.nodeId)}>删除</button>
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={6} className="empty">暂无节点</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </>
        ) : menu === "conn-auth" ? (
          <>
            <h1>连接鉴权</h1>
            <div className="card">
              <div>当前节点: <strong>{status.nodeName || status.nodeId || "-"}</strong></div>
              <div>连接数: <strong>{status.connections}</strong></div>
              <div>当前鉴权: <strong>{status.authType}</strong> (cache={status.authCacheMillis}ms)</div>
            </div>

            <div className="card">
              <label>Auth 鉴权类型</label>
              <select value={form.authType} onChange={(e) => setForm({ ...form, authType: e.target.value })}>
                <option value="">不修改</option>
                <option value="allow_all">allow_all</option>
                <option value="http">http</option>
                <option value="file">file</option>
                <option value="redis">redis</option>
                <option value="db">db</option>
              </select>

              <label>Auth 缓存时间(ms)</label>
              <input
                type="number"
                min="0"
                value={form.authCacheMillis}
                onChange={(e) => setForm({ ...form, authCacheMillis: e.target.value })}
                placeholder="例如 60000"
              />
              <button className="primary-btn" onClick={() => void submitAuthConfig()}>保存连接鉴权配置</button>

              <label>Auth 高级配置(JSON)</label>
              <textarea
                value={authAdvancedJson}
                onChange={(e) => setAuthAdvancedJson(e.target.value)}
                placeholder='例如 {"authChain":"file,redis,http","authFilePath":"auth-users.txt"}'
                rows={4}
              />
              <button className="secondary-btn" onClick={() => void submitAuthAdvanced()}>提交 Auth 高级配置</button>
            </div>
          </>
        ) : menu === "acl-auth" ? (
          <>
            <h1>ACL 鉴权</h1>
            <div className="card">
              <div>当前节点: <strong>{status.nodeName || status.nodeId || "-"}</strong></div>
              <div>连接数: <strong>{status.connections}</strong></div>
              <div>当前 ACL: <strong>{status.aclType}</strong> (cache={status.aclCacheMillis}ms)</div>
            </div>

            <div className="card">
              <label>ACL 鉴权类型</label>
              <select value={form.aclType} onChange={(e) => setForm({ ...form, aclType: e.target.value })}>
                <option value="">不修改</option>
                <option value="allow_all">allow_all</option>
                <option value="http">http</option>
                <option value="file">file</option>
                <option value="redis">redis</option>
              </select>

              <label>ACL 缓存时间(ms)</label>
              <input
                type="number"
                min="0"
                value={form.aclCacheMillis}
                onChange={(e) => setForm({ ...form, aclCacheMillis: e.target.value })}
                placeholder="例如 60000"
              />
              <button className="primary-btn" onClick={() => void submitAclConfig()}>保存 ACL 配置</button>

              <label>ACL 高级配置(JSON)</label>
              <textarea
                value={aclAdvancedJson}
                onChange={(e) => setAclAdvancedJson(e.target.value)}
                placeholder='例如 {"aclType":"http","aclHttpUrl":"http://127.0.0.1:8080/acl/check"}'
                rows={4}
              />
              <button className="secondary-btn" onClick={() => void submitAclAdvanced()}>提交 ACL 高级配置</button>
            </div>
          </>
        ) : (
          <>
            {detail ? (
              <>
                <h1>客户端详情</h1>
                <div className="card">
                  <button className="secondary-btn" onClick={() => setDetail(null)}>返回列表</button>
                  {detailLoading ? (
                    <div className="loading">加载中...</div>
                  ) : (
                    <>
                      <div className="kv-line"><span>所属节点:</span><strong>{detail.nodeName || detail.nodeId || "-"}</strong></div>
                      <div className="kv-line"><span>客户端 ID:</span><strong>{detail.clientId || "-"}</strong></div>
                      <div className="kv-line"><span>上线时间:</span><strong>{formatTime(detail.onlineAtEpochMillis)}</strong></div>
                      <div className="kv-line"><span>用户名:</span><strong>{detail.username || "-"}</strong></div>
                      <div className="kv-line"><span>连接方式:</span><strong>{detail.connectionType || "-"}</strong></div>
                      <div className="kv-line"><span>服务节点 IP:</span><strong>{detail.serviceNodeIp || "-"}</strong></div>
                      <div className="kv-line"><span>Keepalive(秒):</span><strong>{detail.keepAliveSeconds}</strong></div>
                    </>
                  )}
                </div>
                <div className="card table-card">
                  <h3>订阅 Topic</h3>
                  <table className="client-table">
                    <thead>
                      <tr>
                        <th>Topic</th>
                        <th>QoS</th>
                      </tr>
                    </thead>
                    <tbody>
                      {!detail.subscriptions || detail.subscriptions.length === 0 ? (
                        <tr>
                          <td colSpan={2} className="empty">暂无订阅</td>
                        </tr>
                      ) : (
                        detail.subscriptions.map((item) => (
                          <tr key={`${item.topic}-${item.qos}`}>
                            <td>{item.topic}</td>
                            <td>{item.qos}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              <>
                <h1>客户端列表</h1>
                <div className="card filters-row">
                  <div className="filter-item">
                    <label>当前节点</label>
                    <select value={activeNodeId} onChange={(e) => setActiveNodeId(e.target.value)}>
                      <option value="">全部节点</option>
                      {nodes.map((n) => (
                        <option key={n.nodeId} value={n.nodeId}>
                          {n.name || n.nodeId}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
                <div className="card filters-row">
                  <div className="filter-item">
                    <label>客户端 ID</label>
                    <input
                      type="text"
                      value={clientQueryInput}
                      onChange={(e) => setClientQueryInput(e.target.value)}
                      placeholder="输入客户端 ID 关键字"
                    />
                  </div>
                  <div className="filter-item">
                    <label>用户名</label>
                    <input
                      type="text"
                      value={usernameQueryInput}
                      onChange={(e) => setUsernameQueryInput(e.target.value)}
                      placeholder="输入用户名关键字"
                    />
                  </div>
                  <div className="filter-actions">
                    <button className="primary-btn" onClick={() => applyClientQuery()}>查询</button>
                  </div>
                </div>
                <div className="card table-card">
                  <table className="client-table">
                    <thead>
                      <tr>
                        <th>节点</th>
                        <th>客户端 ID</th>
                        <th>上线时间</th>
                        <th>用户名</th>
                        <th>连接方式</th>
                        <th>服务节点 IP</th>
                        <th>Keepalive(秒)</th>
                      </tr>
                    </thead>
                    <tbody>
                      {clients.length === 0 ? (
                        <tr>
                          <td colSpan={7} className="empty">暂无匹配客户端</td>
                        </tr>
                      ) : (
                        clients.map((item) => (
                          <tr
                            key={`${item.nodeId || "na"}-${item.clientId}`}
                            className="clickable-row"
                            onClick={() => void openClientDetail(item.nodeId, item.clientId)}
                          >
                            <td>{item.nodeName || item.nodeId || "-"}</td>
                            <td>{item.clientId || "-"}</td>
                            <td>{formatTime(item.onlineAtEpochMillis)}</td>
                            <td>{item.username || "-"}</td>
                            <td>{item.connectionType || "-"}</td>
                            <td>{item.serviceNodeIp || "-"}</td>
                            <td>{item.keepAliveSeconds}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </>
        )}

        {error ? <div className="error">{error}</div> : null}
      </main>
    </div>
  );
}
