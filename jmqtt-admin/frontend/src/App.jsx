import { useEffect, useState } from "react";

const apiBase = import.meta.env.VITE_ADMIN_API_BASE || "http://127.0.0.1:18083/api/admin";

export default function App() {
  const [status, setStatus] = useState({
    connections: 0,
    authType: "-",
    authCacheMillis: 0,
    aclType: "-",
    aclCacheMillis: 0
  });
  const [form, setForm] = useState({
    authType: "",
    authCacheMillis: "",
    aclType: "",
    aclCacheMillis: ""
  });
  const [error, setError] = useState("");

  useEffect(() => {
    void refresh();
    const timer = setInterval(() => void refresh(), 3000);
    return () => clearInterval(timer);
  }, []);

  async function refresh() {
    try {
      const resp = await fetch(`${apiBase}/status`);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      setStatus(await resp.json());
      setError("");
    } catch (e) {
      setError(`加载状态失败: ${String(e)}`);
    }
  }

  async function submit() {
    const payload = {};
    if (form.authType) payload.authType = form.authType;
    if (form.authCacheMillis !== "") payload.authCacheMillis = Number(form.authCacheMillis);
    if (form.aclType) payload.aclType = form.aclType;
    if (form.aclCacheMillis !== "") payload.aclCacheMillis = Number(form.aclCacheMillis);

    try {
      const resp = await fetch(`${apiBase}/config`, {
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

  return (
    <div className="page">
      <h1>JMQTT 管理后台</h1>
      <div className="card">
        <div>连接数: <strong>{status.connections}</strong></div>
        <div>Auth: <strong>{status.authType}</strong> (cache={status.authCacheMillis}ms)</div>
        <div>ACL: <strong>{status.aclType}</strong> (cache={status.aclCacheMillis}ms)</div>
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

        <button onClick={() => void submit()}>保存配置</button>
      </div>

      {error ? <div className="error">{error}</div> : null}
    </div>
  );
}
