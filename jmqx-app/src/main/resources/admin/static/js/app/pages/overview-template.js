export const overviewPageTemplate = `
  <section class="panel" v-if="activeMenu==='overview'">
    <div class="section-head">
      <div>
        <h2 class="title">集群概览</h2>
        <div class="section-subtitle">总览卡片聚合全节点实时数据，下面保留逐节点明细。</div>
      </div>
      <button class="btn" @click="refreshOverview">刷新</button>
    </div>
    <div class="stats">
      <div class="stat">
        <div class="label">总连接数</div>
        <div class="value">{{ formatNumber(overview.totalConnections) }}</div>
      </div>
      <div class="stat">
        <div class="label">总入流量 (Bytes)</div>
        <div class="value">{{ formatNumber(overview.totalInboundBytes) }}</div>
      </div>
      <div class="stat">
        <div class="label">总出流量 (Bytes)</div>
        <div class="value">{{ formatNumber(overview.totalOutboundBytes) }}</div>
      </div>
    </div>
    <div class="node-grid" v-if="overview.nodes && overview.nodes.length">
      <article class="node-card" v-for="node in overview.nodes" :key="node.nodeId">
        <div class="node-card-head">
          <div>
            <div class="node-name">{{ node.nodeId }}</div>
            <div class="node-meta">{{ node.role || '-' }} · {{ node.nodeIp }}</div>
          </div>
          <span class="status-badge" :class="nodeHealthClass(node)">{{ nodeHealthLabel(node) }}</span>
        </div>
        <div class="node-card-stats">
          <div>
            <span>连接数</span>
            <strong>{{ formatNumber(node.connectedClients) }}</strong>
          </div>
          <div>
            <span>入流量</span>
            <strong>{{ formatNumber(node.inboundBytes) }}</strong>
          </div>
          <div>
            <span>出流量</span>
            <strong>{{ formatNumber(node.outboundBytes) }}</strong>
          </div>
        </div>
        <div class="node-card-foot">最后上报 {{ formatDateTime(node.lastReportTime) }}</div>
      </article>
    </div>
    <table class="data-table">
      <thead>
      <tr>
        <th>节点 ID</th>
        <th>节点角色</th>
        <th>节点 IP</th>
        <th>连接数</th>
        <th>入流量</th>
        <th>出流量</th>
        <th>最后上报时间</th>
      </tr>
      </thead>
      <tbody>
      <tr v-for="node in overview.nodes" :key="node.nodeId">
        <td>{{ node.nodeId }}</td>
        <td>{{ node.role || '-' }}</td>
        <td>{{ node.nodeIp }}</td>
        <td>{{ formatNumber(node.connectedClients) }}</td>
        <td>{{ formatNumber(node.inboundBytes) }}</td>
        <td>{{ formatNumber(node.outboundBytes) }}</td>
        <td>{{ formatDateTime(node.lastReportTime) }}</td>
      </tr>
      </tbody>
    </table>
  </section>
`;
