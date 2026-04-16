export const overviewPageTemplate = `
  <section class="panel overview-shell" v-if="activeMenu==='overview'">
    <div class="section-head overview-head">
      <div>
        <h2 class="title">{{ tr('overview.title') }}</h2>
        <div class="section-subtitle">{{ tr('overview.subtitle') }}</div>
      </div>
      <button class="btn" @click="refreshOverview">{{ tr('overview.refresh') }}</button>
    </div>
    <div class="overview-kpis">
      <div class="overview-kpi">
        <div class="overview-kpi-label">{{ tr('overview.totalConnections') }}</div>
        <div class="overview-kpi-value">{{ formatNumber(overview.totalConnections) }}</div>
      </div>
      <div class="overview-kpi">
        <div class="overview-kpi-label">{{ tr('overview.totalInboundBytes') }}</div>
        <div class="overview-kpi-value">{{ formatNumber(overview.totalInboundBytes) }}</div>
      </div>
      <div class="overview-kpi">
        <div class="overview-kpi-label">{{ tr('overview.totalOutboundBytes') }}</div>
        <div class="overview-kpi-value">{{ formatNumber(overview.totalOutboundBytes) }}</div>
      </div>
      <div class="overview-kpi">
        <div class="overview-kpi-label">{{ tr('overview.authDenyError') }}</div>
        <div class="overview-kpi-value">{{ formatNumber(overview.totalConnectAuthFailure) }} / {{ formatNumber(overview.totalConnectAuthError) }}</div>
      </div>
      <div class="overview-kpi">
        <div class="overview-kpi-label">{{ tr('overview.aclDenyError') }}</div>
        <div class="overview-kpi-value">{{ formatNumber(overview.totalPublishAclDeny) }} / {{ formatNumber(overview.totalPublishAclError) }}</div>
      </div>
      <div class="overview-kpi">
        <div class="overview-kpi-label">{{ tr('overview.maxAuthLatency') }}</div>
        <div class="overview-kpi-value">{{ formatLatencyMs(overview.maxConnectAuthMs) }}</div>
      </div>
      <div class="overview-kpi">
        <div class="overview-kpi-label">{{ tr('overview.maxAclLatency') }}</div>
        <div class="overview-kpi-value">{{ formatLatencyMs(overview.maxPublishAclMs) }}</div>
      </div>
    </div>

    <div class="overview-workbench">
      <section class="overview-table-panel">
        <div class="overview-panel-title">{{ tr('overview.section.matrix') }}</div>
        <table class="data-table overview-table">
          <thead>
          <tr>
            <th>{{ tr('overview.table.nodeId') }}</th>
            <th>{{ tr('overview.table.role') }}</th>
            <th>{{ tr('overview.table.nodeIp') }}</th>
            <th>{{ tr('overview.table.connections') }}</th>
            <th>{{ tr('overview.table.inbound') }}</th>
            <th>{{ tr('overview.table.outbound') }}</th>
            <th>{{ tr('overview.table.auth') }}</th>
            <th>{{ tr('overview.table.acl') }}</th>
            <th>{{ tr('overview.table.maxLatency') }}</th>
            <th>{{ tr('overview.table.lastReportTime') }}</th>
          </tr>
          </thead>
          <tbody>
          <tr v-for="node in overview.nodes" :key="node.nodeId">
            <td>
              <div class="overview-node-cell">
                <strong>{{ node.nodeId }}</strong>
                <span class="status-badge" :class="nodeHealthClass(node)">{{ nodeHealthLabel(node) }}</span>
              </div>
            </td>
            <td>{{ node.role || '-' }}</td>
            <td>{{ node.nodeIp }}</td>
            <td>{{ formatNumber(node.connectedClients) }}</td>
            <td>{{ formatNumber(node.inboundBytes) }}</td>
            <td>{{ formatNumber(node.outboundBytes) }}</td>
            <td>{{ formatNumber(node.connectAuthFailure) }} / {{ formatNumber(node.connectAuthError) }}</td>
            <td>{{ formatNumber(node.publishAclDeny) }} / {{ formatNumber(node.publishAclError) }}</td>
            <td>{{ formatLatencyMs(node.connectAuthMaxMs) }} / {{ formatLatencyMs(node.publishAclMaxMs) }}</td>
            <td>{{ formatDateTime(node.lastReportTime) }}</td>
          </tr>
          </tbody>
        </table>
      </section>
    </div>
  </section>
`;
