export const clientsPageTemplate = `
  <section class="panel" v-if="activeMenu==='clients' && !selectedClient">
    <div class="section-head">
      <div>
        <h2 class="title">{{ tr('clients.title') }}</h2>
        <div class="section-subtitle">{{ clientsSummaryText }}</div>
      </div>
    </div>
    <div class="toolbar">
      <input v-model="search.clientId" :placeholder="tr('clients.search.clientId')"/>
      <input v-model="search.userName" :placeholder="tr('clients.search.username')"/>
      <button class="btn" @click="queryClients">{{ tr('clients.search.submit') }}</button>
      <button class="btn secondary" @click="search={clientId:'',userName:'',pageNo:1,pageSize:20};queryClients()">{{ tr('clients.search.reset') }}</button>
    </div>
    <div class="chips toolbar-chips">
      <span class="chip">{{ tr('clients.chip.pageSize', { size: clients.pageSize }) }}</span>
      <span class="chip">{{ tr('clients.chip.realtime') }}</span>
      <span class="chip">{{ tr('clients.chip.detailHint') }}</span>
    </div>
    <table class="data-table">
      <thead>
      <tr>
        <th>{{ tr('clients.table.clientId') }}</th>
        <th>{{ tr('clients.table.username') }}</th>
        <th>{{ tr('clients.table.node') }}</th>
        <th>{{ tr('clients.table.ip') }}</th>
        <th>{{ tr('clients.table.keepalive') }}</th>
        <th>{{ tr('clients.table.connectionType') }}</th>
        <th>{{ tr('clients.table.connectedAt') }}</th>
        <th>{{ tr('clients.table.actions') }}</th>
      </tr>
      </thead>
      <tbody>
      <tr class="click-row" v-for="c in clients.records" :key="c.clientId" @click="viewClient(c.clientId)">
        <td>{{ c.clientId }}</td>
        <td>{{ c.username || '-' }}</td>
        <td>{{ c.nodeId }}</td>
        <td>{{ c.clientIp }}</td>
        <td>{{ c.keepAliveSeconds }}</td>
        <td>{{ c.connectionType }}</td>
        <td>{{ formatDateTime(c.connectedAt) }}</td>
        <td>
          <div class="table-actions">
            <el-button type="warning" link @click.stop="kickClient(c.clientId)">{{ tr('clients.action.kick') }}</el-button>
            <el-button type="danger" link @click.stop="blockClientByClientId(c.clientId)">{{ tr('clients.action.blockClientId') }}</el-button>
            <el-button v-if="c.clientIp && c.clientIp !== 'unknown'" type="danger" link @click.stop="blockClientByIp(c.clientIp, c.clientId)">{{ tr('clients.action.blockIp') }}</el-button>
          </div>
        </td>
      </tr>
      </tbody>
    </table>
    <div class="hint">{{ tr('clients.footer.total', { total: clients.total }) }}</div>
  </section>

  <section class="panel" v-if="activeMenu==='clients' && selectedClient">
    <div class="section-head">
      <div>
        <h2 class="title">{{ tr('clients.detail.title') }}</h2>
        <div class="section-subtitle">{{ tr('clients.detail.subtitle') }}</div>
      </div>
    </div>
    <div class="toolbar">
      <button class="btn secondary" @click="selectedClient=null">{{ tr('clients.detail.back') }}</button>
      <button class="btn warning" @click="kickClient(selectedClient.session.clientId)">{{ tr('clients.action.kick') }}</button>
      <button class="btn danger" @click="blockClientByClientId(selectedClient.session.clientId)">{{ tr('clients.action.blockClientId') }}</button>
      <button class="btn danger" v-if="selectedClient.session.clientIp && selectedClient.session.clientIp !== 'unknown'" @click="blockClientByIp(selectedClient.session.clientIp, selectedClient.session.clientId)">{{ tr('clients.action.blockIp') }}</button>
    </div>
    <table class="data-table detail-table">
      <tbody>
      <tr><th>{{ tr('clients.table.clientId') }}</th><td>{{ selectedClient.session.clientId }}</td></tr>
      <tr><th>{{ tr('clients.table.username') }}</th><td>{{ selectedClient.session.username || '-' }}</td></tr>
      <tr><th>{{ tr('clients.table.node') }}</th><td>{{ selectedClient.session.nodeId }}</td></tr>
      <tr><th>{{ tr('clients.table.ip') }}</th><td>{{ selectedClient.session.clientIp }}</td></tr>
      <tr><th>{{ tr('clients.table.keepalive') }}</th><td>{{ selectedClient.session.keepAliveSeconds }}</td></tr>
      <tr><th>{{ tr('clients.table.connectionType') }}</th><td>{{ selectedClient.session.connectionType }}</td></tr>
      <tr><th>{{ tr('clients.table.connectedAt') }}</th><td>{{ formatDateTime(selectedClient.session.connectedAt) }}</td></tr>
      </tbody>
    </table>
    <div class="hint">{{ tr('clients.detail.subscribedTopics') }}</div>
    <div class="chips">
      <span class="chip" v-for="topic in selectedClient.subscribedTopics" :key="topic">{{ topic }}</span>
      <span class="hint" v-if="selectedClient.subscribedTopics.length===0">{{ tr('clients.detail.noTopics') }}</span>
    </div>
  </section>
`;
