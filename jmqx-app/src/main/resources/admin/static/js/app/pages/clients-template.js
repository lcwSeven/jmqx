export const clientsPageTemplate = `
  <section class="panel" v-if="activeMenu==='clients' && !selectedClient">
    <div class="section-head">
      <div>
        <h2 class="title">客户端列表</h2>
        <div class="section-subtitle">{{ clientsSummaryText }}</div>
      </div>
    </div>
    <div class="toolbar">
      <input v-model="search.clientId" placeholder="客户端 ID"/>
      <input v-model="search.userName" placeholder="用户名"/>
      <button class="btn" @click="queryClients">查询</button>
      <button class="btn secondary" @click="search={clientId:'',userName:'',pageNo:1,pageSize:20};queryClients()">重置</button>
    </div>
    <div class="chips toolbar-chips">
      <span class="chip">页面容量 {{ clients.pageSize }}</span>
      <span class="chip">实时刷新已启用</span>
      <span class="chip">点击行查看详情</span>
    </div>
    <table class="data-table">
      <thead>
      <tr>
        <th>客户端 ID</th>
        <th>节点</th>
        <th>IP</th>
        <th>Keepalive</th>
        <th>连接方式</th>
        <th>用户名</th>
        <th>上线时间</th>
      </tr>
      </thead>
      <tbody>
      <tr class="click-row" v-for="c in clients.records" :key="c.clientId" @click="viewClient(c.clientId)">
        <td>{{ c.clientId }}</td>
        <td>{{ c.nodeId }}</td>
        <td>{{ c.clientIp }}</td>
        <td>{{ c.keepAliveSeconds }}</td>
        <td>{{ c.connectionType }}</td>
        <td>{{ c.username || '-' }}</td>
        <td>{{ formatDateTime(c.connectedAt) }}</td>
      </tr>
      </tbody>
    </table>
    <div class="hint">总记录: {{ clients.total }}，列表会在连接事件后自动刷新。</div>
  </section>

  <section class="panel" v-if="activeMenu==='clients' && selectedClient">
    <div class="section-head">
      <div>
        <h2 class="title">客户端详情</h2>
        <div class="section-subtitle">查看连接属性、会话信息和当前订阅主题。</div>
      </div>
    </div>
    <div class="toolbar">
      <button class="btn secondary" @click="selectedClient=null">返回列表</button>
    </div>
    <table class="data-table detail-table">
      <tbody>
      <tr><th>客户端 ID</th><td>{{ selectedClient.session.clientId }}</td></tr>
      <tr><th>节点</th><td>{{ selectedClient.session.nodeId }}</td></tr>
      <tr><th>IP</th><td>{{ selectedClient.session.clientIp }}</td></tr>
      <tr><th>Keepalive</th><td>{{ selectedClient.session.keepAliveSeconds }}</td></tr>
      <tr><th>连接方式</th><td>{{ selectedClient.session.connectionType }}</td></tr>
      <tr><th>用户名</th><td>{{ selectedClient.session.username || '-' }}</td></tr>
      <tr><th>上线时间</th><td>{{ formatDateTime(selectedClient.session.connectedAt) }}</td></tr>
      </tbody>
    </table>
    <div class="hint">订阅主题</div>
    <div class="chips">
      <span class="chip" v-for="topic in selectedClient.subscribedTopics" :key="topic">{{ topic }}</span>
      <span class="hint" v-if="selectedClient.subscribedTopics.length===0">暂无订阅主题</span>
    </div>
  </section>
`;
