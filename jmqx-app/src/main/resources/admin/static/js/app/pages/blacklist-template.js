export const blacklistPageTemplate = `
  <section class="panel" v-if="activeMenu==='blacklist'">
    <div class="section-head">
      <div>
        <h2 class="title">黑名单</h2>
        <div class="section-subtitle">{{ blacklistSummaryText }}</div>
      </div>
    </div>
    <div class="blacklist-create">
      <el-select v-model="blacklistForm.type" class="blacklist-type-select">
        <el-option label="clientId" value="clientId"/>
        <el-option label="IP" value="ip"/>
      </el-select>
      <el-input
        v-model="blacklistForm.value"
        :placeholder="blacklistForm.type==='ip' ? '请输入客户端 IP' : '请输入客户端 ID'"
        @keyup.enter="submitBlacklistEntry"
      />
      <el-button type="danger" @click="submitBlacklistEntry">加入黑名单</el-button>
    </div>
    <div class="chips toolbar-chips">
      <span class="chip">按精确值匹配</span>
      <span class="chip">命中后会立即断开在线连接</span>
      <span class="chip">变更会同步到集群其他节点</span>
    </div>
    <el-table :data="blacklistEntries" empty-text="暂无黑名单规则">
      <el-table-column prop="type" label="类型" min-width="120">
        <template #default="{ row }">{{ blacklistTypeLabel(row.type) }}</template>
      </el-table-column>
      <el-table-column prop="value" label="值" min-width="260"/>
      <el-table-column prop="source" label="来源" min-width="180"/>
      <el-table-column prop="createdAt" label="创建时间" min-width="180">
        <template #default="{ row }">{{ formatDateTime(row.createdAt) }}</template>
      </el-table-column>
      <el-table-column label="操作" width="140" fixed="right">
        <template #default="{ row }">
          <el-button type="danger" link @click="removeBlacklistEntry(row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>
  </section>
`;
