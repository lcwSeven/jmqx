export const blacklistPageTemplate = `
  <section class="panel" v-if="activeMenu==='blacklist'">
    <div class="section-head">
      <div>
        <h2 class="title">{{ tr('blacklist.title') }}</h2>
        <div class="section-subtitle">{{ blacklistSummaryText }}</div>
      </div>
    </div>
    <div class="blacklist-create">
      <el-select v-model="blacklistForm.type" class="blacklist-type-select">
        <el-option :label="tr('blacklist.type.clientId')" value="clientId"/>
        <el-option :label="tr('blacklist.type.ip')" value="ip"/>
      </el-select>
      <el-input
        v-model="blacklistForm.value"
        :placeholder="blacklistForm.type==='ip' ? tr('blacklist.input.ip') : tr('blacklist.input.clientId')"
        @keyup.enter="submitBlacklistEntry"
      />
      <el-button type="danger" @click="submitBlacklistEntry">{{ tr('blacklist.action.add') }}</el-button>
    </div>
    <div class="chips toolbar-chips">
      <span class="chip">{{ tr('blacklist.chip.exact') }}</span>
      <span class="chip">{{ tr('blacklist.chip.disconnect') }}</span>
      <span class="chip">{{ tr('blacklist.chip.sync') }}</span>
    </div>
    <el-table :data="blacklistEntries" :empty-text="tr('blacklist.empty')">
      <el-table-column prop="type" :label="tr('blacklist.table.type')" min-width="120">
        <template #default="{ row }">{{ blacklistTypeLabel(row.type) }}</template>
      </el-table-column>
      <el-table-column prop="value" :label="tr('blacklist.table.value')" min-width="260"/>
      <el-table-column prop="source" :label="tr('blacklist.table.source')" min-width="180"/>
      <el-table-column prop="createdAt" :label="tr('blacklist.table.createdAt')" min-width="180">
        <template #default="{ row }">{{ formatDateTime(row.createdAt) }}</template>
      </el-table-column>
      <el-table-column :label="tr('blacklist.table.actions')" width="140" fixed="right">
        <template #default="{ row }">
          <el-button type="danger" link @click="removeBlacklistEntry(row)">{{ tr('blacklist.action.remove') }}</el-button>
        </template>
      </el-table-column>
    </el-table>
  </section>
`;
