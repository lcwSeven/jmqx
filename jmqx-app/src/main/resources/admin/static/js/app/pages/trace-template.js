export const tracePageTemplate = `
  <section class="panel" v-if="activeMenu==='trace'">
    <div class="section-head">
      <div>
        <h2 class="title">日志追踪</h2>
        <div class="section-subtitle">{{ clientTraceSummaryText }}</div>
      </div>
    </div>
    <div class="trace-create">
      <label class="field">
        <span class="field-label">客户端 ID</span>
        <el-input v-model="clientTraceForm.clientId" placeholder="请输入 clientId"/>
      </label>
      <label class="field">
        <span class="field-label">开始时间</span>
        <input v-model="clientTraceForm.startAt" type="datetime-local"/>
      </label>
      <label class="field">
        <span class="field-label">持续时长（分钟）</span>
        <el-input-number v-model="clientTraceForm.durationMinutes" :min="1" :max="30"/>
      </label>
      <div class="trace-create-actions">
        <el-button type="primary" @click="submitClientTrace">创建追踪任务</el-button>
      </div>
    </div>
    <div class="chips toolbar-chips">
      <span class="chip">开始时间必须是未来时间</span>
      <span class="chip">单次时长最长 30 分钟</span>
      <span class="chip">日志会写入独立文件</span>
    </div>
    <el-table :data="clientTraces" empty-text="暂无日志追踪任务">
      <el-table-column prop="clientId" label="clientId" min-width="180"/>
      <el-table-column prop="status" label="状态" width="120">
        <template #default="{ row }">{{ clientTraceStatusLabel(row.status) }}</template>
      </el-table-column>
      <el-table-column prop="startAt" label="开始时间" min-width="180">
        <template #default="{ row }">{{ formatDateTime(row.startAt) }}</template>
      </el-table-column>
      <el-table-column prop="endAt" label="结束时间" min-width="180">
        <template #default="{ row }">{{ formatDateTime(row.endAt) }}</template>
      </el-table-column>
      <el-table-column prop="durationMinutes" label="时长" width="100">
        <template #default="{ row }">{{ row.durationMinutes }} 分钟</template>
      </el-table-column>
      <el-table-column prop="filePath" label="日志文件" min-width="360"/>
      <el-table-column prop="createdBy" label="创建人" min-width="180"/>
      <el-table-column label="操作" width="120" fixed="right">
        <template #default="{ row }">
          <el-button type="danger" link @click="removeClientTrace(row)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>
  </section>
`;
