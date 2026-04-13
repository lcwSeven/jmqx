export const auditPageTemplate = `
  <section class="panel" v-if="activeMenu==='audit'">
    <div class="section-head">
      <div>
        <h2 class="title">操作审计</h2>
        <div class="section-subtitle">{{ auditSummaryText }}</div>
      </div>
      <div class="toolbar">
        <select v-model="auditFilter" class="audit-select">
          <option v-for="action in auditActions" :key="action" :value="action">
            {{ action === 'all' ? '全部操作' : action }}
          </option>
        </select>
        <button class="btn" @click="loadAuditLogs">刷新</button>
      </div>
    </div>
    <div class="audit-list" v-if="filteredAuditLogs && filteredAuditLogs.length">
      <article class="audit-card" v-for="entry in filteredAuditLogs" :key="entry.id">
        <div class="audit-card-head">
          <div>
            <div class="audit-action">{{ entry.action }}</div>
            <div class="audit-meta">{{ entry.source }} · {{ formatDateTime(entry.timestamp) }}</div>
          </div>
          <div class="audit-card-actions">
            <span class="status-badge is-up">{{ entry.clusterId }}</span>
            <button class="btn secondary audit-btn" @click="toggleAudit(entry.id)">
              {{ isAuditExpanded(entry.id) ? '收起' : '展开' }}
            </button>
            <button class="btn secondary audit-btn" @click="copyAudit(entry)">复制</button>
          </div>
        </div>
        <div class="audit-grid" v-if="isAuditExpanded(entry.id)">
          <div>
            <div class="audit-label">变更前</div>
            <pre class="audit-json">{{ formatJsonPreview(entry.beforeJson) }}</pre>
          </div>
          <div>
            <div class="audit-label">变更后</div>
            <pre class="audit-json">{{ formatJsonPreview(entry.afterJson) }}</pre>
          </div>
        </div>
      </article>
    </div>
    <div class="hint" v-else>暂无符合条件的审计记录，配置保存后会在这里显示。</div>
  </section>
`;
