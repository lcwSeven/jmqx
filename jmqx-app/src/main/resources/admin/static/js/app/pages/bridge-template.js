export const bridgePageTemplate = `
  <section class="panel auth-surface" v-if="activeMenu==='bridge' && !bridgeCreateMode">
    <div class="section-head">
      <div>
        <h2 class="title auth-title">桥接全局配置</h2>
        <div class="section-subtitle">全局 Topic Filters 和异步桥接参数会统一作用到所有已创建桥接器。</div>
      </div>
      <el-button type="primary" @click="saveBridgeGlobalConfig">保存全局配置</el-button>
    </div>

    <div class="form-grid auth-config-grid auth-config-stack">
      <label class="field">
        <span class="field-label">全局 Topic Filters</span>
        <el-input
          :model-value="joinLine(bridgeConfig.topicFilters)"
          type="textarea"
          :rows="4"
          placeholder="留空表示桥接所有非 Dashboard 主题，每行一个 topic filter"
          @input="bridgeConfig.topicFilters = $event"
        />
        <div class="hint">留空表示桥接所有非 Dashboard 主题；填写后仅桥接命中的主题。</div>
      </label>

      <details class="advanced-block">
        <summary class="advanced-summary">高级配置：异步桥接</summary>
        <div class="advanced-fields">
          <label class="field">
            <span class="field-label">启用异步桥接</span>
            <el-switch v-model="bridgeConfig.asyncEnabled" active-text="是" inactive-text="否"></el-switch>
          </label>
          <label class="field">
            <span class="field-label">异步队列容量</span>
            <el-input-number v-model="bridgeConfig.asyncQueueCapacity" :min="1024" controls-position="right" />
          </label>
          <label class="field">
            <span class="field-label">异步 Worker 数</span>
            <el-input-number v-model="bridgeConfig.asyncWorkerCount" :min="1" controls-position="right" />
          </label>
        </div>
      </details>
    </div>

    <div class="auth-chain-preview">
      <div class="field-label">当前桥接器</div>
      <div class="auth-chain-preview-value">
        {{ joinComma(bridgeConfig.types) || '未创建桥接器' }}
      </div>
      <div class="hint">桥接器之间互相独立，不存在链式执行顺序；是否生效由各自的启用状态决定。</div>
    </div>

    <div class="auth-list-toolbar bridge-list-toolbar">
      <div>
        <h2 class="title auth-title">桥接器列表</h2>
        <div class="section-subtitle">创建和管理具体的 Kafka、RocketMQ、MySQL 桥接器。</div>
      </div>
      <el-button type="primary" size="large" @click="openBridgeCreate" :disabled="!availableBridgeDatasourceOptions.length">+ 创建</el-button>
    </div>

    <el-table v-if="bridgeEntries.length" :data="bridgeEntries" stripe class="ep-table auth-ep-table">
      <el-table-column label="桥接器" min-width="280">
        <template #default="scope">
          <div class="auth-main-cell">
            <div class="auth-main-name">{{ scope.row.displayName }}</div>
            <div class="auth-main-sub">{{ scope.row.summary }}</div>
          </div>
        </template>
      </el-table-column>

      <el-table-column label="状态" width="140">
        <template #default="scope">
          <el-tag :type="bridgeEntryEnabled(scope.row.type) ? 'success' : 'info'" effect="light">
            {{ bridgeEntryEnabled(scope.row.type) ? '已启用' : '已停用' }}
          </el-tag>
        </template>
      </el-table-column>

      <el-table-column label="局部 Topic Filters" min-width="240">
        <template #default="scope">
          {{ scope.row.type === 'rocketmq'
              ? (joinComma(bridgeConfig.rocketmq.sourceTopicFilters) || '未配置')
              : scope.row.type === 'mysql'
                  ? (joinComma(bridgeConfig.mysql.sourceTopicFilters) || '未配置')
                  : (joinComma(bridgeConfig.kafka.sourceTopicFilters) || '未配置') }}
        </template>
      </el-table-column>

      <el-table-column label="操作" min-width="240">
        <template #default="scope">
          <div class="auth-actions ep-actions auth-list-actions">
            <el-switch
              :model-value="bridgeEntryEnabled(scope.row.type)"
              active-text="启用"
              inactive-text="停用"
              @change="toggleBridgeEntryEnabled(scope.row.type, $event)"
            />
            <el-button size="small" @click="openBridgeSettings(scope.row.type)">设置</el-button>
            <el-button size="small" type="danger" plain @click="removeBridgeEntry(scope.row.type)">删除</el-button>
          </div>
        </template>
      </el-table-column>
    </el-table>

    <el-empty v-else description="暂无桥接器配置。" class="auth-empty"></el-empty>
  </section>

  <section class="panel auth-surface" v-if="activeMenu==='bridge' && bridgeCreateMode">
    <div class="auth-create-header">
      <el-button plain @click="cancelBridgeCreate">返回</el-button>
      <span class="auth-sep">|</span>
      <h2 class="title auth-title">{{ bridgeEditingType ? '设置桥接器' : '创建桥接器' }}</h2>
    </div>

    <el-steps :active="bridgeStep" align-center finish-status="success" class="auth-steps">
      <el-step title="桥接器类型"></el-step>
      <el-step title="配置参数"></el-step>
    </el-steps>

    <div v-if="bridgeStep===1">
      <div class="auth-hint">选择要创建的桥接器类型</div>
      <div class="auth-option-grid datasource-grid">
        <el-button
          v-for="item in bridgeDraftDatasourceOptions"
          :key="item.key"
          class="auth-option-card ep-option-card"
          :type="bridgeDraft.datasource===item.key ? 'primary' : 'default'"
          :plain="bridgeDraft.datasource!==item.key"
          @click="selectBridgeDatasource(item.key)">
          <span class="ds-icon">{{ item.icon }}</span>{{ item.label }}
        </el-button>
      </div>
    </div>

    <div v-if="bridgeStep===2">
      <div class="auth-hint">当前桥接器：{{ bridgeDatasourceLabel() }}</div>

      <div class="form-grid auth-config-grid auth-config-stack">
        <div class="auth-chain-preview">
          <div class="field-label">保存后生效的桥接器</div>
          <div class="auth-chain-preview-value">
            {{ joinComma(bridgeEditingType ? bridgeConfig.types : [...bridgeConfig.types, bridgeDraft.datasource].filter((value, index, array) => value && array.indexOf(value) === index)) || '未创建桥接器' }}
          </div>
          <div class="hint">全局 Topic Filters 请在上一页顶部配置；这里仅维护当前桥接器自己的连接和目标参数。</div>
        </div>

        <label class="field">
          <span class="field-label">启用当前桥接器</span>
          <el-switch
            v-if="bridgeDraft.datasource==='rocketmq'"
            v-model="bridgeDraft.rocketmq.enabled"
            active-text="启用"
            inactive-text="停用"
          ></el-switch>
          <el-switch
            v-else-if="bridgeDraft.datasource==='mysql'"
            v-model="bridgeDraft.mysql.enabled"
            active-text="启用"
            inactive-text="停用"
          ></el-switch>
          <el-switch
            v-else
            v-model="bridgeDraft.kafka.enabled"
            active-text="启用"
            inactive-text="停用"
          ></el-switch>
          <div class="hint">每个桥接器都可以单独启用或停用，不影响其他桥接器。</div>
        </label>

        <template v-if="bridgeDraft.datasource==='kafka'">
          <label class="field">
            <span class="field-label">Bootstrap Servers</span>
            <el-input v-model="bridgeDraft.kafka.bootstrapServers" placeholder="127.0.0.1:9092" />
          </label>
          <label class="field">
            <span class="field-label">目标 Topic</span>
            <el-input v-model="bridgeDraft.kafka.topic" placeholder="jmqx-messages" />
          </label>
          <label class="field">
            <span class="field-label">局部 Topic Filters</span>
            <el-input
              :model-value="joinLine(bridgeDraft.kafka.sourceTopicFilters)"
              type="textarea"
              :rows="3"
              placeholder="每行一个 topic filter，留空表示不过滤"
              @input="bridgeDraft.kafka.sourceTopicFilters = $event"
            />
          </label>
          <label class="field">
            <span class="field-label">acks</span>
            <el-input v-model="bridgeDraft.kafka.acks" placeholder="1" />
          </label>
          <label class="field">
            <span class="field-label">clientId</span>
            <el-input v-model="bridgeDraft.kafka.clientId" placeholder="jmqx-bridge" />
          </label>
          <label class="field">
            <span class="field-label">compressionType</span>
            <el-input v-model="bridgeDraft.kafka.compressionType" placeholder="none" />
          </label>
        </template>

        <template v-if="bridgeDraft.datasource==='rocketmq'">
          <label class="field">
            <span class="field-label">NameServer</span>
            <el-input v-model="bridgeDraft.rocketmq.nameServer" placeholder="127.0.0.1:9876" />
          </label>
          <label class="field">
            <span class="field-label">Producer Group</span>
            <el-input v-model="bridgeDraft.rocketmq.producerGroup" placeholder="jmqx-bridge-group" />
          </label>
          <label class="field">
            <span class="field-label">目标 Topic</span>
            <el-input v-model="bridgeDraft.rocketmq.topic" placeholder="JMQX_MESSAGES" />
          </label>
          <label class="field">
            <span class="field-label">局部 Topic Filters</span>
            <el-input
              :model-value="joinLine(bridgeDraft.rocketmq.sourceTopicFilters)"
              type="textarea"
              :rows="3"
              placeholder="每行一个 topic filter，留空表示不过滤"
              @input="bridgeDraft.rocketmq.sourceTopicFilters = $event"
            />
          </label>
          <label class="field">
            <span class="field-label">同步发送</span>
            <el-switch v-model="bridgeDraft.rocketmq.syncSend" active-text="是" inactive-text="否"></el-switch>
          </label>
          <label class="field">
            <span class="field-label">超时时间（毫秒）</span>
            <el-input-number v-model="bridgeDraft.rocketmq.timeoutMs" :min="100" controls-position="right" />
          </label>
        </template>

        <template v-if="bridgeDraft.datasource==='mysql'">
          <label class="field">
            <span class="field-label">JDBC Driver</span>
            <el-input v-model="bridgeDraft.mysql.driver" placeholder="com.mysql.cj.jdbc.Driver" />
          </label>
          <label class="field">
            <span class="field-label">JDBC URL</span>
            <el-input v-model="bridgeDraft.mysql.url" placeholder="jdbc:mysql://127.0.0.1:3306/jmqx" />
          </label>
          <label class="field">
            <span class="field-label">用户名</span>
            <el-input v-model="bridgeDraft.mysql.user" placeholder="root" />
          </label>
          <label class="field">
            <span class="field-label">密码</span>
            <el-input v-model="bridgeDraft.mysql.password" type="password" show-password />
          </label>
          <label class="field">
            <span class="field-label">表名</span>
            <el-input v-model="bridgeDraft.mysql.table" placeholder="jmqx_bridge_message" />
          </label>
          <label class="field">
            <span class="field-label">局部 Topic Filters</span>
            <el-input
              :model-value="joinLine(bridgeDraft.mysql.sourceTopicFilters)"
              type="textarea"
              :rows="3"
              placeholder="每行一个 topic filter，留空表示不过滤"
              @input="bridgeDraft.mysql.sourceTopicFilters = $event"
            />
          </label>
          <label class="field">
            <span class="field-label">自动建表</span>
            <el-switch v-model="bridgeDraft.mysql.autoCreateTable" active-text="是" inactive-text="否"></el-switch>
          </label>
        </template>
      </div>
    </div>

    <div class="auth-footer-actions">
      <el-button @click="cancelBridgeCreate">取消</el-button>
      <el-button v-if="bridgeStep>1" @click="previousBridgeStep">上一步</el-button>
      <el-button type="primary" v-if="bridgeStep<2" @click="nextBridgeStep">下一步</el-button>
      <el-button type="primary" v-if="bridgeStep===2" @click="createBridgeAndSave">{{ bridgeEditingType ? '保存' : '创建' }}</el-button>
    </div>
  </section>
`;
