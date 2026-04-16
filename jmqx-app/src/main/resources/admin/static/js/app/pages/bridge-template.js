export const bridgePageTemplate = `
  <section class="panel auth-surface" v-if="activeMenu==='bridge' && !bridgeCreateMode">
    <div class="section-head">
      <div>
        <h2 class="title auth-title">{{ tr('bridge.global.title') }}</h2>
        <div class="section-subtitle">{{ tr('bridge.global.subtitle') }}</div>
      </div>
      <el-button type="primary" @click="saveBridgeGlobalConfig">{{ tr('bridge.global.save') }}</el-button>
    </div>

    <div class="form-grid auth-config-grid auth-config-stack">
      <label class="field">
        <span class="field-label">{{ tr('bridge.global.topicFilters') }}</span>
        <el-input
          :model-value="joinLine(bridgeConfig.topicFilters)"
          type="textarea"
          :rows="4"
          :placeholder="tr('bridge.global.placeholder')"
          @input="bridgeConfig.topicFilters = $event"
        />
        <div class="hint">{{ tr('bridge.global.hint') }}</div>
      </label>

      <details class="advanced-block">
        <summary class="advanced-summary">{{ tr('bridge.global.advanced') }}</summary>
        <div class="advanced-fields">
          <label class="field">
            <span class="field-label">{{ tr('bridge.global.asyncEnabled') }}</span>
            <el-switch v-model="bridgeConfig.asyncEnabled" :active-text="tr('common.yes')" :inactive-text="tr('common.no')"></el-switch>
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.global.asyncQueueCapacity') }}</span>
            <el-input-number v-model="bridgeConfig.asyncQueueCapacity" :min="1024" controls-position="right" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.global.asyncWorkerCount') }}</span>
            <el-input-number v-model="bridgeConfig.asyncWorkerCount" :min="1" controls-position="right" />
          </label>
        </div>
      </details>
    </div>

    <div class="auth-chain-preview">
      <div class="field-label">{{ tr('bridge.global.currentBridges') }}</div>
      <div class="auth-chain-preview-value">
        {{ joinComma(bridgeConfig.types) || tr('bridge.global.empty') }}
      </div>
      <div class="hint">{{ tr('bridge.global.chainHint') }}</div>
    </div>

    <div class="auth-list-toolbar bridge-list-toolbar">
      <div>
        <h2 class="title auth-title">{{ tr('bridge.list.title') }}</h2>
        <div class="section-subtitle">{{ tr('bridge.list.subtitle') }}</div>
      </div>
      <el-button type="primary" size="large" @click="openBridgeCreate" :disabled="!availableBridgeDatasourceOptions.length">{{ tr('bridge.list.create') }}</el-button>
    </div>

    <el-table v-if="bridgeEntries.length" :data="bridgeEntries" stripe class="ep-table auth-ep-table">
      <el-table-column :label="tr('bridge.list.column.bridge')" min-width="280">
        <template #default="scope">
          <div class="auth-main-cell">
            <div class="auth-main-name">{{ scope.row.displayName }}</div>
            <div class="auth-main-sub">{{ scope.row.summary }}</div>
          </div>
        </template>
      </el-table-column>

      <el-table-column :label="tr('bridge.list.column.status')" width="140">
        <template #default="scope">
          <el-tag :type="bridgeEntryEnabled(scope.row.type) ? 'success' : 'info'" effect="light">
            {{ bridgeEntryEnabled(scope.row.type) ? tr('status.enabled') : tr('status.disabled') }}
          </el-tag>
        </template>
      </el-table-column>

      <el-table-column :label="tr('bridge.list.column.localFilters')" min-width="240">
        <template #default="scope">
          {{ scope.row.type === 'rocketmq'
              ? (joinComma(bridgeConfig.rocketmq.sourceTopicFilters) || tr('status.notConfigured'))
              : scope.row.type === 'mysql'
                  ? (joinComma(bridgeConfig.mysql.sourceTopicFilters) || tr('status.notConfigured'))
                  : (joinComma(bridgeConfig.kafka.sourceTopicFilters) || tr('status.notConfigured')) }}
        </template>
      </el-table-column>

      <el-table-column :label="tr('bridge.list.column.actions')" min-width="240">
        <template #default="scope">
          <div class="auth-actions ep-actions auth-list-actions">
            <el-switch
              :model-value="bridgeEntryEnabled(scope.row.type)"
              :active-text="tr('bridge.list.enable')"
              :inactive-text="tr('bridge.list.disable')"
              @change="toggleBridgeEntryEnabled(scope.row.type, $event)"
            />
            <el-button size="small" @click="openBridgeSettings(scope.row.type)">{{ tr('security.common.edit') }}</el-button>
            <el-button size="small" type="danger" plain @click="removeBridgeEntry(scope.row.type)">{{ tr('security.common.delete') }}</el-button>
          </div>
        </template>
      </el-table-column>
    </el-table>

    <el-empty v-else :description="tr('bridge.list.empty')" class="auth-empty"></el-empty>
  </section>

  <section class="panel auth-surface" v-if="activeMenu==='bridge' && bridgeCreateMode">
    <div class="auth-create-header">
      <el-button plain @click="cancelBridgeCreate">{{ tr('common.back') }}</el-button>
      <span class="auth-sep">|</span>
      <h2 class="title auth-title">{{ bridgeEditingType ? tr('bridge.edit.title') : tr('bridge.create.title') }}</h2>
    </div>

    <el-steps :active="bridgeStep" align-center finish-status="success" class="auth-steps">
      <el-step :title="tr('bridge.step.type')"></el-step>
      <el-step :title="tr('bridge.step.config')"></el-step>
    </el-steps>

    <div v-if="bridgeStep===1">
      <div class="auth-hint">{{ tr('bridge.hint.selectType') }}</div>
      <div class="auth-option-grid datasource-grid">
        <el-button
          v-for="item in bridgeDraftDatasourceOptions"
          :key="item.key"
          class="auth-option-card ep-option-card"
          :type="bridgeDraft.datasource===item.key ? 'primary' : 'default'"
          :plain="bridgeDraft.datasource!==item.key"
          @click="selectBridgeDatasource(item.key)">
          <span class="ds-icon">{{ item.icon }}</span>{{ tr(item.label) }}
        </el-button>
      </div>
    </div>

    <div v-if="bridgeStep===2">
      <div class="auth-hint">{{ tr('bridge.hint.current', { value: bridgeDatasourceLabel() }) }}</div>

      <div class="form-grid auth-config-grid auth-config-stack">
        <div class="auth-chain-preview">
          <div class="field-label">{{ tr('bridge.chainAfterSave') }}</div>
          <div class="auth-chain-preview-value">
            {{ joinComma(bridgeEditingType ? bridgeConfig.types : [...bridgeConfig.types, bridgeDraft.datasource].filter((value, index, array) => value && array.indexOf(value) === index)) || tr('bridge.global.empty') }}
          </div>
          <div class="hint">{{ tr('bridge.hint.parameters') }}</div>
        </div>

        <label class="field">
          <span class="field-label">{{ tr('bridge.field.enableCurrent') }}</span>
          <el-switch
            v-if="bridgeDraft.datasource==='rocketmq'"
            v-model="bridgeDraft.rocketmq.enabled"
            :active-text="tr('bridge.list.enable')"
            :inactive-text="tr('bridge.list.disable')"
          ></el-switch>
          <el-switch
            v-else-if="bridgeDraft.datasource==='mysql'"
            v-model="bridgeDraft.mysql.enabled"
            :active-text="tr('bridge.list.enable')"
            :inactive-text="tr('bridge.list.disable')"
          ></el-switch>
          <el-switch
            v-else
            v-model="bridgeDraft.kafka.enabled"
            :active-text="tr('bridge.list.enable')"
            :inactive-text="tr('bridge.list.disable')"
          ></el-switch>
          <div class="hint">{{ tr('bridge.hint.enableCurrent') }}</div>
        </label>

        <template v-if="bridgeDraft.datasource==='kafka'">
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.bootstrapServers') }}</span>
            <el-input v-model="bridgeDraft.kafka.bootstrapServers" placeholder="127.0.0.1:9092" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.targetTopic') }}</span>
            <el-input v-model="bridgeDraft.kafka.topic" placeholder="jmqx-messages" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.localTopicFilters') }}</span>
            <el-input
              :model-value="joinLine(bridgeDraft.kafka.sourceTopicFilters)"
              type="textarea"
              :rows="3"
              :placeholder="tr('bridge.placeholder.localTopicFilters')"
              @input="bridgeDraft.kafka.sourceTopicFilters = $event"
            />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.acks') }}</span>
            <el-input v-model="bridgeDraft.kafka.acks" placeholder="1" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.clientId') }}</span>
            <el-input v-model="bridgeDraft.kafka.clientId" placeholder="jmqx-bridge" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.compressionType') }}</span>
            <el-input v-model="bridgeDraft.kafka.compressionType" placeholder="none" />
          </label>
        </template>

        <template v-if="bridgeDraft.datasource==='rocketmq'">
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.nameServer') }}</span>
            <el-input v-model="bridgeDraft.rocketmq.nameServer" placeholder="127.0.0.1:9876" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.producerGroup') }}</span>
            <el-input v-model="bridgeDraft.rocketmq.producerGroup" placeholder="jmqx-bridge-group" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.targetTopic') }}</span>
            <el-input v-model="bridgeDraft.rocketmq.topic" placeholder="JMQX_MESSAGES" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.localTopicFilters') }}</span>
            <el-input
              :model-value="joinLine(bridgeDraft.rocketmq.sourceTopicFilters)"
              type="textarea"
              :rows="3"
              :placeholder="tr('bridge.placeholder.localTopicFilters')"
              @input="bridgeDraft.rocketmq.sourceTopicFilters = $event"
            />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.syncSend') }}</span>
            <el-switch v-model="bridgeDraft.rocketmq.syncSend" :active-text="tr('common.yes')" :inactive-text="tr('common.no')"></el-switch>
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.timeoutMs') }}</span>
            <el-input-number v-model="bridgeDraft.rocketmq.timeoutMs" :min="100" controls-position="right" />
          </label>
        </template>

        <template v-if="bridgeDraft.datasource==='mysql'">
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.jdbcDriver') }}</span>
            <el-input v-model="bridgeDraft.mysql.driver" placeholder="com.mysql.cj.jdbc.Driver" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.jdbcUrl') }}</span>
            <el-input v-model="bridgeDraft.mysql.url" placeholder="jdbc:mysql://127.0.0.1:3306/jmqx" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.username') }}</span>
            <el-input v-model="bridgeDraft.mysql.user" placeholder="root" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.password') }}</span>
            <el-input v-model="bridgeDraft.mysql.password" type="password" show-password />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.tableName') }}</span>
            <el-input v-model="bridgeDraft.mysql.table" placeholder="jmqx_bridge_message" />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.localTopicFilters') }}</span>
            <el-input
              :model-value="joinLine(bridgeDraft.mysql.sourceTopicFilters)"
              type="textarea"
              :rows="3"
              :placeholder="tr('bridge.placeholder.localTopicFilters')"
              @input="bridgeDraft.mysql.sourceTopicFilters = $event"
            />
          </label>
          <label class="field">
            <span class="field-label">{{ tr('bridge.field.autoCreateTable') }}</span>
            <el-switch v-model="bridgeDraft.mysql.autoCreateTable" :active-text="tr('common.yes')" :inactive-text="tr('common.no')"></el-switch>
          </label>
        </template>
      </div>
    </div>

    <div class="auth-footer-actions">
      <el-button @click="cancelBridgeCreate">{{ tr('common.cancel') }}</el-button>
      <el-button v-if="bridgeStep>1" @click="previousBridgeStep">{{ tr('common.previous') }}</el-button>
      <el-button type="primary" v-if="bridgeStep<2" @click="nextBridgeStep">{{ tr('common.next') }}</el-button>
      <el-button type="primary" v-if="bridgeStep===2" @click="createBridgeAndSave">{{ bridgeEditingType ? tr('common.save') : tr('common.create') }}</el-button>
    </div>
  </section>
`;
