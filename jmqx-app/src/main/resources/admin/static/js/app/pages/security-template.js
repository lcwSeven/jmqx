export const securityPageTemplate = `
  <section class="panel auth-surface" v-if="activeMenu==='acl' && !aclCreateMode">
    <div class="auth-list-toolbar">
      <div>
        <h2 class="title auth-title">{{ tr('security.acl.title') }}</h2>
        <div class="section-subtitle">{{ tr('security.acl.subtitle') }}</div>
      </div>
      <el-button type="primary" size="large" @click="openAclCreate" :disabled="!availableAclDatasourceOptions.length">{{ tr('security.acl.create') }}</el-button>
    </div>
    <el-table v-if="aclEntries.length" :data="aclEntries" stripe class="ep-table auth-ep-table">
      <el-table-column :label="tr('security.acl.column.datasource')" min-width="260">
        <template #default="scope">
          <div class="auth-main-cell">
            <div class="auth-main-name">{{ scope.row.displayName }}</div>
            <div class="auth-main-sub">{{ scope.row.summary }}</div>
          </div>
        </template>
      </el-table-column>
      <el-table-column :label="tr('security.acl.column.order')" width="120">
        <template #default="scope">
          {{ tr('security.acl.column.orderValue', { index: scope.row.index + 1 }) }}
        </template>
      </el-table-column>
      <el-table-column :label="tr('security.acl.column.defaultPolicy')" width="140">
        <template #default>
          <el-tag :type="securityConfig.aclDefaultAllow ? 'warning' : 'info'" effect="light">
            {{ securityConfig.aclDefaultAllow ? tr('security.acl.defaultAllow') : tr('security.acl.defaultDeny') }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="tr('security.acl.column.enabled')" width="120">
        <template #default>
          <el-switch v-model="securityConfig.aclEnabled" @change="toggleAclEnabled"/>
        </template>
      </el-table-column>
      <el-table-column :label="tr('clients.table.actions')" min-width="320">
        <template #default="scope">
          <div class="auth-actions ep-actions">
            <el-button size="small" @click="moveAclEntryUp(scope.row.plugin)" :disabled="scope.row.index===0">{{ tr('security.common.moveUp') }}</el-button>
            <el-button size="small" @click="moveAclEntryDown(scope.row.plugin)" :disabled="scope.row.index===aclEntries.length-1">{{ tr('security.common.moveDown') }}</el-button>
            <el-button size="small" @click="openAclSettings(scope.row.plugin)">{{ tr('security.common.edit') }}</el-button>
            <el-button size="small" type="danger" plain @click="removeAclEntry(scope.row.plugin)">{{ tr('security.common.delete') }}</el-button>
          </div>
        </template>
      </el-table-column>
    </el-table>
    <el-empty v-else :description="tr('security.acl.empty')" class="auth-empty"></el-empty>
    <div class="hint">{{ tr('security.acl.currentChain') }}{{ joinComma(securityConfig.aclChain) || tr('security.common.notConfiguredAllowAll') }}</div>
  </section>

  <section class="panel auth-surface" v-if="activeMenu==='acl' && aclCreateMode">
    <div class="auth-create-header">
      <el-button plain @click="cancelAclCreate">{{ tr('common.back') }}</el-button>
      <span class="auth-sep">|</span>
      <h2 class="title auth-title">{{ tr('security.acl.createTitle') }}</h2>
    </div>

    <el-steps :active="aclStep" align-center finish-status="success" class="auth-steps">
      <el-step :title="tr('security.acl.step.method')"></el-step>
      <el-step :title="tr('security.acl.step.source')"></el-step>
      <el-step :title="tr('security.acl.step.config')"></el-step>
    </el-steps>

    <div v-if="aclStep===1">
      <div class="auth-hint">{{ tr('security.acl.hint.scope') }}</div>
      <div class="auth-option-grid">
        <el-button
            v-for="item in aclMethodOptions"
            :key="item.key"
            class="auth-option-card ep-option-card"
            :type="aclDraft.method===item.key ? 'primary' : 'default'"
            :plain="aclDraft.method!==item.key"
            @click="selectAclMethod(item.key)">
          {{ tr(item.label) }}
        </el-button>
      </div>
    </div>

    <div v-if="aclStep===2">
      <div class="auth-hint">{{ tr('security.acl.hint.source') }}</div>
      <div class="auth-option-grid datasource-grid">
        <el-button
            v-for="item in aclDraftDatasourceOptions"
            :key="item.key"
            class="auth-option-card ep-option-card"
            :type="aclDraft.datasource===item.key ? 'primary' : 'default'"
            :plain="aclDraft.datasource!==item.key"
            @click="selectAclDatasource(item.key)">
          <span class="ds-icon">{{ item.icon }}</span>{{ tr(item.label) }}
        </el-button>
      </div>
    </div>

    <div v-if="aclStep===3">
      <div class="auth-hint">{{ tr('security.acl.hint.current', { method: aclMethodLabel(), source: aclDatasourceLabel() }) }}</div>
      <div class="form-grid auth-config-grid auth-config-stack">
        <div class="auth-chain-preview">
          <div class="field-label">{{ tr('security.acl.chainAfterSave') }}</div>
          <div class="auth-chain-preview-value">
            {{ joinComma(aclEditingPlugin ? securityConfig.aclChain : [...securityConfig.aclChain, mapAclDatasourceToPlugin(aclDraft.datasource)].filter((value, index, array) => value && array.indexOf(value) === index)) || tr('security.common.notConfiguredAllowAll') }}
          </div>
          <div class="hint">{{ tr('security.acl.chainHint') }}</div>
        </div>
        <label class="field">
          <span class="field-label">{{ tr('security.acl.field.cacheTtl') }}</span>
          <el-input-number v-model="aclDraft.cacheTtlMs" :min="0" controls-position="right" />
        </label>
        <label class="field">
          <span class="field-label">{{ tr('security.acl.field.defaultAllow') }}</span>
          <el-switch v-model="aclDraft.defaultAllow" :active-text="tr('common.yes')" :inactive-text="tr('common.no')"></el-switch>
          <div class="hint">{{ tr('security.acl.hint.defaultAllow') }}</div>
        </label>
        <label class="field" v-if="aclDraft.datasource==='file'">
          <span class="field-label">{{ tr('security.acl.field.filePath') }}</span>
          <el-input v-model="aclDraft.filePath" placeholder="acl-rules.txt"/>
        </label>
        <label class="field" v-if="aclDraft.datasource==='http'">
          <span class="field-label">{{ tr('security.acl.field.httpUrl') }}</span>
          <el-input v-model="aclDraft.httpUrl" placeholder="http://127.0.0.1:8080/acl/check"/>
        </label>
        <label class="field" v-if="aclDraft.datasource==='http'">
          <span class="field-label">{{ tr('security.acl.field.httpTimeout') }}</span>
          <el-input-number v-model="aclDraft.httpTimeoutMs" :min="200" controls-position="right" />
        </label>
        <label class="field" v-if="aclDraft.datasource==='http'">
          <span class="field-label">{{ tr('security.common.requestBody') }}</span>
          <el-input
              v-model="aclDraft.httpBodyTemplate"
              type="textarea"
              :rows="8"
              placeholder='{"clientId":"\${clientId}","username":"\${username}","topic":"\${topic}","action":"\${action}"}'/>
          <div class="hint">{{ tr('security.common.availablePlaceholders') }}<code>\${clientId}</code>、<code>\${username}</code>、<code>\${topic}</code>、<code>\${action}</code></div>
        </label>
        <label class="field" v-if="aclDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisHost') }}</span>
          <el-input v-model="aclDraft.redisHost" placeholder="127.0.0.1"/>
        </label>
        <label class="field" v-if="aclDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisPort') }}</span>
          <el-input-number v-model="aclDraft.redisPort" :min="1" controls-position="right" />
        </label>
        <label class="field" v-if="aclDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisPassword') }}</span>
          <el-input v-model="aclDraft.redisPassword" type="password" show-password />
        </label>
        <label class="field" v-if="aclDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisDb') }}</span>
          <el-input-number v-model="aclDraft.redisDb" :min="0" controls-position="right" />
        </label>
        <label class="field" v-if="aclDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisKeyPrefix') }}</span>
          <el-input v-model="aclDraft.redisKeyPrefix" placeholder="jmqx:acl"/>
        </label>
        <label class="field" v-if="aclDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisTimeout') }}</span>
          <el-input-number v-model="aclDraft.redisTimeoutMs" :min="200" controls-position="right" />
        </label>
      </div>
    </div>

    <div class="auth-footer-actions">
      <el-button @click="cancelAclCreate">{{ tr('common.cancel') }}</el-button>
      <el-button v-if="aclStep>1" @click="previousAclStep">{{ tr('common.previous') }}</el-button>
      <el-button type="primary" v-if="aclStep<3" @click="nextAclStep">{{ tr('common.next') }}</el-button>
      <el-button type="primary" v-if="aclStep===3" @click="createAclAndSave">{{ aclEditingPlugin ? tr('common.save') : tr('common.create') }}</el-button>
    </div>
  </section>

  <section class="panel auth-surface" v-if="activeMenu==='auth' && !authCreateMode">
    <div class="auth-list-toolbar">
      <div>
        <h2 class="title auth-title">{{ tr('security.auth.title') }}</h2>
        <div class="section-subtitle">{{ tr('security.auth.subtitle') }}</div>
      </div>
      <el-button type="primary" size="large" @click="openAuthCreate">{{ tr('security.acl.create') }}</el-button>
    </div>
    <el-table v-if="authEntries.length" :data="authEntries" stripe class="ep-table auth-ep-table">
      <el-table-column :label="tr('security.auth.column.datasource')" min-width="260">
        <template #default="scope">
          <div class="auth-main-cell">
            <div class="auth-main-name">{{ scope.row.displayName }}</div>
            <div class="auth-main-sub">{{ scope.row.summary }}</div>
          </div>
        </template>
      </el-table-column>
      <el-table-column :label="tr('security.acl.column.order')" width="120">
        <template #default="scope">
          {{ tr('security.acl.column.orderValue', { index: scope.row.index + 1 }) }}
        </template>
      </el-table-column>
      <el-table-column :label="tr('security.auth.column.status')" width="120">
        <template #default>
          <el-tag :type="securityConfig.authEnabled ? 'success' : 'info'" effect="light">
            {{ securityConfig.authEnabled ? tr('status.enabled') : tr('status.disabled') }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="tr('security.acl.column.enabled')" width="120">
        <template #default>
          <el-switch v-model="securityConfig.authEnabled" @change="toggleAuthEnabled"/>
        </template>
      </el-table-column>
      <el-table-column :label="tr('clients.table.actions')" min-width="320">
        <template #default="scope">
          <div class="auth-actions ep-actions">
            <el-button size="small" @click="moveAuthEntryUp(scope.row.plugin)" :disabled="scope.row.index===0">{{ tr('security.common.moveUp') }}</el-button>
            <el-button size="small" @click="moveAuthEntryDown(scope.row.plugin)" :disabled="scope.row.index===authEntries.length-1">{{ tr('security.common.moveDown') }}</el-button>
            <el-button size="small" @click="openAuthSettings(scope.row.plugin)">{{ tr('security.common.edit') }}</el-button>
            <el-button size="small" v-if="scope.row.plugin==='built_in_database'" @click="openBuiltInUserManagement">{{ tr('security.auth.action.userManagement') }}</el-button>
            <el-button size="small" type="danger" plain @click="removeAuthEntry(scope.row.plugin)">{{ tr('security.common.delete') }}</el-button>
          </div>
        </template>
      </el-table-column>
    </el-table>
    <el-empty v-else :description="tr('security.auth.empty')" class="auth-empty"></el-empty>
    <div class="hint">{{ tr('security.auth.currentChain') }}{{ joinComma(securityConfig.authChain) || tr('security.common.notConfiguredAllowAll') }}</div>
  </section>

  <section class="panel auth-surface" v-if="activeMenu==='auth' && authCreateMode">
    <div class="auth-create-header">
      <el-button plain @click="cancelAuthCreate">{{ tr('common.back') }}</el-button>
      <span class="auth-sep">|</span>
      <h2 class="title auth-title">{{ tr('security.auth.createTitle') }}</h2>
    </div>

    <el-steps :active="authStep" align-center finish-status="success" class="auth-steps">
      <el-step :title="tr('security.auth.step.method')"></el-step>
      <el-step :title="tr('security.auth.step.source')"></el-step>
      <el-step :title="tr('security.auth.step.config')"></el-step>
    </el-steps>

    <div v-if="authStep===1">
      <div class="auth-hint">{{ tr('security.auth.hint.scope') }}</div>
      <div class="auth-option-grid">
        <el-button
            v-for="item in authMethodOptions"
            :key="item.key"
            class="auth-option-card ep-option-card"
            :type="authDraft.method===item.key ? 'primary' : 'default'"
            :plain="authDraft.method!==item.key"
            @click="selectAuthMethod(item.key)">
          {{ tr(item.label) }}
        </el-button>
      </div>
    </div>

    <div v-if="authStep===2">
      <div class="auth-hint">{{ tr('security.auth.hint.source') }}</div>
      <div class="auth-option-grid datasource-grid">
        <el-button
            v-for="item in authDraftDatasourceOptions"
            :key="item.key"
            class="auth-option-card ep-option-card"
            :type="authDraft.datasource===item.key ? 'primary' : 'default'"
            :plain="authDraft.datasource!==item.key"
            @click="selectAuthDatasource(item.key)">
          <span class="ds-icon">{{ item.icon }}</span>{{ tr(item.label) }}
        </el-button>
      </div>
    </div>

    <div v-if="authStep===3">
      <div class="auth-hint">{{ tr('security.auth.hint.current', { method: authMethodLabel(), source: authDatasourceLabel() }) }}</div>
      <div class="form-grid auth-config-grid auth-config-stack">
        <div class="auth-chain-preview">
          <div class="field-label">{{ tr('security.auth.chainAfterSave') }}</div>
          <div class="auth-chain-preview-value">
            {{ joinComma(authEditingPlugin ? securityConfig.authChain : [...securityConfig.authChain, mapDatasourceToPlugin(authDraft.datasource)].filter((value, index, array) => value && array.indexOf(value) === index)) || tr('security.common.notConfiguredAllowAll') }}
          </div>
          <div class="hint">{{ tr('security.auth.chainHint') }}</div>
        </div>
        <label class="field">
          <span class="field-label">{{ tr('security.auth.field.cacheTtl') }}</span>
          <el-input-number v-model="authDraft.cacheTtlMs" :min="0" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='built_in_database'">
          <span class="field-label">{{ tr('security.auth.field.accountType') }}</span>
          <el-select v-model="authDraft.builtInDatabaseAccountType">
            <el-option value="username" label="username"></el-option>
            <el-option value="clientId" label="clientId"></el-option>
          </el-select>
        </label>
        <label class="field" v-if="authDraft.datasource==='built_in_database'">
          <span class="field-label">{{ tr('security.auth.field.hashAlgorithm') }}</span>
          <el-select v-model="authDraft.builtInDatabasePasswordHashAlgorithm">
            <el-option value="plain" label="plain"></el-option>
            <el-option value="md5" label="md5"></el-option>
            <el-option value="sha" label="sha"></el-option>
            <el-option value="sha256" label="sha256"></el-option>
            <el-option value="sha512" label="sha512"></el-option>
            <el-option value="bcrypt" label="bcrypt"></el-option>
            <el-option value="pbkdf2" label="pbkdf2"></el-option>
          </el-select>
        </label>
        <label class="field" v-if="authDraft.datasource==='built_in_database'">
          <span class="field-label">{{ tr('security.auth.field.saltPosition') }}</span>
          <el-select v-model="authDraft.builtInDatabaseSaltPosition">
            <el-option value="disable" label="disable"></el-option>
            <el-option value="prefix" label="prefix"></el-option>
            <el-option value="suffix" label="suffix"></el-option>
          </el-select>
        </label>
        <label class="field" v-if="authDraft.datasource==='http'">
          <span class="field-label">{{ tr('security.auth.field.httpMethod') }}</span>
          <el-select v-model="authDraft.httpMethod">
            <el-option value="POST" label="POST"></el-option>
            <el-option value="GET" label="GET"></el-option>
            <el-option value="PUT" label="PUT"></el-option>
          </el-select>
        </label>
        <label class="field" v-if="authDraft.datasource==='http'">
          <span class="field-label">{{ tr('security.auth.field.httpUrl') }}</span>
          <el-input v-model="authDraft.httpUrl" placeholder="http://host:port/auth/check"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='http'">
          <span class="field-label">{{ tr('security.auth.field.tlsEnabled') }}</span>
          <el-switch v-model="authDraft.httpTlsEnabled" :active-text="tr('common.yes')" :inactive-text="tr('common.no')"></el-switch>
        </label>
        <div class="field auth-http-header-field" v-if="authDraft.datasource==='http'">
          <span class="field-label">{{ tr('security.auth.field.headers') }}</span>
          <div class="auth-http-headers">
            <div class="auth-http-header-row auth-http-header-head">
              <span>{{ tr('common.key') }}</span>
              <span>{{ tr('common.value') }}</span>
              <el-button text type="primary" @click="addHttpHeaderRow">{{ tr('common.add') }}</el-button>
            </div>
            <div class="auth-http-header-row" v-for="(header, index) in authDraft.httpHeaders" :key="'http-header-' + index">
              <el-input v-model="header.key" placeholder="content-type"/>
              <el-input v-model="header.value" placeholder="application/json"/>
              <el-button text type="danger" @click="removeHttpHeaderRow(index)">{{ tr('common.delete') }}</el-button>
            </div>
          </div>
        </div>
        <label class="field" v-if="authDraft.datasource==='http'">
          <span class="field-label">{{ tr('security.common.requestBody') }}</span>
          <el-input
              v-model="authDraft.httpBodyTemplate"
              type="textarea"
              :rows="8"
              placeholder='{"username":"\${username}","password":"\${password}"}'/>
        </label>
        <details class="advanced-block" v-if="authDraft.datasource==='http'">
          <summary class="advanced-summary">{{ tr('security.auth.advanced.http') }}</summary>
          <div class="advanced-fields">
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolSize') }}</span>
              <el-input-number v-model="authDraft.httpPoolSize" :min="1" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.rateLimit') }}</span>
              <el-input-number v-model="authDraft.httpRateLimitPerSecond" :min="0" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.requestTimeout') }}</span>
              <el-input-number v-model="authDraft.httpRequestTimeoutMs" :min="200" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.connectTimeout') }}</span>
              <el-input-number v-model="authDraft.httpConnectTimeoutMs" :min="200" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.pipelineCount') }}</span>
              <el-input-number v-model="authDraft.httpPipelineCount" :min="1" controls-position="right" />
            </label>
          </div>
        </details>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisHost') }}</span>
          <el-input v-model="authDraft.redisHost" placeholder="127.0.0.1"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisPort') }}</span>
          <el-input-number v-model="authDraft.redisPort" :min="1" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisPassword') }}</span>
          <el-input v-model="authDraft.redisPassword" type="password" show-password />
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisDb') }}</span>
          <el-input-number v-model="authDraft.redisDb" :min="0" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisKeyPrefix') }}</span>
          <el-input v-model="authDraft.redisKeyPrefix" placeholder="jmqx:auth"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">{{ tr('security.common.redisTimeout') }}</span>
          <el-input-number v-model="authDraft.redisTimeoutMs" :min="0" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='mysql'">
          <span class="field-label">{{ tr('security.auth.field.mysqlUrl') }}</span>
          <el-input v-model="authDraft.mysqlUrl" placeholder="jdbc:mysql://127.0.0.1:3306/jmqx"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='mysql'">
          <span class="field-label">{{ tr('security.auth.field.mysqlUser') }}</span>
          <el-input v-model="authDraft.mysqlUser" placeholder="root"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='mysql'">
          <span class="field-label">{{ tr('security.auth.field.mysqlPassword') }}</span>
          <el-input v-model="authDraft.mysqlPassword" type="password" show-password />
        </label>
        <label class="field" v-if="authDraft.datasource==='mysql'">
          <span class="field-label">{{ tr('security.auth.field.mysqlQuery') }}</span>
          <el-input v-model="authDraft.mysqlQuery" placeholder="SELECT password FROM mqtt_user WHERE username = ?"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='postgresql'">
          <span class="field-label">{{ tr('security.auth.field.postgresqlUrl') }}</span>
          <el-input v-model="authDraft.postgresqlUrl" placeholder="jdbc:postgresql://127.0.0.1:5432/jmqx"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='postgresql'">
          <span class="field-label">{{ tr('security.auth.field.postgresqlUser') }}</span>
          <el-input v-model="authDraft.postgresqlUser" placeholder="postgres"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='postgresql'">
          <span class="field-label">{{ tr('security.auth.field.postgresqlPassword') }}</span>
          <el-input v-model="authDraft.postgresqlPassword" type="password" show-password />
        </label>
        <label class="field" v-if="authDraft.datasource==='postgresql'">
          <span class="field-label">{{ tr('security.auth.field.postgresqlQuery') }}</span>
          <el-input v-model="authDraft.postgresqlQuery" placeholder="SELECT password FROM mqtt_user WHERE username = ?"/>
        </label>
        <details class="advanced-block" v-if="authDraft.datasource==='mysql'">
          <summary class="advanced-summary">{{ tr('security.auth.advanced.mysql') }}</summary>
          <div class="advanced-fields">
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolMinIdle') }}</span>
              <el-input-number v-model="authDraft.mysqlPoolMinIdle" :min="0" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolMaxSize') }}</span>
              <el-input-number v-model="authDraft.mysqlPoolMaxSize" :min="1" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolAcquireTimeout') }}</span>
              <el-input-number v-model="authDraft.mysqlPoolConnectionTimeoutMs" :min="250" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolIdleTimeout') }}</span>
              <el-input-number v-model="authDraft.mysqlPoolIdleTimeoutMs" :min="10000" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolMaxLifetime') }}</span>
              <el-input-number v-model="authDraft.mysqlPoolMaxLifetimeMs" :min="30000" controls-position="right" />
            </label>
          </div>
        </details>
        <details class="advanced-block" v-if="authDraft.datasource==='postgresql'">
          <summary class="advanced-summary">{{ tr('security.auth.advanced.postgresql') }}</summary>
          <div class="advanced-fields">
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolMinIdle') }}</span>
              <el-input-number v-model="authDraft.postgresqlPoolMinIdle" :min="0" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolMaxSize') }}</span>
              <el-input-number v-model="authDraft.postgresqlPoolMaxSize" :min="1" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolAcquireTimeout') }}</span>
              <el-input-number v-model="authDraft.postgresqlPoolConnectionTimeoutMs" :min="250" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolIdleTimeout') }}</span>
              <el-input-number v-model="authDraft.postgresqlPoolIdleTimeoutMs" :min="10000" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">{{ tr('security.auth.field.poolMaxLifetime') }}</span>
              <el-input-number v-model="authDraft.postgresqlPoolMaxLifetimeMs" :min="30000" controls-position="right" />
            </label>
          </div>
        </details>
      </div>
    </div>

    <div class="auth-footer-actions">
      <el-button @click="cancelAuthCreate">{{ tr('common.cancel') }}</el-button>
      <el-button v-if="authStep>1" @click="previousAuthStep">{{ tr('common.previous') }}</el-button>
      <el-button type="primary" v-if="authStep<3" @click="nextAuthStep">{{ tr('common.next') }}</el-button>
      <el-button type="primary" v-if="authStep===3" @click="createAuthAndSave">{{ authEditingPlugin ? tr('common.save') : tr('common.create') }}</el-button>
    </div>
  </section>
`;
