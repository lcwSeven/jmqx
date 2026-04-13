export const securityPageTemplate = `
  <section class="panel" v-if="activeMenu==='acl'">
    <div class="section-head">
      <div>
        <h2 class="title">ACL 鉴权配置</h2>
        <div class="section-subtitle">控制主题发布与订阅的访问链路。</div>
      </div>
      <span class="status-badge" :class="aclStatusClass">{{ aclStatusText }}</span>
    </div>
    <div class="form-grid">
      <label class="field checkbox-field">
        <input type="checkbox" v-model="securityConfig.aclEnabled"/>
        <span>启用 ACL 鉴权</span>
      </label>
      <label class="field">
        <span class="field-label">ACL 链（逗号分隔，按顺序执行）</span>
        <input :value="joinComma(securityConfig.aclChain)" @input="securityConfig.aclChain=$event.target.value"/>
      </label>
      <label class="field">
        <span class="field-label">鉴权缓存时间（毫秒）</span>
        <input type="number" min="0" v-model.number="securityConfig.cacheTtlMs"/>
      </label>
    </div>
    <div class="actions">
      <button class="btn" @click="saveAclConfig">保存 ACL 配置</button>
    </div>
  </section>

  <section class="panel auth-surface" v-if="activeMenu==='auth' && !authCreateMode">
    <div class="auth-list-toolbar">
      <div>
        <h2 class="title auth-title">客户端认证</h2>
        <div class="section-subtitle">可创建多种鉴权方式，列表顺序就是最终鉴权链执行顺序。</div>
      </div>
      <el-button type="primary" size="large" @click="openAuthCreate">+ 创建</el-button>
    </div>
    <el-table v-if="authEntries.length" :data="authEntries" stripe class="ep-table auth-ep-table">
      <el-table-column label="数据源及认证方式" min-width="260">
        <template #default="scope">
          <div class="auth-main-cell">
            <div class="auth-main-name">{{ scope.row.displayName }}</div>
            <div class="auth-main-sub">{{ scope.row.summary }}</div>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="链路顺序" width="120">
        <template #default="scope">
          第 {{ scope.row.index + 1 }} 位
        </template>
      </el-table-column>
      <el-table-column label="数据源状态" width="120">
        <template #default>
          <el-tag :type="securityConfig.authEnabled ? 'success' : 'info'" effect="light">
            {{ securityConfig.authEnabled ? '已启用' : '已停用' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="是否启用" width="120">
        <template #default>
          <el-switch v-model="securityConfig.authEnabled" @change="toggleAuthEnabled"/>
        </template>
      </el-table-column>
      <el-table-column label="操作" min-width="320">
        <template #default="scope">
          <div class="auth-actions ep-actions">
            <el-button size="small" @click="moveAuthEntryUp(scope.row.plugin)" :disabled="scope.row.index===0">上移</el-button>
            <el-button size="small" @click="moveAuthEntryDown(scope.row.plugin)" :disabled="scope.row.index===authEntries.length-1">下移</el-button>
            <el-button size="small" @click="openAuthSettings(scope.row.plugin)">设置</el-button>
            <el-button size="small" v-if="scope.row.plugin==='built_in_database'" @click="openBuiltInUserManagement">用户管理</el-button>
            <el-button size="small" type="danger" plain @click="removeAuthEntry(scope.row.plugin)">删除</el-button>
          </div>
        </template>
      </el-table-column>
    </el-table>
    <el-empty v-else description="暂无认证配置；未配置时默认允许全部连接。" class="auth-empty"></el-empty>
    <div class="hint">当前鉴权链：{{ joinComma(securityConfig.authChain) || '未配置（允许全部）' }}</div>
  </section>

  <section class="panel auth-surface" v-if="activeMenu==='auth' && authCreateMode">
    <div class="auth-create-header">
      <el-button plain @click="cancelAuthCreate">返回</el-button>
      <span class="auth-sep">|</span>
      <h2 class="title auth-title">创建认证</h2>
    </div>

    <el-steps :active="authStep" align-center finish-status="success" class="auth-steps">
      <el-step title="认证方式"></el-step>
      <el-step title="数据源"></el-step>
      <el-step title="配置参数"></el-step>
    </el-steps>

    <div v-if="authStep===1">
      <div class="auth-hint">使用客户端用户名、Client ID 与密码进行认证</div>
      <div class="auth-option-grid">
        <el-button
            v-for="item in authMethodOptions"
            :key="item.key"
            class="auth-option-card ep-option-card"
            :type="authDraft.method===item.key ? 'primary' : 'default'"
            :plain="authDraft.method!==item.key"
            @click="selectAuthMethod(item.key)">
          {{ item.label }}
        </el-button>
      </div>
    </div>

    <div v-if="authStep===2">
      <div class="auth-hint">选择存储认证数据的数据源</div>
      <div class="auth-option-grid datasource-grid">
        <el-button
            v-for="item in authDraftDatasourceOptions"
            :key="item.key"
            class="auth-option-card ep-option-card"
            :type="authDraft.datasource===item.key ? 'primary' : 'default'"
            :plain="authDraft.datasource!==item.key"
            @click="selectAuthDatasource(item.key)">
          <span class="ds-icon">{{ item.icon }}</span>{{ item.label }}
        </el-button>
      </div>
    </div>

    <div v-if="authStep===3">
      <div class="auth-hint">当前组合：{{ authMethodLabel() }} / {{ authDatasourceLabel() }}</div>
      <div class="form-grid auth-config-grid auth-config-stack">
        <div class="auth-chain-preview">
          <div class="field-label">加入后鉴权链</div>
          <div class="auth-chain-preview-value">
            {{ joinComma(authEditingPlugin ? securityConfig.authChain : [...securityConfig.authChain, mapDatasourceToPlugin(authDraft.datasource)].filter((value, index, array) => value && array.indexOf(value) === index)) || '未配置（允许全部）' }}
          </div>
          <div class="hint">列表顺序决定执行顺序，创建成功后会自动加入鉴权链末尾。</div>
        </div>
        <label class="field">
          <span class="field-label">缓存时间（毫秒）</span>
          <el-input-number v-model="authDraft.cacheTtlMs" :min="0" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='file'">
          <span class="field-label">用户文件路径</span>
          <el-input v-model="authDraft.filePath" placeholder="conf/auth-users.txt"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='built_in_database'">
          <span class="field-label">账号类型</span>
          <el-select v-model="authDraft.builtInDatabaseAccountType">
            <el-option value="username" label="username"></el-option>
            <el-option value="clientId" label="clientId"></el-option>
          </el-select>
        </label>
        <label class="field" v-if="authDraft.datasource==='built_in_database'">
          <span class="field-label">密码加密方式</span>
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
          <span class="field-label">加盐方式</span>
          <el-select v-model="authDraft.builtInDatabaseSaltPosition">
            <el-option value="disable" label="disable"></el-option>
            <el-option value="prefix" label="prefix"></el-option>
            <el-option value="suffix" label="suffix"></el-option>
          </el-select>
        </label>
        <label class="field" v-if="authDraft.datasource==='http'">
          <span class="field-label">HTTP 认证地址</span>
          <el-input v-model="authDraft.httpUrl" placeholder="http://host:port/auth/check"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='http'">
          <span class="field-label">HTTP 超时（毫秒）</span>
          <el-input-number v-model="authDraft.httpTimeoutMs" :min="0" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">Redis Host</span>
          <el-input v-model="authDraft.redisHost" placeholder="127.0.0.1"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">Redis Port</span>
          <el-input-number v-model="authDraft.redisPort" :min="1" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">Redis Password</span>
          <el-input v-model="authDraft.redisPassword" type="password" show-password />
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">Redis DB</span>
          <el-input-number v-model="authDraft.redisDb" :min="0" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">Redis Key Prefix</span>
          <el-input v-model="authDraft.redisKeyPrefix" placeholder="jmqx:auth"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='redis'">
          <span class="field-label">Redis 超时（毫秒）</span>
          <el-input-number v-model="authDraft.redisTimeoutMs" :min="0" controls-position="right" />
        </label>
        <label class="field" v-if="authDraft.datasource==='mysql'">
          <span class="field-label">MySQL 连接串</span>
          <el-input v-model="authDraft.mysqlUrl" placeholder="jdbc:mysql://127.0.0.1:3306/jmqx"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='mysql'">
          <span class="field-label">MySQL 用户名</span>
          <el-input v-model="authDraft.mysqlUser" placeholder="root"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='mysql'">
          <span class="field-label">MySQL 密码</span>
          <el-input v-model="authDraft.mysqlPassword" type="password" show-password />
        </label>
        <label class="field" v-if="authDraft.datasource==='mysql'">
          <span class="field-label">MySQL 认证 SQL</span>
          <el-input v-model="authDraft.mysqlQuery" placeholder="SELECT password FROM mqtt_user WHERE username = ?"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='postgresql'">
          <span class="field-label">PostgreSQL 连接串</span>
          <el-input v-model="authDraft.postgresqlUrl" placeholder="jdbc:postgresql://127.0.0.1:5432/jmqx"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='postgresql'">
          <span class="field-label">PostgreSQL 用户名</span>
          <el-input v-model="authDraft.postgresqlUser" placeholder="postgres"/>
        </label>
        <label class="field" v-if="authDraft.datasource==='postgresql'">
          <span class="field-label">PostgreSQL 密码</span>
          <el-input v-model="authDraft.postgresqlPassword" type="password" show-password />
        </label>
        <label class="field" v-if="authDraft.datasource==='postgresql'">
          <span class="field-label">PostgreSQL 认证 SQL</span>
          <el-input v-model="authDraft.postgresqlQuery" placeholder="SELECT password FROM mqtt_user WHERE username = ?"/>
        </label>
        <details class="advanced-block" v-if="authDraft.datasource==='mysql'">
          <summary class="advanced-summary">高级配置：MySQL 连接池</summary>
          <div class="advanced-fields">
            <label class="field">
              <span class="field-label">连接池最小空闲连接</span>
              <el-input-number v-model="authDraft.mysqlPoolMinIdle" :min="0" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">连接池最大连接数</span>
              <el-input-number v-model="authDraft.mysqlPoolMaxSize" :min="1" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">获取连接超时（毫秒）</span>
              <el-input-number v-model="authDraft.mysqlPoolConnectionTimeoutMs" :min="250" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">空闲超时（毫秒）</span>
              <el-input-number v-model="authDraft.mysqlPoolIdleTimeoutMs" :min="10000" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">连接最大生命周期（毫秒）</span>
              <el-input-number v-model="authDraft.mysqlPoolMaxLifetimeMs" :min="30000" controls-position="right" />
            </label>
          </div>
        </details>
        <details class="advanced-block" v-if="authDraft.datasource==='postgresql'">
          <summary class="advanced-summary">高级配置：PostgreSQL 连接池</summary>
          <div class="advanced-fields">
            <label class="field">
              <span class="field-label">连接池最小空闲连接</span>
              <el-input-number v-model="authDraft.postgresqlPoolMinIdle" :min="0" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">连接池最大连接数</span>
              <el-input-number v-model="authDraft.postgresqlPoolMaxSize" :min="1" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">获取连接超时（毫秒）</span>
              <el-input-number v-model="authDraft.postgresqlPoolConnectionTimeoutMs" :min="250" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">空闲超时（毫秒）</span>
              <el-input-number v-model="authDraft.postgresqlPoolIdleTimeoutMs" :min="10000" controls-position="right" />
            </label>
            <label class="field">
              <span class="field-label">连接最大生命周期（毫秒）</span>
              <el-input-number v-model="authDraft.postgresqlPoolMaxLifetimeMs" :min="30000" controls-position="right" />
            </label>
          </div>
        </details>
      </div>
    </div>

    <div class="auth-footer-actions">
      <el-button @click="cancelAuthCreate">取消</el-button>
      <el-button v-if="authStep>1" @click="previousAuthStep">上一步</el-button>
      <el-button type="primary" v-if="authStep<3" @click="nextAuthStep">下一步</el-button>
      <el-button type="primary" v-if="authStep===3" @click="createAuthAndSave">{{ authEditingPlugin ? '保存' : '创建' }}</el-button>
    </div>
  </section>
`;
