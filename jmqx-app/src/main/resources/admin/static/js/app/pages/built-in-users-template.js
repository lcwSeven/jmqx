export const builtInUsersPageTemplate = `
  <section class="panel" v-if="activeMenu==='built-in-users'">
    <div class="section-head">
      <div>
        <h2 class="title">内置数据库用户管理</h2>
        <div class="section-subtitle">管理内置数据库中的认证用户，新增和导入都会按照当前账号类型写入。</div>
      </div>
      <div class="built-in-toolbar">
        <el-button type="primary" @click="openBuiltInUserCreateDialog">添加用户</el-button>
        <el-button @click="openBuiltInUserImportDialog">导入用户</el-button>
        <el-button plain @click="setMenu('auth')">返回连接鉴权</el-button>
      </div>
    </div>

    <div class="built-in-meta">
      <el-tag effect="light" type="success">账号类型：{{ builtInAccountFieldLabel }}</el-tag>
      <el-tag effect="light">加密方式：{{ builtInUsers.passwordHashAlgorithm }}</el-tag>
      <el-tag effect="light">加盐方式：{{ builtInUsers.saltPosition }}</el-tag>
    </div>
    <div class="hint built-in-match-hint">{{ builtInUserIdMatchHint }}</div>

    <el-table v-if="builtInUsers.records.length" :data="builtInUsers.records" stripe class="ep-table">
      <el-table-column :label="builtInAccountFieldLabel" min-width="220">
        <template #default="scope">
          {{ scope.row.userId }}
        </template>
      </el-table-column>
      <el-table-column label="用户类型" width="130">
        <template #default="scope">
          <span class="built-in-status" :class="{ 'is-superuser': scope.row.superuser }">
            <span class="built-in-status-dot"></span>
            <span>{{ scope.row.superuser ? '超级用户' : '普通用户' }}</span>
          </span>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="120">
        <template #default="scope">
          <el-button size="small" type="danger" plain @click="deleteBuiltInUserRecord(scope.row.userId)">删除</el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-empty v-else description="暂无内置数据库用户。"></el-empty>

    <el-dialog v-model="builtInUserDialogs.create" title="添加内置数据库用户" width="520px" destroy-on-close append-to-body>
      <div class="built-in-dialog-body">
        <label class="field">
          <span class="field-label">{{ builtInAccountFieldLabel }}</span>
          <el-input v-model="builtInUserForm.userId" :placeholder="'请输入' + builtInAccountFieldLabel"/>
        </label>
        <label class="field">
          <span class="field-label">密码</span>
          <el-input type="password" show-password v-model="builtInUserForm.password" placeholder="请输入密码"/>
        </label>
        <label class="field">
          <span class="field-label">是否超级用户</span>
          <el-switch v-model="builtInUserForm.superuser" active-text="是" inactive-text="否"></el-switch>
        </label>
        <div class="hint built-in-superuser-hint">超级用户认证通过后可绕过 ACL 鉴权；普通用户仍会继续执行 ACL 校验。</div>
      </div>
      <template #footer>
        <el-button @click="closeBuiltInUserCreateDialog">取消</el-button>
        <el-button type="primary" @click="createBuiltInUserRecord">添加用户</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="builtInUserDialogs.import" title="导入内置数据库用户" width="620px" destroy-on-close append-to-body>
      <div class="built-in-dialog-body">
        <div class="hint">请上传 CSV 文件，支持表头：userId,password,superuser</div>
        <label class="field">
          <span class="field-label">CSV 文件</span>
          <el-upload
            class="built-in-upload"
            drag
            action=""
            :auto-upload="false"
            :limit="1"
            accept=".csv,text/csv"
            :on-change="handleBuiltInCsvChange"
            :on-remove="removeBuiltInCsvFile">
            <div class="el-upload__text">将 CSV 文件拖到此处，或 <em>点击上传</em></div>
            <template #tip>
              <div class="el-upload__tip">文件内容格式：userId,password,superuser。第三列可选，取值为 true 或 false。</div>
            </template>
          </el-upload>
        </label>
      </div>
      <template #footer>
        <el-button @click="closeBuiltInUserImportDialog">取消</el-button>
        <el-button type="primary" @click="importBuiltInUserRecords">导入用户</el-button>
      </template>
    </el-dialog>
  </section>
`;
