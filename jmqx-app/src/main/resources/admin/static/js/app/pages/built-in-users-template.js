export const builtInUsersPageTemplate = `
  <section class="panel" v-if="activeMenu==='built-in-users'">
    <div class="section-head">
      <div>
        <h2 class="title">{{ tr('builtInUsers.title') }}</h2>
        <div class="section-subtitle">{{ tr('builtInUsers.subtitle') }}</div>
      </div>
      <div class="built-in-toolbar">
        <el-button type="primary" @click="openBuiltInUserCreateDialog">{{ tr('builtInUsers.action.add') }}</el-button>
        <el-button @click="openBuiltInUserImportDialog">{{ tr('builtInUsers.action.import') }}</el-button>
        <el-button plain @click="setMenu('auth')">{{ tr('builtInUsers.action.back') }}</el-button>
      </div>
    </div>

    <div class="built-in-meta">
      <el-tag effect="light" type="success">{{ tr('builtInUsers.meta.accountType', { value: builtInAccountFieldLabel }) }}</el-tag>
      <el-tag effect="light">{{ tr('builtInUsers.meta.hashAlgorithm', { value: builtInUsers.passwordHashAlgorithm }) }}</el-tag>
      <el-tag effect="light">{{ tr('builtInUsers.meta.saltPosition', { value: builtInUsers.saltPosition }) }}</el-tag>
    </div>
    <div class="hint built-in-match-hint">{{ builtInUserIdMatchHint }}</div>

    <el-table v-if="builtInUsers.records.length" :data="builtInUsers.records" stripe class="ep-table">
      <el-table-column :label="builtInAccountFieldLabel" min-width="220">
        <template #default="scope">
          {{ scope.row.userId }}
        </template>
      </el-table-column>
      <el-table-column :label="tr('builtInUsers.table.userType')" width="130">
        <template #default="scope">
          <span class="built-in-status" :class="{ 'is-superuser': scope.row.superuser }">
            <span class="built-in-status-dot"></span>
            <span>{{ scope.row.superuser ? tr('builtInUsers.userType.superuser') : tr('builtInUsers.userType.regular') }}</span>
          </span>
        </template>
      </el-table-column>
      <el-table-column :label="tr('builtInUsers.table.actions')" width="120">
        <template #default="scope">
          <el-button size="small" type="danger" plain @click="deleteBuiltInUserRecord(scope.row.userId)">{{ tr('security.common.delete') }}</el-button>
        </template>
      </el-table-column>
    </el-table>
    <el-empty v-else :description="tr('builtInUsers.empty')"></el-empty>

    <el-dialog v-model="builtInUserDialogs.create" :title="tr('builtInUsers.dialog.createTitle')" width="520px" destroy-on-close append-to-body>
      <div class="built-in-dialog-body">
        <label class="field">
          <span class="field-label">{{ builtInAccountFieldLabel }}</span>
          <el-input v-model="builtInUserForm.userId" :placeholder="tr('builtInUsers.field.enter', { field: builtInAccountFieldLabel })"/>
        </label>
        <label class="field">
          <span class="field-label">{{ tr('login.password') }}</span>
          <el-input type="password" show-password v-model="builtInUserForm.password" :placeholder="tr('login.password.placeholder')"/>
        </label>
        <label class="field">
          <span class="field-label">{{ tr('builtInUsers.field.superuser') }}</span>
          <el-switch v-model="builtInUserForm.superuser" :active-text="tr('common.yes')" :inactive-text="tr('common.no')"></el-switch>
        </label>
        <div class="hint built-in-superuser-hint">{{ tr('builtInUsers.hint.superuser') }}</div>
      </div>
      <template #footer>
        <el-button @click="closeBuiltInUserCreateDialog">{{ tr('common.cancel') }}</el-button>
        <el-button type="primary" @click="createBuiltInUserRecord">{{ tr('builtInUsers.action.add') }}</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="builtInUserDialogs.import" :title="tr('builtInUsers.dialog.importTitle')" width="620px" destroy-on-close append-to-body>
      <div class="built-in-dialog-body">
        <div class="hint">{{ tr('builtInUsers.upload.title') }}</div>
        <label class="field">
          <span class="field-label">{{ tr('builtInUsers.field.csv') }}</span>
          <el-upload
            class="built-in-upload"
            drag
            action=""
            :auto-upload="false"
            :limit="1"
            accept=".csv,text/csv"
            :on-change="handleBuiltInCsvChange"
            :on-remove="removeBuiltInCsvFile">
            <div class="el-upload__text">{{ tr('builtInUsers.upload.drag') }}<em>{{ tr('builtInUsers.upload.click') }}</em></div>
            <template #tip>
              <div class="el-upload__tip">{{ tr('builtInUsers.upload.tip') }}</div>
            </template>
          </el-upload>
        </label>
      </div>
      <template #footer>
        <el-button @click="closeBuiltInUserImportDialog">{{ tr('common.cancel') }}</el-button>
        <el-button type="primary" @click="importBuiltInUserRecords">{{ tr('builtInUsers.action.import') }}</el-button>
      </template>
    </el-dialog>
  </section>
`;
