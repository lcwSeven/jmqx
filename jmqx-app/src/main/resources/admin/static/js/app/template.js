import { overviewPageTemplate } from "./pages/overview-template.js";
import { clientsPageTemplate } from "./pages/clients-template.js";
import { blacklistPageTemplate } from "./pages/blacklist-template.js";
import { securityPageTemplate } from "./pages/security-template.js";
import { builtInUsersPageTemplate } from "./pages/built-in-users-template.js";
import { bridgePageTemplate } from "./pages/bridge-template.js";

export const adminTemplate = `
      <div v-if="adminAuthRequired && !adminAuthenticated" class="admin-login-shell">
        <section class="admin-login-card panel">
          <div class="login-actions">
            <el-select :model-value="locale" size="small" class="locale-switch" @change="switchLocale">
              <el-option
                v-for="localeOption in availableLocales"
                :key="localeOption.value"
                :value="localeOption.value"
                :label="tr(localeOption.labelKey)"
              ></el-option>
            </el-select>
          </div>
          <div class="login-brand">
            <img class="login-brand-logo" src="/admin/image/logo.png" alt="JMQX logo"/>
          </div>
          <div class="eyebrow">{{ tr('app.brand.console') }}</div>
          <h1 class="hero-title">{{ tr('login.title') }}</h1>
          <p class="hero-desc">{{ tr('login.description') }}</p>
          <div v-if="error" class="error">{{ error }}</div>
          <label class="field">
            <span class="field-label">{{ tr('login.username') }}</span>
            <el-input v-model="adminLoginForm.username" :placeholder="tr('login.username')" @keyup.enter="loginAdminPanel"/>
          </label>
          <label class="field">
            <span class="field-label">{{ tr('login.password') }}</span>
            <el-input v-model="adminLoginForm.password" type="password" show-password :placeholder="tr('login.password.placeholder')" @keyup.enter="loginAdminPanel"/>
          </label>
          <div class="admin-login-actions">
            <el-button type="primary" size="large" @click="loginAdminPanel">{{ tr('login.submit') }}</el-button>
          </div>
        </section>
      </div>
      <div v-else class="layout" :class="{ 'layout-collapsed': sidebarCollapsed }">
        <aside class="sidebar">
          <div class="sidebar-head">
            <button class="sidebar-toggle" type="button" @click="toggleSidebar" :title="sidebarCollapsed ? tr('sidebar.expand') : tr('sidebar.collapse')">
              {{ sidebarCollapsed ? '›' : '‹' }}
            </button>
          </div>
          <div v-if="!sidebarCollapsed" class="sidebar-body">
            <div class="logo-wrap">
              <div class="logo">{{ tr('app.brand.console') }}</div>
              <div class="logo-sub">{{ tr('app.brand.subtitle') }}</div>
              <div class="logo-role">{{ adminRoleLabel }}</div>
            </div>
            <div class="menu-title">{{ tr('sidebar.section.observability') }}</div>
            <button class="menu-item" :class="{active: activeMenu==='overview'}" @click="setMenu('overview')">{{ tr('menu.overview') }}</button>
            <button class="menu-item" :class="{active: activeMenu==='clients'}" @click="setMenu('clients')">{{ tr('menu.clients') }}</button>
            <div class="menu-title">{{ tr('sidebar.section.security') }}</div>
            <button class="menu-item" :class="{active: activeMenu==='blacklist'}" @click="setMenu('blacklist')">{{ tr('menu.blacklist') }}</button>
            <button class="menu-item" :class="{active: activeMenu==='acl'}" @click="setMenu('acl')">{{ tr('menu.acl') }}</button>
            <button class="menu-item" :class="{active: activeMenu==='auth'}" @click="setMenu('auth')">{{ tr('menu.auth') }}</button>
            <div class="menu-title">{{ tr('sidebar.section.system') }}</div>
            <button class="menu-item" :class="{active: activeMenu==='bridge'}" @click="setMenu('bridge')">{{ tr('menu.bridge') }}</button>
          </div>
        </aside>
        <main class="content">
          <section class="page-header panel">
            <div class="page-header-main">
              <div class="eyebrow">{{ tr('app.brand.console') }}</div>
              <h1 class="page-title">{{ activeMenuLabel }}</h1>
              <p class="page-desc">{{ activeMenuDescription }}</p>
            </div>
            <div class="page-header-side">
              <div class="page-toolbar page-toolbar-compact">
                <div class="cluster-switch cluster-switch-inline">
                  <span class="toolbar-label">{{ tr('hero.currentCluster') }}</span>
                  <el-select
                    :model-value="currentClusterId"
                    :placeholder="tr('cluster.select.placeholder')"
                    @change="switchCluster"
                  >
                    <el-option
                      v-for="cluster in clusters"
                      :key="cluster.clusterId"
                      :label="cluster.displayName ? cluster.displayName + ' (' + cluster.clusterId + ')' : cluster.clusterId"
                      :value="cluster.clusterId"
                    />
                  </el-select>
                </div>
                <el-dropdown trigger="click" @command="handleLocaleCommand">
                  <el-button class="toolbar-icon-button" :title="tr('toolbar.language')">
                    <span class="toolbar-icon-glyph">🌐</span>
                  </el-button>
                  <template #dropdown>
                    <el-dropdown-menu>
                      <el-dropdown-item
                        v-for="localeOption in availableLocales"
                        :key="localeOption.value"
                        :command="localeOption.value"
                      >
                        {{ tr(localeOption.labelKey) }}
                      </el-dropdown-item>
                    </el-dropdown-menu>
                  </template>
                </el-dropdown>
                <el-dropdown trigger="click" @command="handleAdminMenuCommand">
                  <el-button class="toolbar-account-button">
                    {{ tr('account.settings') }}
                  </el-button>
                  <template #dropdown>
                  <el-dropdown-menu>
                    <el-dropdown-item command="change-password">{{ tr('account.changePassword') }}</el-dropdown-item>
                    <el-dropdown-item command="logout" divided>{{ tr('account.signOut') }}</el-dropdown-item>
                  </el-dropdown-menu>
                </template>
                </el-dropdown>
              </div>
            </div>
          </section>

          <div v-if="message" class="success">{{ message }}</div>
          <div v-if="error" class="error">{{ error }}</div>
          <el-dialog
            v-model="adminDialogs.password"
            :title="tr('dialog.password.title')"
            width="460px"
            append-to-body
            destroy-on-close
          >
            <div class="form-stack">
              <label class="field">
                <span class="field-label">{{ tr('dialog.password.current') }}</span>
                <el-input v-model="adminPasswordForm.currentPassword" type="password" show-password :placeholder="tr('dialog.password.current.placeholder')"/>
              </label>
              <label class="field">
                <span class="field-label">{{ tr('dialog.password.new') }}</span>
                <el-input v-model="adminPasswordForm.newPassword" type="password" show-password :placeholder="tr('dialog.password.new.placeholder')"/>
              </label>
              <label class="field">
                <span class="field-label">{{ tr('dialog.password.confirm') }}</span>
                <el-input v-model="adminPasswordForm.confirmPassword" type="password" show-password :placeholder="tr('dialog.password.confirm.placeholder')"/>
              </label>
            </div>
            <template #footer>
              <div class="dialog-actions">
                <el-button @click="closeAdminPasswordDialog">{{ tr('common.cancel') }}</el-button>
                <el-button type="primary" @click="submitAdminPasswordChange">{{ tr('dialog.password.save') }}</el-button>
              </div>
            </template>
          </el-dialog>
          ${overviewPageTemplate}
          ${clientsPageTemplate}
          ${blacklistPageTemplate}
          ${securityPageTemplate}
          ${builtInUsersPageTemplate}
          ${bridgePageTemplate}
        </main>
      </div>
    `;
