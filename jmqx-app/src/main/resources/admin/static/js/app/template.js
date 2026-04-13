import { overviewPageTemplate } from "./pages/overview-template.js";
import { clientsPageTemplate } from "./pages/clients-template.js";
import { securityPageTemplate } from "./pages/security-template.js";
import { builtInUsersPageTemplate } from "./pages/built-in-users-template.js";
import { clusterPageTemplate } from "./pages/cluster-template.js";
import { auditPageTemplate } from "./pages/audit-template.js";

export const adminTemplate = `
      <div v-if="adminAuthRequired && !adminAuthenticated" class="admin-login-shell">
        <section class="admin-login-card panel">
          <div class="eyebrow">JMQX Admin</div>
          <h1 class="hero-title">管理后台登录</h1>
          <p class="hero-desc">请输入内嵌管理后台账号密码后，再访问配置接口和实时 Dashboard。</p>
          <div v-if="error" class="error">{{ error }}</div>
          <label class="field">
            <span class="field-label">用户名</span>
            <el-input v-model="adminLoginForm.username" placeholder="admin" @keyup.enter="loginAdminPanel"/>
          </label>
          <label class="field">
            <span class="field-label">密码</span>
            <el-input v-model="adminLoginForm.password" type="password" show-password placeholder="请输入密码" @keyup.enter="loginAdminPanel"/>
          </label>
          <div class="admin-login-actions">
            <el-button type="primary" size="large" @click="loginAdminPanel">登录后台</el-button>
          </div>
        </section>
      </div>
      <div v-else class="layout">
        <aside class="sidebar">
          <div class="logo-wrap">
            <div class="logo">JMQX Admin</div>
            <div class="logo-sub">Cluster Console</div>
            <div class="logo-role">{{ adminRoleLabel }}</div>
          </div>
          <div class="menu-title">监控</div>
          <button class="menu-item" :class="{active: activeMenu==='overview'}" @click="setMenu('overview')">集群概览</button>
          <button class="menu-item" :class="{active: activeMenu==='clients'}" @click="setMenu('clients')">客户端列表</button>
          <div class="menu-title">安全策略</div>
          <button class="menu-item" :class="{active: activeMenu==='acl'}" @click="setMenu('acl')">ACL 鉴权</button>
          <button class="menu-item" :class="{active: activeMenu==='auth'}" @click="setMenu('auth')">连接鉴权</button>
          <div class="menu-title">系统配置</div>
          <button class="menu-item" :class="{active: activeMenu==='cluster'}" @click="setMenu('cluster')">集群配置</button>
          <button class="menu-item" :class="{active: activeMenu==='audit'}" @click="setMenu('audit')">操作审计</button>
          <div class="sidebar-footer">
            <div class="sidebar-foot-label">账号</div>
            <div class="sidebar-foot-value">{{ adminSession.username || '-' }}</div>
            <div class="sidebar-foot-label">权限</div>
            <div class="sidebar-status">{{ adminRoleLabel }}</div>
            <div class="sidebar-foot-label">Cluster</div>
            <div class="sidebar-foot-value">{{ currentClusterId }}</div>
            <div class="sidebar-foot-label">Realtime</div>
            <div class="sidebar-status">{{ mqttStatus }}</div>
          </div>
        </aside>
        <main class="content">
          <section class="hero panel">
            <div>
              <div class="eyebrow">JMQX Control Surface</div>
              <h1 class="hero-title">{{ activeMenuLabel }}</h1>
              <p class="hero-desc">{{ activeMenuDescription }}</p>
            </div>
            <div class="hero-side">
              <div class="hero-account">
                <div class="hero-account-meta">
                  <span class="hero-account-name">{{ adminSession.username || 'admin' }}</span>
                  <span class="hero-account-role">{{ adminRoleLabel }}</span>
                </div>
                <el-dropdown trigger="click" @command="handleAdminMenuCommand">
                  <el-button>
                    账号设置
                  </el-button>
                  <template #dropdown>
                    <el-dropdown-menu>
                      <el-dropdown-item command="change-password">修改密码</el-dropdown-item>
                      <el-dropdown-item command="logout" divided>退出登录</el-dropdown-item>
                    </el-dropdown-menu>
                  </template>
                </el-dropdown>
              </div>
              <div class="hero-metrics">
                <div class="hero-pill">
                  <span class="hero-pill-label">Cluster</span>
                  <strong>{{ currentClusterId }}</strong>
                </div>
                <div class="hero-pill">
                  <span class="hero-pill-label">Dashboard</span>
                  <strong>{{ mqttStatus }}</strong>
                </div>
                <div class="hero-pill">
                  <span class="hero-pill-label">Nodes</span>
                  <strong>{{ totalNodes }}</strong>
                </div>
              </div>
            </div>
          </section>

          <div v-if="message" class="success">{{ message }}</div>
          <div v-if="error" class="error">{{ error }}</div>
          <el-dialog
            v-model="adminDialogs.password"
            title="修改管理后台密码"
            width="460px"
            append-to-body
            destroy-on-close
          >
            <div class="form-stack">
              <label class="field">
                <span class="field-label">当前密码</span>
                <el-input v-model="adminPasswordForm.currentPassword" type="password" show-password placeholder="请输入当前密码"/>
              </label>
              <label class="field">
                <span class="field-label">新密码</span>
                <el-input v-model="adminPasswordForm.newPassword" type="password" show-password placeholder="请输入新密码"/>
              </label>
              <label class="field">
                <span class="field-label">确认新密码</span>
                <el-input v-model="adminPasswordForm.confirmPassword" type="password" show-password placeholder="请再次输入新密码"/>
              </label>
            </div>
            <template #footer>
              <div class="dialog-actions">
                <el-button @click="closeAdminPasswordDialog">取消</el-button>
                <el-button type="primary" @click="submitAdminPasswordChange">保存密码</el-button>
              </div>
            </template>
          </el-dialog>
          ${overviewPageTemplate}
          ${clientsPageTemplate}
          ${securityPageTemplate}
          ${builtInUsersPageTemplate}
          ${clusterPageTemplate}
          ${auditPageTemplate}
        </main>
      </div>
    `;
