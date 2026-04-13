import { overviewPageTemplate } from "./pages/overview-template.js";
import { clientsPageTemplate } from "./pages/clients-template.js";
import { securityPageTemplate } from "./pages/security-template.js";
import { builtInUsersPageTemplate } from "./pages/built-in-users-template.js";
import { clusterPageTemplate } from "./pages/cluster-template.js";
import { auditPageTemplate } from "./pages/audit-template.js";

export const adminTemplate = `
      <div class="layout">
        <aside class="sidebar">
          <div class="logo-wrap">
            <div class="logo">JMQX Admin</div>
            <div class="logo-sub">Cluster Console</div>
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
          </section>

          <div v-if="message" class="success">{{ message }}</div>
          <div v-if="error" class="error">{{ error }}</div>
          ${overviewPageTemplate}
          ${clientsPageTemplate}
          ${securityPageTemplate}
          ${builtInUsersPageTemplate}
          ${clusterPageTemplate}
          ${auditPageTemplate}
        </main>
      </div>
    `;
