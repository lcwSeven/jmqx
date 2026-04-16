export const clusterPageTemplate = `
  <section class="panel" v-if="activeMenu==='cluster'">
    <div class="section-head">
      <div>
        <h2 class="title">集群配置</h2>
        <div class="section-subtitle">保存集群部署参数。修改后需要重启或重部署节点才会生效。</div>
      </div>
    </div>
    <div class="hint">以下配置不会在运行中热更新，请仅在准备重启或重部署时修改。</div>
    <div class="hint">Core 节点（每行一个 host:port）</div>
    <textarea :value="joinLine(clusterConfig.coreNodes)" @input="clusterConfig.coreNodes=$event.target.value"></textarea>
    <div class="hint">Replicant 节点（每行一个 host:port）</div>
    <textarea :value="joinLine(clusterConfig.replicantNodes)" @input="clusterConfig.replicantNodes=$event.target.value"></textarea>
    <div class="toolbar">
      <label><input type="checkbox" v-model="clusterConfig.coreAcceptClientConnections"/> Core 可接入客户端</label>
    </div>
    <div class="hint">共享订阅每组最大成员数</div>
    <input type="number" min="1" v-model.number="clusterConfig.sharedSubscriptionMaxMembersPerGroup"/>
    <div style="margin-top: 10px">
      <button class="btn" @click="saveClusterConfig">保存部署配置</button>
    </div>
  </section>
`;
