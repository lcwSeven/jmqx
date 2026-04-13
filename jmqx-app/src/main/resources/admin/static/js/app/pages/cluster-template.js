export const clusterPageTemplate = `
  <section class="panel" v-if="activeMenu==='cluster'">
    <div class="section-head">
      <div>
        <h2 class="title">集群配置</h2>
        <div class="section-subtitle">调整节点列表、接入策略和共享订阅容量。</div>
      </div>
    </div>
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
      <button class="btn" @click="saveClusterConfig">保存</button>
    </div>
  </section>
`;
