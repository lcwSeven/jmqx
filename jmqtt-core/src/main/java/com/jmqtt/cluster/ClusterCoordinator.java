package com.jmqtt.cluster;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class ClusterCoordinator implements ClusterReplicator {
    private static final Logger LOG = Logger.getLogger(ClusterCoordinator.class.getName());

    private final ClusterProperties properties;
    private final ClusterMessageBus messageBus;
    private final BrokerClusterReceiver brokerClusterReceiver;

    public ClusterCoordinator(
        ClusterProperties properties,
        ClusterMessageBus messageBus,
        BrokerClusterReceiver brokerClusterReceiver
    ) {
        this.properties = properties;
        this.messageBus = messageBus;
        this.brokerClusterReceiver = brokerClusterReceiver;
    }

    public void start() {
        if (!properties.isEnabled()) {
            return;
        }
        try {
            messageBus.registerListener(message -> {
                if (properties.getNodeId().equals(message.getSourceNodeId())) {
                    return;
                }
                brokerClusterReceiver.onClusterPublish(message);
            });
            messageBus.start();
            LOG.info(() -> "[CLUSTER] started nodeId=" + properties.getNodeId()
                + ", role=" + properties.getRole() + ", busType=" + properties.getBusType());
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Cluster start failed: " + e.getMessage(), e);
        }
    }

    public void stop() {
        if (!properties.isEnabled()) {
            return;
        }
        messageBus.stop();
    }

    @Override
    public void replicatePublish(String topic, byte[] payload, int qos, boolean retain) {
        if (!properties.isEnabled()) {
            return;
        }
        messageBus.publish(new ClusterPublishMessage(properties.getNodeId(), topic, payload, qos, retain));
    }
}
