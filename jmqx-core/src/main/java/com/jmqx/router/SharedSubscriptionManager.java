package com.jmqx.router;

import com.jmqx.session.ClientSession;
import com.jmqx.session.SessionRegistry;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * 共享订阅管理器。
 *
 * 功能：
 * 1. 限制每个共享组的最大订阅成员数
 * 2. 轮询负载均衡并跳过 inactive session
 * 3. 识别慢消费者并从共享组中剔除
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public class SharedSubscriptionManager {
    private static final Logger LOG = Logger.getLogger(SharedSubscriptionManager.class.getName());

    private volatile int maxSubscribersPerGroup;
    private volatile int slowConsumerStrikeThreshold;
    private final ConcurrentMap<String, SharedGroupState> groupStates = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> groupsByClient = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicInteger> slowConsumerStrikes = new ConcurrentHashMap<>();

    public SharedSubscriptionManager() {
        this(1_000, 3);
    }

    public SharedSubscriptionManager(int maxSubscribersPerGroup, int slowConsumerStrikeThreshold) {
        this.maxSubscribersPerGroup = Math.max(maxSubscribersPerGroup, 1);
        this.slowConsumerStrikeThreshold = Math.max(slowConsumerStrikeThreshold, 1);
    }

    /**
     * 动态更新共享订阅限流参数。
     */
    public void reconfigure(int maxSubscribersPerGroup, int slowConsumerStrikeThreshold) {
        this.maxSubscribersPerGroup = Math.max(maxSubscribersPerGroup, 1);
        this.slowConsumerStrikeThreshold = Math.max(slowConsumerStrikeThreshold, 1);
    }

    /**
     * 注册共享组成员。
     *
     * @return true 表示注册成功；false 表示达到容量限制
     */
    public boolean register(String group, String clientId) {
        if (group == null || group.isBlank() || clientId == null || clientId.isBlank()) {
            return true;
        }

        SharedGroupState state = groupStates.computeIfAbsent(group, ignored -> new SharedGroupState());
        synchronized (state) {
            if (state.members.contains(clientId)) {
                return true;
            }
            if (state.members.size() >= maxSubscribersPerGroup) {
                LOG.warning("[SHARED] group is full, reject subscribe group=" + group
                    + ", clientId=" + clientId + ", limit=" + maxSubscribersPerGroup);
                return false;
            }
            state.members.add(clientId);
        }

        groupsByClient.computeIfAbsent(clientId, ignored -> ConcurrentHashMap.newKeySet()).add(group);
        return true;
    }

    public void unregister(String group, String clientId) {
        if (group == null || group.isBlank() || clientId == null || clientId.isBlank()) {
            return;
        }
        SharedGroupState state = groupStates.get(group);
        if (state == null) {
            return;
        }
        synchronized (state) {
            state.members.remove(clientId);
            if (state.members.isEmpty()) {
                groupStates.remove(group, state);
            }
        }
        Set<String> groups = groupsByClient.get(clientId);
        if (groups != null) {
            groups.remove(group);
            if (groups.isEmpty()) {
                groupsByClient.remove(clientId, groups);
            }
        }
    }

    public void removeClient(String clientId) {
        if (clientId == null || clientId.isBlank()) {
            return;
        }
        Set<String> groups = groupsByClient.remove(clientId);
        if (groups != null) {
            for (String group : groups) {
                unregister(group, clientId);
            }
        }
        slowConsumerStrikes.remove(clientId);
    }

    /**
     * 选择共享组内一个可用订阅者。
     * 会跳过 inactive session，并对慢消费者做剔除。
     */
    public String selectSubscriber(String group, Set<String> candidates, SessionRegistry sessionRegistry) {
        if (group == null || group.isBlank() || candidates == null || candidates.isEmpty()) {
            return null;
        }

        SharedGroupState state = groupStates.get(group);
        if (state == null) {
            return null;
        }

        List<String> snapshot;
        int start;
        synchronized (state) {
            if (state.members.isEmpty()) {
                return null;
            }
            snapshot = new ArrayList<>(state.members);
            start = Math.floorMod(state.nextIndex.getAndIncrement(), snapshot.size());
        }

        for (int i = 0; i < snapshot.size(); i++) {
            String clientId = snapshot.get((start + i) % snapshot.size());
            if (!candidates.contains(clientId)) {
                continue;
            }
            ClientSession session = sessionRegistry.get(clientId).orElse(null);
            if (session == null) {
                continue;
            }
            Channel channel = session.channel();
            if (channel == null || !channel.isActive()) {
                continue;
            }
            if (!channel.isWritable()) {
                int strikes = slowConsumerStrikes
                    .computeIfAbsent(clientId, ignored -> new AtomicInteger(0))
                    .incrementAndGet();
                if (strikes >= slowConsumerStrikeThreshold) {
                    unregister(group, clientId);
                    try {
                        channel.close();
                    } catch (Exception ignored) {
                    }
                    LOG.warning("[SHARED] evict slow consumer group=" + group + ", clientId=" + clientId
                        + ", strikes=" + strikes);
                }
                continue;
            }
            slowConsumerStrikes.remove(clientId);
            return clientId;
        }
        return null;
    }

    public List<String> listGroupMembers(String group) {
        SharedGroupState state = groupStates.get(group);
        if (state == null) {
            return Collections.emptyList();
        }
        return List.copyOf(state.members);
    }

    private static final class SharedGroupState {
        private final CopyOnWriteArrayList<String> members = new CopyOnWriteArrayList<>();
        private final AtomicInteger nextIndex = new AtomicInteger(0);
    }
}
