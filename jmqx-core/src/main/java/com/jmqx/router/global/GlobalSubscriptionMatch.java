package com.jmqx.router.global;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Global route match result.
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public record GlobalSubscriptionMatch(Set<String> normalNodes,
                                      Map<String, Set<String>> sharedGroupToNodes) {

    public GlobalSubscriptionMatch(Set<String> normalNodes, Map<String, Set<String>> sharedGroupToNodes) {
        this.normalNodes = normalNodes == null ? Collections.emptySet() : normalNodes;
        this.sharedGroupToNodes = sharedGroupToNodes == null ? Collections.emptyMap() : sharedGroupToNodes;
    }
}
