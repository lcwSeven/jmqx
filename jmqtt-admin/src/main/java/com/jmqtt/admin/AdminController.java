package com.jmqtt.admin;

import com.jmqtt.session.ClientSession;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.springframework.http.HttpStatus.NOT_FOUND;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
@RestController
@RequestMapping("/api/admin")
@CrossOrigin(origins = "*")
public class AdminController {
    private final AdminBackendState state;

    public AdminController(AdminBackendState state) {
        this.state = state;
    }

    @GetMapping("/status")
    public AdminStatusResponse status() {
        return buildStatus();
    }

    @GetMapping("/clients")
    public List<AdminClientResponse> clients(
        @RequestParam(value = "clientId", required = false) String clientId,
        @RequestParam(value = "username", required = false) String username
    ) {
        String clientIdQuery = normalize(clientId);
        String usernameQuery = normalize(username);

        return state.getSessionRegistry().list().stream()
            .filter(session -> containsIgnoreCase(session.clientId(), clientIdQuery))
            .filter(session -> containsIgnoreCase(session.username(), usernameQuery))
            .sorted(Comparator.comparing(ClientSession::connectedAt).reversed())
            .map(this::toClientResponse)
            .toList();
    }

    @GetMapping("/clients/{clientId}")
    public AdminClientDetailResponse clientDetail(@PathVariable("clientId") String clientId) {
        ClientSession session = state.getSessionRegistry().get(clientId)
            .orElseThrow(() -> new ResponseStatusException(NOT_FOUND, "client not found: " + clientId));

        AdminClientDetailResponse detail = new AdminClientDetailResponse();
        detail.setClientId(session.clientId());
        detail.setUsername(session.username());
        detail.setConnectionType(session.connectionType());
        detail.setServiceNodeIp(session.serviceNodeIp());
        detail.setKeepAliveSeconds(session.keepAliveSeconds());
        detail.setOnlineAtEpochMillis(session.connectedAt().toEpochMilli());
        detail.setSubscriptions(toSubscriptionList(state.getSubscriptionRegistry().findSubscriptions(clientId)));
        return detail;
    }

    @PostMapping("/config")
    public AdminStatusResponse updateConfig(@RequestBody AdminConfigUpdateRequest request) {
        state.getRuntimeConfigService().update(
            request.getAuthType(),
            request.getAuthCacheMillis(),
            request.getAclType(),
            request.getAclCacheMillis()
        );
        return buildStatus();
    }

    private AdminStatusResponse buildStatus() {
        AdminStatusResponse resp = new AdminStatusResponse();
        resp.setConnections(state.getConnectionMetrics().getActiveConnections());
        resp.setAuthType(state.getRuntimeConfigService().getAuthType());
        resp.setAuthCacheMillis(state.getRuntimeConfigService().getAuthCacheMillis());
        resp.setAclType(state.getRuntimeConfigService().getAclType());
        resp.setAclCacheMillis(state.getRuntimeConfigService().getAclCacheMillis());
        return resp;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    private static boolean containsIgnoreCase(String value, String query) {
        if (query == null) {
            return true;
        }
        if (value == null) {
            return false;
        }
        return value.toLowerCase().contains(query.toLowerCase());
    }

    private AdminClientResponse toClientResponse(ClientSession session) {
        AdminClientResponse resp = new AdminClientResponse();
        resp.setClientId(session.clientId());
        resp.setUsername(session.username());
        resp.setConnectionType(session.connectionType());
        resp.setServiceNodeIp(session.serviceNodeIp());
        resp.setKeepAliveSeconds(session.keepAliveSeconds());
        resp.setOnlineAtEpochMillis(session.connectedAt().toEpochMilli());
        return resp;
    }

    private List<AdminClientSubscriptionResponse> toSubscriptionList(Map<String, Integer> subscriptions) {
        return subscriptions.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> {
                AdminClientSubscriptionResponse item = new AdminClientSubscriptionResponse();
                item.setTopic(entry.getKey());
                item.setQos(entry.getValue() == null ? 0 : entry.getValue());
                return item;
            })
            .toList();
    }
}
