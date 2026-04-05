package com.jmqtt.admin;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
}
