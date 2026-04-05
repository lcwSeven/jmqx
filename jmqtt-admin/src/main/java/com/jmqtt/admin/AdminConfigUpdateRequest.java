package com.jmqtt.admin;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
public class AdminConfigUpdateRequest {
    private String authType;
    private Integer authCacheMillis;
    private String aclType;
    private Integer aclCacheMillis;

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    public Integer getAuthCacheMillis() {
        return authCacheMillis;
    }

    public void setAuthCacheMillis(Integer authCacheMillis) {
        this.authCacheMillis = authCacheMillis;
    }

    public String getAclType() {
        return aclType;
    }

    public void setAclType(String aclType) {
        this.aclType = aclType;
    }

    public Integer getAclCacheMillis() {
        return aclCacheMillis;
    }

    public void setAclCacheMillis(Integer aclCacheMillis) {
        this.aclCacheMillis = aclCacheMillis;
    }
}
