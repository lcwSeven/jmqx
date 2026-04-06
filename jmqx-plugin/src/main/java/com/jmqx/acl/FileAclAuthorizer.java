package com.jmqx.acl;

import com.jmqx.common.TopicMatcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class FileAclAuthorizer implements AclAuthorizer {
    private static final Logger LOG = Logger.getLogger(FileAclAuthorizer.class.getName());

    private final List<AclRule> rules;
    private final boolean defaultAllow;

    public FileAclAuthorizer(AclProperties properties) {
        this.rules = loadRules(Path.of(properties.getFilePath()));
        this.defaultAllow = properties.isDefaultAllow();
    }

    @Override
    public boolean isAllowed(AclRequest request) {
        for (AclRule rule : rules) {
            if (rule.matches(request)) {
                return rule.allow;
            }
        }
        return defaultAllow;
    }

    private List<AclRule> loadRules(Path path) {
        List<AclRule> loadedRules = new ArrayList<>();
        if (!Files.exists(path)) {
            LOG.warning("ACL file not found: " + path.toAbsolutePath());
            return loadedRules;
        }
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            for (String line : lines) {
                String trim = line.trim();
                if (trim.isEmpty() || trim.startsWith("#")) {
                    continue;
                }
                String[] parts = trim.split("\\s+");
                if (parts.length < 4) {
                    continue;
                }
                boolean allow = "allow".equalsIgnoreCase(parts[0]);
                String action = parts[1].toLowerCase(Locale.ROOT);
                String username = parts[2];
                String topicFilter = normalizeFilter(parts[3]);
                loadedRules.add(new AclRule(allow, action, username, topicFilter));
            }
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Load ACL file failed: " + path.toAbsolutePath(), e);
        }
        return loadedRules;
    }

    private static String normalizeFilter(String raw) {
        if ("*".equals(raw)) {
            return "#";
        }
        return raw;
    }

    /**
     * @author liucaiwen
     * @date 2026/4/4
     */
    private static class AclRule {
        private final boolean allow;
        private final String action;
        private final String username;
        private final String topicFilter;

        private AclRule(boolean allow, String action, String username, String topicFilter) {
            this.allow = allow;
            this.action = action;
            this.username = username;
            this.topicFilter = topicFilter;
        }

        private boolean matches(AclRequest request) {
            String reqAction = request.getAction().name().toLowerCase(Locale.ROOT);
            boolean actionMatch = "*".equals(action) || action.equals(reqAction);
            if (!actionMatch) {
                return false;
            }
            boolean usernameMatch = "*".equals(username) || username.equals(request.getUsername());
            if (!usernameMatch) {
                return false;
            }
            return TopicMatcher.matches(topicFilter, request.getTopic());
        }
    }
}
