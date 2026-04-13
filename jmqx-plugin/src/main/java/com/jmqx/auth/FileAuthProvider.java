package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class FileAuthProvider implements AuthProvider {
    private static final Logger LOG = Logger.getLogger(FileAuthProvider.class.getName());

    private final Map<String, String> userPasswordMap;

    public FileAuthProvider(AuthProperties properties) {
        this.userPasswordMap = loadUsers(Path.of(properties.getFilePath()));
    }

    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        String username = request.getUsername();
        if (username == null || username.isBlank()) {
            return AuthResult.deny();
        }
        String expected = userPasswordMap.get(username);
        if (expected == null) {
            return AuthResult.notFound();
        }
        return expected.equals(request.getPassword()) ? AuthResult.allow() : AuthResult.deny();
    }

    private Map<String, String> loadUsers(Path path) {
        Map<String, String> map = new HashMap<>();
        if (!Files.exists(path)) {
            LOG.warning("Auth user file not found: " + path.toAbsolutePath());
            return map;
        }
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            for (String line : lines) {
                String trim = line.trim();
                if (trim.isEmpty() || trim.startsWith("#")) {
                    continue;
                }
                int idx = trim.indexOf(':');
                if (idx <= 0) {
                    continue;
                }
                String username = trim.substring(0, idx).trim();
                String password = trim.substring(idx + 1).trim();
                map.put(username, password);
            }
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Load auth file failed: " + path.toAbsolutePath(), e);
        }
        return map;
    }
}
