package com.jmqx.acl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class RedisAclAuthorizer implements AclAuthorizer {
    private static final Logger LOG = Logger.getLogger(RedisAclAuthorizer.class.getName());

    private final String host;
    private final int port;
    private final String password;
    private final int db;
    private final String keyPrefix;
    private final int timeoutMs;
    private final boolean defaultAllow;

    public RedisAclAuthorizer(AclProperties properties) {
        this.host = properties.getRedisHost();
        this.port = properties.getRedisPort();
        this.password = properties.getRedisPassword();
        this.db = properties.getRedisDb();
        this.keyPrefix = properties.getRedisKeyPrefix();
        this.timeoutMs = Math.max(properties.getRedisTimeoutMs(), 200);
        this.defaultAllow = properties.isDefaultAllow();
    }

    @Override
    public boolean isAllowed(AclRequest request) {
        List<String> keys = buildKeys(request);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            socket.setSoTimeout(timeoutMs);
            BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
            BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());

            if (password != null && !password.isBlank()) {
                writeCommand(out, "AUTH", password);
                readReply(in);
            }
            if (db > 0) {
                writeCommand(out, "SELECT", Integer.toString(db));
                readReply(in);
            }

            for (String key : keys) {
                writeCommand(out, "GET", key);
                String value = readReply(in);
                if (value == null) {
                    continue;
                }
                String normalized = value.trim().toLowerCase(Locale.ROOT);
                if ("allow".equals(normalized) || "true".equals(normalized) || "1".equals(normalized)) {
                    return true;
                }
                if ("deny".equals(normalized) || "false".equals(normalized) || "0".equals(normalized)) {
                    return false;
                }
            }
            return defaultAllow;
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Redis ACL request failed: " + e.getMessage(), e);
            return defaultAllow;
        }
    }

    private List<String> buildKeys(AclRequest request) {
        String action = request.getAction().name().toLowerCase(Locale.ROOT);
        String username = safe(request.getUsername());
        String topic = safe(request.getTopic());
        List<String> keys = new ArrayList<>();
        keys.add(keyPrefix + ":" + action + ":" + username + ":" + topic);
        keys.add(keyPrefix + ":" + action + ":" + username + ":*");
        keys.add(keyPrefix + ":" + action + ":*:" + topic);
        keys.add(keyPrefix + ":" + action + ":*:*");
        keys.add(keyPrefix + ":*:" + username + ":" + topic);
        keys.add(keyPrefix + ":*:*:*");
        return keys;
    }

    private static String safe(String value) {
        return value == null || value.isBlank() ? "anonymous" : value;
    }

    private static void writeCommand(BufferedOutputStream out, String... args) throws IOException {
        out.write(("*" + args.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String arg : args) {
            byte[] bytes = arg.getBytes(StandardCharsets.UTF_8);
            out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            out.write(bytes);
            out.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        out.flush();
    }

    private static String readReply(BufferedInputStream in) throws IOException {
        int prefix = in.read();
        if (prefix == -1) {
            throw new IOException("Redis closed connection");
        }
        return switch (prefix) {
            case '+' -> readLine(in);
            case '-' -> throw new IOException("Redis error: " + readLine(in));
            case ':' -> readLine(in);
            case '$' -> {
                int len = Integer.parseInt(readLine(in));
                if (len < 0) {
                    yield null;
                }
                byte[] data = in.readNBytes(len);
                in.read();
                in.read();
                yield new String(data, StandardCharsets.UTF_8);
            }
            default -> throw new IOException("Unsupported Redis reply prefix: " + (char) prefix);
        };
    }

    private static String readLine(BufferedInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int ch = in.read();
            if (ch == -1) {
                throw new IOException("Unexpected EOF");
            }
            if (ch == '\r') {
                int next = in.read();
                if (next == '\n') {
                    break;
                }
                sb.append((char) ch).append((char) next);
                continue;
            }
            sb.append((char) ch);
        }
        return sb.toString();
    }
}
