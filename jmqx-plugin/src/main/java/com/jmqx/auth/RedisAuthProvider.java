package com.jmqx.auth;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/4
 */
public class RedisAuthProvider implements AuthProvider {
    private static final Logger LOG = Logger.getLogger(RedisAuthProvider.class.getName());

    private final String host;
    private final int port;
    private final String password;
    private final int db;
    private final String keyPrefix;
    private final int timeoutMs;

    public RedisAuthProvider(AuthProperties properties) {
        this.host = properties.getRedisHost();
        this.port = properties.getRedisPort();
        this.password = properties.getRedisPassword();
        this.db = properties.getRedisDb();
        this.keyPrefix = properties.getRedisKeyPrefix();
        this.timeoutMs = Math.max(properties.getRedisTimeoutMs(), 200);
    }

    @Override
    public boolean authenticate(AuthRequest request) {
        return authenticateDecision(request) == AuthDecision.ALLOW;
    }

    @Override
    public AuthDecision authenticateDecision(AuthRequest request) {
        String username = request.getUsername();
        if (username == null || username.isBlank()) {
            return AuthDecision.DENY;
        }
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
            writeCommand(out, "GET", keyPrefix + ":" + username);
            String expected = readReply(in);
            if (expected == null) {
                return AuthDecision.NOT_FOUND;
            }
            return expected.equals(request.getPassword()) ? AuthDecision.ALLOW : AuthDecision.DENY;
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Redis auth request failed: " + e.getMessage(), e);
            return AuthDecision.DENY;
        }
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
