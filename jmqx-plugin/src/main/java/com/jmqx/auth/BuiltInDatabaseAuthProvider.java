package com.jmqx.auth;

import com.jmqx.protocol.AuthResult;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.mindrot.jbcrypt.BCrypt;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 基于 RocksDB 的内置数据库认证。
 *
 * @author liucaiwen
 * @since 2026/4/13
 */
public class BuiltInDatabaseAuthProvider implements AuthProvider {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger LOG = Logger.getLogger(BuiltInDatabaseAuthProvider.class.getName());

    private final String dbPath;
    private final String accountType;
    private final String passwordHashAlgorithm;
    private final String saltPosition;

    public BuiltInDatabaseAuthProvider(AuthProperties properties) {
        this.dbPath = properties.getBuiltInDatabasePath();
        this.accountType = normalize(properties.getBuiltInDatabaseAccountType(), "username");
        this.passwordHashAlgorithm = normalize(properties.getBuiltInDatabasePasswordHashAlgorithm(), "plain");
        this.saltPosition = normalize(properties.getBuiltInDatabaseSaltPosition(), "disable");
        ensureParentDirectory(dbPath);
    }

    @Override
    public AuthResult authenticateResult(AuthRequest request) {
        String principal = resolvePrincipal(request);
        if (principal == null || principal.isBlank()) {
            return AuthResult.deny();
        }
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.openReadOnly(options, dbPath)) {
            byte[] raw = db.get(principal.getBytes(StandardCharsets.UTF_8));
            if (raw == null) {
                return AuthResult.notFound();
            }
            StoredCredential credential = StoredCredential.parse(new String(raw, StandardCharsets.UTF_8));
            return verifyCredential(request.getPassword(), credential)
                    ? AuthResult.allow(credential.superuser())
                    : AuthResult.deny();
        } catch (RocksDBException exception) {
            LOG.log(Level.WARNING, "Built-in database auth failed: " + exception.getMessage(), exception);
            return AuthResult.deny();
        }
    }

    private String resolvePrincipal(AuthRequest request) {
        if ("clientid".equals(accountType) || "client_id".equals(accountType)) {
            return request.getClientId();
        }
        return request.getUsername();
    }

    private boolean verifyCredential(String rawPassword, StoredCredential credential) {
        if (rawPassword == null) {
            return false;
        }
        String passwordWithSalt = applySalt(rawPassword, credential.salt());
        switch (passwordHashAlgorithm) {
            case "plain":
                return passwordWithSalt.equals(credential.passwordHash());
            case "md5":
                return digestHex("MD5", passwordWithSalt).equalsIgnoreCase(credential.passwordHash());
            case "sha":
                return digestHex("SHA-1", passwordWithSalt).equalsIgnoreCase(credential.passwordHash());
            case "sha256":
                return digestHex("SHA-256", passwordWithSalt).equalsIgnoreCase(credential.passwordHash());
            case "sha512":
                return digestHex("SHA-512", passwordWithSalt).equalsIgnoreCase(credential.passwordHash());
            case "bcrypt":
                try {
                    return BCrypt.checkpw(passwordWithSalt, credential.passwordHash());
                } catch (Exception exception) {
                    LOG.log(Level.WARNING, "Verify bcrypt password failed", exception);
                    return false;
                }
            case "pbkdf2":
                return pbkdf2Hex(passwordWithSalt, credential.salt(), credential.iterations())
                        .equalsIgnoreCase(credential.passwordHash());
            default:
                return false;
        }
    }

    private String applySalt(String rawPassword, String salt) {
        if (salt == null || salt.isBlank() || "disable".equals(saltPosition)) {
            return rawPassword;
        }
        if ("prefix".equals(saltPosition)) {
            return salt + rawPassword;
        }
        if ("suffix".equals(saltPosition)) {
            return rawPassword + salt;
        }
        return rawPassword;
    }

    private static String digestHex(String algorithm, String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return toHex(bytes);
        } catch (Exception exception) {
            throw new IllegalStateException("digest " + algorithm + " failed", exception);
        }
    }

    private static String pbkdf2Hex(String value, String salt, int iterations) {
        try {
            PBEKeySpec spec = new PBEKeySpec(
                    value.toCharArray(),
                    (salt == null ? "" : salt).getBytes(StandardCharsets.UTF_8),
                    Math.max(iterations, 1),
                    256
            );
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            return toHex(secretKeyFactory.generateSecret(spec).getEncoded());
        } catch (Exception exception) {
            throw new IllegalStateException("pbkdf2 verify failed", exception);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte aByte : bytes) {
            builder.append(String.format("%02x", aByte));
        }
        return builder.toString();
    }

    private static void ensureParentDirectory(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return;
        }
        try {
            Path path = Path.of(rawPath).toAbsolutePath();
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (Exception exception) {
            LOG.log(Level.WARNING, "Prepare built-in auth db path failed: " + rawPath, exception);
        }
    }

    private static String normalize(String value, String defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private record StoredCredential(String passwordHash, String salt, int iterations, boolean superuser) {
        private static StoredCredential parse(String raw) {
            if (raw == null || raw.isBlank()) {
                return new StoredCredential("", "", 65536, false);
            }
            String trimmed = raw.trim();
            if (!trimmed.startsWith("{")) {
                return new StoredCredential(trimmed, "", 65536, false);
            }
            return new StoredCredential(
                    extractString(trimmed, "passwordHash"),
                    extractString(trimmed, "salt"),
                    extractInt(trimmed, "iterations", 65536),
                    extractBoolean(trimmed, "superuser", false)
            );
        }

        private static String extractString(String body, String key) {
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]*)\"").matcher(body);
            if (matcher.find()) {
                return matcher.group(1);
            }
            return "";
        }

        private static int extractInt(String body, String key, int defaultValue) {
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*(\\d+)").matcher(body);
            if (matcher.find()) {
                try {
                    return Integer.parseInt(matcher.group(1));
                } catch (Exception ignored) {
                    return defaultValue;
                }
            }
            return defaultValue;
        }

        private static boolean extractBoolean(String body, String key, boolean defaultValue) {
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*(true|false)").matcher(body);
            if (matcher.find()) {
                return Boolean.parseBoolean(matcher.group(1));
            }
            return defaultValue;
        }
    }
}
