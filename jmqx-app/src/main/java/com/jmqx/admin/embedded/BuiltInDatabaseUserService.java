package com.jmqx.admin.embedded;

import com.jmqx.auth.AuthProperties;
import org.mindrot.jbcrypt.BCrypt;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * 内置数据库用户管理服务。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public final class BuiltInDatabaseUserService {
    static {
        RocksDB.loadLibrary();
    }

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int DEFAULT_PBKDF2_ITERATIONS = 65536;

    private final String dbPath;

    public BuiltInDatabaseUserService() {
        this.dbPath = new AuthProperties().getBuiltInDatabasePath();
        ensureParentDirectory(dbPath);
    }

    public List<UserRecord> listUsers() {
        List<UserRecord> records = new ArrayList<>();
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.openReadOnly(options, dbPath);
             RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String userId = new String(iterator.key(), StandardCharsets.UTF_8);
                StoredCredential credential = StoredCredential.parse(new String(iterator.value(), StandardCharsets.UTF_8));
                records.add(new UserRecord(
                        userId,
                        credential.salt() != null && !credential.salt().isBlank(),
                        credential.iterations(),
                        credential.superuser()
                ));
            }
        } catch (RocksDBException exception) {
            throw new IllegalStateException("list built-in database users failed", exception);
        }
        records.sort(Comparator.comparing(UserRecord::userId));
        return records;
    }

    public void upsertUser(
            EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config,
            String userId,
            String rawPassword,
            boolean superuser
    ) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId is required");
        }
        if (rawPassword == null || rawPassword.isBlank()) {
            throw new IllegalArgumentException("password is required");
        }
        StoredCredential credential = encodeCredential(config, rawPassword, superuser);
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, dbPath)) {
            db.put(
                    userId.trim().getBytes(StandardCharsets.UTF_8),
                    credential.toJson().getBytes(StandardCharsets.UTF_8)
            );
        } catch (RocksDBException exception) {
            throw new IllegalStateException("save built-in database user failed", exception);
        }
    }

    public int importUsers(EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config, List<UserInput> users) {
        int count = 0;
        for (UserInput user : users) {
            if (user == null || user.userId() == null || user.userId().isBlank()) {
                continue;
            }
            if (user.password() == null || user.password().isBlank()) {
                continue;
            }
            upsertUser(config, user.userId(), user.password(), user.superuser());
            count++;
        }
        return count;
    }

    public String encodeUserCredential(
            EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config,
            String rawPassword,
            boolean superuser
    ) {
        if (rawPassword == null || rawPassword.isBlank()) {
            throw new IllegalArgumentException("password is required");
        }
        return encodeCredential(config, rawPassword, superuser).toJson();
    }

    public void upsertEncodedUser(String userId, String encodedCredential) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId is required");
        }
        if (encodedCredential == null || encodedCredential.isBlank()) {
            throw new IllegalArgumentException("encoded credential is required");
        }
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, dbPath)) {
            db.put(
                    userId.trim().getBytes(StandardCharsets.UTF_8),
                    encodedCredential.getBytes(StandardCharsets.UTF_8)
            );
        } catch (RocksDBException exception) {
            throw new IllegalStateException("save built-in database user failed", exception);
        }
    }

    public void deleteUser(String userId) {
        if (userId == null || userId.isBlank()) {
            return;
        }
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, dbPath)) {
            db.delete(userId.trim().getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException exception) {
            throw new IllegalStateException("delete built-in database user failed", exception);
        }
    }

    public void deleteAllUsers() {
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, dbPath);
             RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                db.delete(iterator.key());
            }
        } catch (RocksDBException exception) {
            throw new IllegalStateException("delete all built-in database users failed", exception);
        }
    }

    private static StoredCredential encodeCredential(
            EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig config,
            String rawPassword,
            boolean superuser
    ) {
        String algorithm = normalize(config.passwordHashAlgorithm(), "sha256");
        String saltPosition = normalize(config.saltPosition(), "suffix");
        String salt = shouldGenerateSalt(algorithm, saltPosition) ? randomSalt() : "";
        String passwordWithSalt = applySalt(rawPassword, salt, saltPosition);
        if ("plain".equals(algorithm)) {
            return new StoredCredential(passwordWithSalt, salt, DEFAULT_PBKDF2_ITERATIONS, superuser);
        }
        if ("md5".equals(algorithm)) {
            return new StoredCredential(digestHex("MD5", passwordWithSalt), salt, DEFAULT_PBKDF2_ITERATIONS, superuser);
        }
        if ("sha".equals(algorithm)) {
            return new StoredCredential(digestHex("SHA-1", passwordWithSalt), salt, DEFAULT_PBKDF2_ITERATIONS, superuser);
        }
        if ("sha256".equals(algorithm)) {
            return new StoredCredential(digestHex("SHA-256", passwordWithSalt), salt, DEFAULT_PBKDF2_ITERATIONS, superuser);
        }
        if ("sha512".equals(algorithm)) {
            return new StoredCredential(digestHex("SHA-512", passwordWithSalt), salt, DEFAULT_PBKDF2_ITERATIONS, superuser);
        }
        if ("bcrypt".equals(algorithm)) {
            return new StoredCredential(BCrypt.hashpw(passwordWithSalt, BCrypt.gensalt()), salt, DEFAULT_PBKDF2_ITERATIONS, superuser);
        }
        if ("pbkdf2".equals(algorithm)) {
            String effectiveSalt = salt.isBlank() ? randomSalt() : salt;
            String passwordForHash = applySalt(rawPassword, effectiveSalt, saltPosition);
            return new StoredCredential(
                    pbkdf2Hex(passwordForHash, effectiveSalt, DEFAULT_PBKDF2_ITERATIONS),
                    effectiveSalt,
                    DEFAULT_PBKDF2_ITERATIONS,
                    superuser
            );
        }
        throw new IllegalArgumentException("unsupported built-in database algorithm: " + algorithm);
    }

    private static boolean shouldGenerateSalt(String algorithm, String saltPosition) {
        return !"disable".equals(saltPosition) || "pbkdf2".equals(algorithm);
    }

    private static String applySalt(String rawPassword, String salt, String saltPosition) {
        if (salt == null || salt.isBlank() || "disable".equals(saltPosition)) {
            return rawPassword;
        }
        if ("prefix".equals(saltPosition)) {
            return salt + rawPassword;
        }
        return rawPassword + salt;
    }

    private static String digestHex(String algorithm, String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return toHex(bytes);
        } catch (Exception exception) {
            throw new IllegalStateException("digest failed", exception);
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
            throw new IllegalStateException("pbkdf2 hash failed", exception);
        }
    }

    private static String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            builder.append(String.format("%02x", value));
        }
        return builder.toString();
    }

    private static String randomSalt() {
        byte[] bytes = new byte[8];
        RANDOM.nextBytes(bytes);
        return toHex(bytes);
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
            throw new IllegalStateException("prepare built-in auth db path failed", exception);
        }
    }

    private static String normalize(String value, String defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    public record UserRecord(String userId, boolean salted, int iterations, boolean superuser) {
    }

    public record UserInput(String userId, String password, boolean superuser) {
    }

    private record StoredCredential(String passwordHash, String salt, int iterations, boolean superuser) {
        private String toJson() {
            return "{"
                    + "\"passwordHash\":\"" + escape(passwordHash) + "\","
                    + "\"salt\":\"" + escape(salt) + "\","
                    + "\"iterations\":" + iterations + ","
                    + "\"superuser\":" + superuser
                    + "}";
        }

        private static StoredCredential parse(String raw) {
            if (raw == null || raw.isBlank() || !raw.trim().startsWith("{")) {
                return new StoredCredential(raw == null ? "" : raw.trim(), "", DEFAULT_PBKDF2_ITERATIONS, false);
            }
            String trimmed = raw.trim();
            return new StoredCredential(
                    extractString(trimmed, "passwordHash"),
                    extractString(trimmed, "salt"),
                    extractInt(trimmed, "iterations", DEFAULT_PBKDF2_ITERATIONS),
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

        private static String escape(String value) {
            return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}
