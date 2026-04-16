package com.jmqx.admin.embedded;

import com.jmqx.common.logging.ClientTraceManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * 管理端配置编解码器。
 *
 * @author liucaiwen
 * @date 2026/4/13
 */
public final class AdminConfigCodec {
    private AdminConfigCodec() {
    }

    public static String encodeSecurityConfigToString(EmbeddedAdminStateStore.SecurityConfig config) {
        return Base64.getEncoder().encodeToString(encodeSecurityConfig(config));
    }

    public static EmbeddedAdminStateStore.SecurityConfig decodeSecurityConfigFromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return decodeSecurityConfig(Base64.getDecoder().decode(raw));
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    public static String encodeClusterConfigToString(EmbeddedAdminStateStore.ClusterConfig config) {
        return Base64.getEncoder().encodeToString(encodeClusterConfig(config));
    }

    public static EmbeddedAdminStateStore.ClusterConfig decodeClusterConfigFromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return decodeClusterConfig(Base64.getDecoder().decode(raw));
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    public static String encodeBridgeConfigToString(EmbeddedAdminStateStore.BridgeConfig config) {
        return Base64.getEncoder().encodeToString(encodeBridgeConfig(config));
    }

    public static EmbeddedAdminStateStore.BridgeConfig decodeBridgeConfigFromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return decodeBridgeConfig(Base64.getDecoder().decode(raw));
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    public static String encodeBlacklistEntryToString(EmbeddedAdminStateStore.BlacklistEntry entry) {
        return Base64.getEncoder().encodeToString(encodeBlacklistEntry(entry));
    }

    public static EmbeddedAdminStateStore.BlacklistEntry decodeBlacklistEntryFromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return decodeBlacklistEntry(Base64.getDecoder().decode(raw));
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    public static String encodeClientTraceTaskToString(ClientTraceManager.ClientTraceTask task) {
        return Base64.getEncoder().encodeToString(encodeClientTraceTask(task));
    }

    public static ClientTraceManager.ClientTraceTask decodeClientTraceTaskFromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return decodeClientTraceTask(Base64.getDecoder().decode(raw));
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private static byte[] encodeClusterConfig(EmbeddedAdminStateStore.ClusterConfig config) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeStringList(data, config.coreNodes());
            writeStringList(data, config.replicantNodes());
            data.writeBoolean(config.coreAcceptClientConnections());
            data.writeInt(config.sharedSubscriptionMaxMembersPerGroup());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode cluster config failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.ClusterConfig decodeClusterConfig(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new EmbeddedAdminStateStore.ClusterConfig(
                    readStringList(in),
                    readStringList(in),
                    in.readBoolean(),
                    in.readInt()
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeBridgeConfig(EmbeddedAdminStateStore.BridgeConfig config) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(3);
            data.writeBoolean(config.enabled());
            writeStringList(data, config.types());
            writeStringList(data, config.topicFilters());
            data.writeBoolean(config.asyncEnabled());
            data.writeInt(config.asyncQueueCapacity());
            data.writeInt(config.asyncWorkerCount());
            data.writeBoolean(config.kafka().enabled());
            writeString(data, config.kafka().bootstrapServers());
            writeString(data, config.kafka().topic());
            writeStringList(data, config.kafka().sourceTopicFilters());
            writeString(data, config.kafka().acks());
            writeString(data, config.kafka().clientId());
            writeString(data, config.kafka().compressionType());
            data.writeBoolean(config.rocketmq().enabled());
            writeString(data, config.rocketmq().nameServer());
            writeString(data, config.rocketmq().producerGroup());
            writeString(data, config.rocketmq().topic());
            writeStringList(data, config.rocketmq().sourceTopicFilters());
            data.writeBoolean(config.rocketmq().syncSend());
            data.writeInt(config.rocketmq().timeoutMs());
            data.writeBoolean(config.mysql().enabled());
            writeString(data, config.mysql().driver());
            writeString(data, config.mysql().url());
            writeString(data, config.mysql().user());
            writeString(data, config.mysql().password());
            writeString(data, config.mysql().table());
            writeStringList(data, config.mysql().sourceTopicFilters());
            data.writeBoolean(config.mysql().autoCreateTable());
            data.writeInt(config.mysql().poolMinIdle());
            data.writeInt(config.mysql().poolMaxSize());
            data.writeLong(config.mysql().poolConnectionTimeoutMs());
            data.writeLong(config.mysql().poolIdleTimeoutMs());
            data.writeLong(config.mysql().poolMaxLifetimeMs());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode bridge config failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.BridgeConfig decodeBridgeConfig(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            int version = in.readByte();
            if (version == 3) {
                return new EmbeddedAdminStateStore.BridgeConfig(
                        in.readBoolean(),
                        readStringList(in),
                        readStringList(in),
                        in.readBoolean(),
                        in.readInt(),
                        in.readInt(),
                        new EmbeddedAdminStateStore.BridgeKafkaConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.BridgeRocketmqConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                in.readBoolean(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.BridgeMysqlConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                in.readBoolean(),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version == 2) {
                EmbeddedAdminStateStore.BridgeMysqlConfig defaults = EmbeddedAdminStateStore.BridgeMysqlConfig.defaults();
                return new EmbeddedAdminStateStore.BridgeConfig(
                        in.readBoolean(),
                        readStringList(in),
                        readStringList(in),
                        in.readBoolean(),
                        in.readInt(),
                        in.readInt(),
                        new EmbeddedAdminStateStore.BridgeKafkaConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.BridgeRocketmqConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                in.readBoolean(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.BridgeMysqlConfig(
                                in.readBoolean(),
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                readStringList(in),
                                in.readBoolean(),
                                defaults.poolMinIdle(),
                                defaults.poolMaxSize(),
                                defaults.poolConnectionTimeoutMs(),
                                defaults.poolIdleTimeoutMs(),
                                defaults.poolMaxLifetimeMs()
                        )
                );
            }
            if (version != 1) {
                return null;
            }
            EmbeddedAdminStateStore.BridgeMysqlConfig defaults = EmbeddedAdminStateStore.BridgeMysqlConfig.defaults();
            return new EmbeddedAdminStateStore.BridgeConfig(
                    in.readBoolean(),
                    readStringList(in),
                    readStringList(in),
                    in.readBoolean(),
                    in.readInt(),
                    in.readInt(),
                    new EmbeddedAdminStateStore.BridgeKafkaConfig(
                            false,
                            readString(in),
                            readString(in),
                            readStringList(in),
                            readString(in),
                            readString(in),
                            readString(in)
                    ),
                    new EmbeddedAdminStateStore.BridgeRocketmqConfig(
                            false,
                            readString(in),
                            readString(in),
                            readString(in),
                            readStringList(in),
                            in.readBoolean(),
                            in.readInt()
                    ),
                    new EmbeddedAdminStateStore.BridgeMysqlConfig(
                            false,
                            readString(in),
                            readString(in),
                            readString(in),
                            readString(in),
                            readString(in),
                            readStringList(in),
                            in.readBoolean(),
                            defaults.poolMinIdle(),
                            defaults.poolMaxSize(),
                            defaults.poolConnectionTimeoutMs(),
                            defaults.poolIdleTimeoutMs(),
                            defaults.poolMaxLifetimeMs()
                    )
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeSecurityConfig(EmbeddedAdminStateStore.SecurityConfig config) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(12);
            data.writeBoolean(config.aclEnabled());
            writeStringList(data, config.aclChain());
            data.writeBoolean(config.aclDefaultAllow());
            writeString(data, config.aclHttp().url());
            data.writeInt(config.aclHttp().timeoutMs());
            writeString(data, config.aclHttp().bodyTemplate());
            writeString(data, config.aclFile().path());
            writeString(data, config.aclRedis().host());
            data.writeInt(config.aclRedis().port());
            writeString(data, config.aclRedis().password());
            data.writeInt(config.aclRedis().db());
            writeString(data, config.aclRedis().keyPrefix());
            data.writeInt(config.aclRedis().timeoutMs());
            data.writeBoolean(config.authEnabled());
            writeStringList(data, config.authChain());
            data.writeLong(config.cacheTtlMs());
            writeString(data, config.authHttp().method());
            writeString(data, config.authHttp().url());
            writeString(data, config.authHttp().headersText());
            data.writeBoolean(config.authHttp().tlsEnabled());
            writeString(data, config.authHttp().bodyTemplate());
            data.writeInt(config.authHttp().poolSize());
            data.writeInt(config.authHttp().rateLimitPerSecond());
            data.writeInt(config.authHttp().requestTimeoutMs());
            data.writeInt(config.authHttp().connectTimeoutMs());
            data.writeInt(config.authHttp().pipelineCount());
            writeString(data, config.authBuiltInDatabase().accountType());
            writeString(data, config.authBuiltInDatabase().passwordHashAlgorithm());
            writeString(data, config.authBuiltInDatabase().saltPosition());
            writeString(data, config.authRedis().host());
            data.writeInt(config.authRedis().port());
            writeString(data, config.authRedis().password());
            data.writeInt(config.authRedis().db());
            writeString(data, config.authRedis().keyPrefix());
            data.writeInt(config.authRedis().timeoutMs());
            writeString(data, config.authMysql().url());
            writeString(data, config.authMysql().user());
            writeString(data, config.authMysql().password());
            writeString(data, config.authMysql().query());
            data.writeInt(config.authMysql().poolMinIdle());
            data.writeInt(config.authMysql().poolMaxSize());
            data.writeLong(config.authMysql().poolConnectionTimeoutMs());
            data.writeLong(config.authMysql().poolIdleTimeoutMs());
            data.writeLong(config.authMysql().poolMaxLifetimeMs());
            writeString(data, config.authPostgresql().url());
            writeString(data, config.authPostgresql().user());
            writeString(data, config.authPostgresql().password());
            writeString(data, config.authPostgresql().query());
            data.writeInt(config.authPostgresql().poolMinIdle());
            data.writeInt(config.authPostgresql().poolMaxSize());
            data.writeLong(config.authPostgresql().poolConnectionTimeoutMs());
            data.writeLong(config.authPostgresql().poolIdleTimeoutMs());
            data.writeLong(config.authPostgresql().poolMaxLifetimeMs());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode security config failed", exception);
        }
    }

    private static byte[] encodeBlacklistEntry(EmbeddedAdminStateStore.BlacklistEntry entry) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeString(data, entry.type());
            writeString(data, entry.value());
            data.writeLong(entry.createdAt());
            writeString(data, entry.source());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode blacklist entry failed", exception);
        }
    }

    private static EmbeddedAdminStateStore.BlacklistEntry decodeBlacklistEntry(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new EmbeddedAdminStateStore.BlacklistEntry(
                    readString(in),
                    readString(in),
                    in.readLong(),
                    readString(in)
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static byte[] encodeClientTraceTask(ClientTraceManager.ClientTraceTask task) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream data = new DataOutputStream(out);
            data.writeByte(1);
            writeString(data, task.id());
            writeString(data, task.clientId());
            data.writeLong(task.startAt());
            data.writeLong(task.endAt());
            data.writeLong(task.createdAt());
            writeString(data, task.createdBy());
            writeString(data, task.filePath());
            data.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode client trace task failed", exception);
        }
    }

    private static ClientTraceManager.ClientTraceTask decodeClientTraceTask(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            if (in.readByte() != 1) {
                return null;
            }
            return new ClientTraceManager.ClientTraceTask(
                    readString(in),
                    readString(in),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    readString(in),
                    readString(in)
            ).normalize();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static EmbeddedAdminStateStore.SecurityConfig decodeSecurityConfig(byte[] raw) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
            int version = in.readByte();
            if (version == 12) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        new EmbeddedAdminStateStore.AclHttpConfig(
                                readString(in),
                                in.readInt(),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AclFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AclRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        new EmbeddedAdminStateStore.AuthHttpConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readBoolean(),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        ),
                        new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version == 11) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        new EmbeddedAdminStateStore.AclHttpConfig(
                                readString(in),
                                in.readInt(),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AclFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AclRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        new EmbeddedAdminStateStore.AuthHttpConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readBoolean(),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        ),
                        new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version == 10) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        new EmbeddedAdminStateStore.AclHttpConfig(
                                readString(in),
                                in.readInt(),
                                EmbeddedAdminStateStore.AclHttpConfig.defaults().bodyTemplate()
                        ),
                        new EmbeddedAdminStateStore.AclFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AclRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        new EmbeddedAdminStateStore.AuthHttpConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readBoolean(),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        ),
                        new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version == 8) {
                return new EmbeddedAdminStateStore.SecurityConfig(
                        in.readBoolean(),
                        readStringList(in),
                        in.readBoolean(),
                        readStringList(in),
                        in.readLong(),
                        new EmbeddedAdminStateStore.AuthHttpConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readBoolean(),
                                readString(in),
                                in.readInt(),
                                0,
                                in.readInt(),
                                in.readInt(),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                        new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                                readString(in),
                                readString(in),
                                readString(in)
                        ),
                        new EmbeddedAdminStateStore.AuthRedisConfig(
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt(),
                                readString(in),
                                in.readInt()
                        ),
                        new EmbeddedAdminStateStore.AuthMysqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        ),
                        new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                                readString(in),
                                readString(in),
                                readString(in),
                                readString(in),
                                in.readInt(),
                                in.readInt(),
                                in.readLong(),
                                in.readLong(),
                                in.readLong()
                        )
                );
            }
            if (version != 9) {
                return null;
            }
            return new EmbeddedAdminStateStore.SecurityConfig(
                    in.readBoolean(),
                    readStringList(in),
                    in.readBoolean(),
                    readStringList(in),
                    in.readLong(),
                    new EmbeddedAdminStateStore.AuthHttpConfig(
                            readString(in),
                            readString(in),
                            readString(in),
                            in.readBoolean(),
                            readString(in),
                            in.readInt(),
                            in.readInt(),
                            in.readInt(),
                            in.readInt(),
                            in.readInt()
                    ),
                    new EmbeddedAdminStateStore.AuthFileConfig(readString(in)),
                    new EmbeddedAdminStateStore.AuthBuiltInDatabaseConfig(
                            readString(in),
                            readString(in),
                            readString(in)
                    ),
                    new EmbeddedAdminStateStore.AuthRedisConfig(
                            readString(in),
                            in.readInt(),
                            readString(in),
                            in.readInt(),
                            readString(in),
                            in.readInt()
                    ),
                    new EmbeddedAdminStateStore.AuthMysqlConfig(
                            readString(in),
                            readString(in),
                            readString(in),
                            readString(in),
                            in.readInt(),
                            in.readInt(),
                            in.readLong(),
                            in.readLong(),
                            in.readLong()
                    ),
                    new EmbeddedAdminStateStore.AuthPostgresqlConfig(
                            readString(in),
                            readString(in),
                            readString(in),
                            readString(in),
                            in.readInt(),
                            in.readInt(),
                            in.readLong(),
                            in.readLong(),
                            in.readLong()
                    )
            );
        } catch (Exception ignored) {
            return null;
        }
    }

    private static void writeString(DataOutputStream out, String value) throws Exception {
        byte[] bytes = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws Exception {
        int length = in.readInt();
        if (length <= 0) {
            return "";
        }
        byte[] bytes = in.readNBytes(length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeStringList(DataOutputStream out, List<String> values) throws Exception {
        List<String> safeValues = values == null ? List.of() : values;
        out.writeInt(safeValues.size());
        for (String value : safeValues) {
            writeString(out, value);
        }
    }

    private static List<String> readStringList(DataInputStream in) throws Exception {
        int size = in.readInt();
        if (size <= 0) {
            return List.of();
        }
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(readString(in));
        }
        return values;
    }
}
