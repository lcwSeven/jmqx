package com.jmqx.bench;

import com.jmqx.common.logging.Loggers;
import com.jmqx.common.logging.LoggingBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.util.AttributeKey;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;

/**
 * jmqx benchmark entry.
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public class JmqxBenchApplication {
    private static final Logger LOG = Loggers.getLogger(JmqxBenchApplication.class);
    private static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("jmqx.bench.clientId");
    private static final int MAX_MQTT_MESSAGE_SIZE = 256 * 1024;

    private final BenchConfig config;
    private final BenchMetrics metrics = new BenchMetrics();
    private final NioEventLoopGroup ioGroup;
    private final ScheduledExecutorService scheduler;
    private final Map<String, Channel> activeChannels = new ConcurrentHashMap<>();
    private final AtomicInteger packetId = new AtomicInteger(1);
    private final AtomicLong publishRoundRobin = new AtomicLong(0);
    private final byte[] publishPayload;
    private final String subscribeFilter;

    private volatile boolean running = true;
    private Bootstrap bootstrap;

    public JmqxBenchApplication(BenchConfig config) {
        this.config = config;
        this.ioGroup = new NioEventLoopGroup(Math.max(config.ioThreads, 1));
        this.scheduler = Executors.newScheduledThreadPool(3, new NamedThreadFactory("jmqx-bench-scheduler"));
        this.publishPayload = buildPayload(config.payloadBytes);
        this.subscribeFilter = config.topicPrefix + "/#";
    }

    public static void main(String[] args) throws InterruptedException {
        LoggingBootstrap.initialize();
        BenchConfig config = BenchConfig.parse(args);
        if (config.help) {
            BenchConfig.printHelp();
            return;
        }

        JmqxBenchApplication app = new JmqxBenchApplication(config);
        Runtime.getRuntime().addShutdownHook(new Thread(app::shutdown));
        app.start();
        Thread.currentThread().join();
    }

    private void start() {
        LOG.info("JMQX-BENCH start host={}, port={}, clients={}, connectRatePerSec={}, subscribe={}, publishRatePerSec={}, payloadBytes={}, qos={}",
            config.host,
            config.port,
            config.clientCount,
            config.connectRatePerSecond,
            config.subscribe,
            config.publishRatePerSecond,
            config.payloadBytes,
            config.qos);

        bootstrap = new Bootstrap()
            .group(ioGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectTimeoutMs)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline()
                        .addLast("mqtt-decoder", new MqttDecoder(MAX_MQTT_MESSAGE_SIZE))
                        .addLast("mqtt-encoder", MqttEncoder.INSTANCE)
                        .addLast("bench-handler", new BenchClientHandler());
                }
            });

        scheduler.scheduleAtFixedRate(this::printStats, config.reportIntervalSeconds, config.reportIntervalSeconds, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::publishTick, 1, 1, TimeUnit.SECONDS);
        scheduler.execute(this::connectRampUp);
    }

    private void connectRampUp() {
        long delayNanos = config.connectRatePerSecond <= 0
            ? 0
            : TimeUnit.SECONDS.toNanos(1) / Math.max(config.connectRatePerSecond, 1);

        for (int i = 0; i < config.clientCount && running; i++) {
            String clientId = config.clientPrefix + "-" + i;
            connectOne(clientId);
            if (delayNanos > 0) {
                sleepNanos(delayNanos);
            }
        }
    }

    private void connectOne(String clientId) {
        metrics.connectAttempt.incrementAndGet();
        ChannelFuture future = bootstrap.connect(config.host, config.port);
        future.addListener(connectFuture -> {
            if (!connectFuture.isSuccess()) {
                metrics.connectFail.incrementAndGet();
                Throwable cause = connectFuture.cause();
                if (cause != null) {
                    LOG.debug("[CONNECT-FAIL] clientId={}, error={}", clientId, cause.getMessage());
                }
                return;
            }
            Channel channel = future.channel();
            channel.attr(CLIENT_ID).set(clientId);
            channel.writeAndFlush(buildConnectMessage(clientId));
        });
    }

    private MqttMessage buildConnectMessage(String clientId) {
        return MqttMessageBuilders.connect()
            .protocolVersion(io.netty.handler.codec.mqtt.MqttVersion.MQTT_3_1_1)
            .clientId(clientId)
            .cleanSession(config.cleanSession)
            .keepAlive(Math.max(config.keepAliveSeconds, 0))
            .build();
    }

    private void publishTick() {
        if (!running || config.publishRatePerSecond <= 0) {
            return;
        }
        List<Channel> channels = new ArrayList<>(activeChannels.values());
        if (channels.isEmpty()) {
            return;
        }

        int total = config.publishRatePerSecond;
        long start = publishRoundRobin.getAndAdd(total);
        for (int i = 0; i < total; i++) {
            Channel channel = channels.get((int) Math.floorMod(start + i, channels.size()));
            if (!channel.isActive()) {
                metrics.publishFail.incrementAndGet();
                continue;
            }
            String topic = config.topicPrefix + "/" + Math.floorMod(start + i, config.topicShards);
            ByteBuf payload = Unpooled.wrappedBuffer(publishPayload);
            MqttMessageBuilders.PublishBuilder publishBuilder = MqttMessageBuilders.publish()
                .topicName(topic)
                .retained(false)
                .qos(MqttQoS.valueOf(config.qos))
                .payload(payload);
            if (config.qos == MqttQoS.AT_LEAST_ONCE.value()) {
                publishBuilder.messageId(nextPacketId());
            }
            MqttPublishMessage message = publishBuilder.build();
            channel.writeAndFlush(message).addListener(writeFuture -> {
                if (writeFuture.isSuccess()) {
                    metrics.publishSuccess.incrementAndGet();
                } else {
                    metrics.publishFail.incrementAndGet();
                }
            });
        }
    }

    private void printStats() {
        long online = activeChannels.size();
        long connectAttempt = metrics.connectAttempt.get();
        long connectSuccess = metrics.connectSuccess.get();
        long connectFail = metrics.connectFail.get();
        long subscribeAck = metrics.subscribeAck.get();
        long publishSuccess = metrics.publishSuccess.get();
        long publishFail = metrics.publishFail.get();
        long recvPublish = metrics.recvPublish.get();
        long error = metrics.errorCount.get();

        LOG.info("[STATS] online={}, connectAttempt={}, connectSuccess={}, connectFail={}, subAck={}, publishSuccess={}, publishFail={}, recvPublish={}, error={}",
            online,
            connectAttempt,
            connectSuccess,
            connectFail,
            subscribeAck,
            publishSuccess,
            publishFail,
            recvPublish,
            error);
    }

    private void shutdown() {
        if (!running) {
            return;
        }
        running = false;
        scheduler.shutdownNow();
        activeChannels.values().forEach(channel -> {
            try {
                channel.close();
            } catch (Exception ignored) {
            }
        });
        activeChannels.clear();
        ioGroup.shutdownGracefully();
    }

    private int nextPacketId() {
        int next = packetId.updateAndGet(v -> v >= 65535 ? 1 : v + 1);
        return next;
    }

    private static void sleepNanos(long nanos) {
        if (nanos <= 0) {
            return;
        }
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        int extraNanos = (int) (nanos - TimeUnit.MILLISECONDS.toNanos(millis));
        try {
            Thread.sleep(millis, extraNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static byte[] buildPayload(int size) {
        int finalSize = Math.max(size, 1);
        byte[] data = new byte[finalSize];
        byte[] source = "jmqx-bench".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < finalSize; i++) {
            data[i] = source[i % source.length];
        }
        return data;
    }

    /**
     * MQTT client channel handler for one benchmark connection.
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private class BenchClientHandler extends SimpleChannelInboundHandler<MqttMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MqttMessage message) {
            if (message == null || message.fixedHeader() == null) {
                return;
            }
            MqttMessageType type = message.fixedHeader().messageType();
            switch (type) {
                case CONNACK -> onConnAck(ctx, (MqttConnAckMessage) message);
                case SUBACK -> metrics.subscribeAck.incrementAndGet();
                case PUBLISH -> onInboundPublish(ctx, (MqttPublishMessage) message);
                default -> {
                }
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            String clientId = ctx.channel().attr(CLIENT_ID).get();
            if (clientId != null) {
                activeChannels.remove(clientId);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            metrics.errorCount.incrementAndGet();
            if (cause != null) {
                LOG.debug("[BENCH-ERROR] {}", cause.getMessage(), cause);
            }
            ctx.close();
        }

        private void onConnAck(ChannelHandlerContext ctx, MqttConnAckMessage connAckMessage) {
            String clientId = ctx.channel().attr(CLIENT_ID).get();
            if (connAckMessage.variableHeader().connectReturnCode() != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                metrics.connectFail.incrementAndGet();
                LOG.debug("[CONNACK] rejected clientId={}, code={}", clientId, connAckMessage.variableHeader().connectReturnCode());
                ctx.close();
                return;
            }

            metrics.connectSuccess.incrementAndGet();
            if (clientId != null) {
                activeChannels.put(clientId, ctx.channel());
            }
            if (config.subscribe) {
                ctx.writeAndFlush(buildSubscribeMessage());
            }
        }

        private void onInboundPublish(ChannelHandlerContext ctx, MqttPublishMessage message) {
            metrics.recvPublish.incrementAndGet();
            if (message.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE) {
                ctx.writeAndFlush(MqttMessageBuilders.pubAck().packetId(message.variableHeader().packetId()).build());
            }
        }

        private MqttSubscribeMessage buildSubscribeMessage() {
            return MqttMessageBuilders.subscribe()
                .messageId(nextPacketId())
                .addSubscription(MqttQoS.valueOf(config.qos), subscribeFilter)
                .build();
        }
    }

    /**
     * Benchmark runtime counters.
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private static class BenchMetrics {
        private final AtomicLong connectAttempt = new AtomicLong();
        private final AtomicLong connectSuccess = new AtomicLong();
        private final AtomicLong connectFail = new AtomicLong();
        private final AtomicLong subscribeAck = new AtomicLong();
        private final AtomicLong publishSuccess = new AtomicLong();
        private final AtomicLong publishFail = new AtomicLong();
        private final AtomicLong recvPublish = new AtomicLong();
        private final AtomicLong errorCount = new AtomicLong();
    }

    /**
     * Benchmark config parser.
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private static class BenchConfig {
        private boolean help;
        private String host = "127.0.0.1";
        private int port = 1883;
        private int clientCount = 30_000;
        private int connectRatePerSecond = 2_000;
        private int connectTimeoutMs = 5_000;
        private int ioThreads = Math.max(Runtime.getRuntime().availableProcessors(), 4);
        private String clientPrefix = "bench-client";
        private int keepAliveSeconds = 120;
        private boolean cleanSession = true;
        private boolean subscribe = false;
        private String topicPrefix = "bench/topic";
        private int topicShards = 1024;
        private int publishRatePerSecond = 0;
        private int payloadBytes = 128;
        private int qos = 0;
        private int reportIntervalSeconds = 5;

        private static BenchConfig parse(String[] args) {
            BenchConfig config = new BenchConfig();
            for (String arg : args) {
                if (arg == null || arg.isBlank()) {
                    continue;
                }
                if ("--help".equals(arg) || "-h".equals(arg)) {
                    config.help = true;
                    continue;
                }
                if (!arg.startsWith("--") || !arg.contains("=")) {
                    continue;
                }
                int idx = arg.indexOf('=');
                String key = arg.substring(2, idx);
                String value = arg.substring(idx + 1);
                switch (key) {
                    case "host" -> config.host = value;
                    case "port" -> config.port = parseInt(value, config.port);
                    case "clients" -> config.clientCount = parseInt(value, config.clientCount);
                    case "connectRate" -> config.connectRatePerSecond = parseInt(value, config.connectRatePerSecond);
                    case "connectTimeoutMs" -> config.connectTimeoutMs = parseInt(value, config.connectTimeoutMs);
                    case "ioThreads" -> config.ioThreads = parseInt(value, config.ioThreads);
                    case "clientPrefix" -> config.clientPrefix = value;
                    case "keepAliveSeconds" -> config.keepAliveSeconds = parseInt(value, config.keepAliveSeconds);
                    case "cleanSession" -> config.cleanSession = parseBoolean(value, config.cleanSession);
                    case "subscribe" -> config.subscribe = parseBoolean(value, config.subscribe);
                    case "topicPrefix" -> config.topicPrefix = value;
                    case "topicShards" -> config.topicShards = parseInt(value, config.topicShards);
                    case "publishRate" -> config.publishRatePerSecond = parseInt(value, config.publishRatePerSecond);
                    case "payloadBytes" -> config.payloadBytes = parseInt(value, config.payloadBytes);
                    case "qos" -> config.qos = parseInt(value, config.qos);
                    case "reportSeconds" -> config.reportIntervalSeconds = parseInt(value, config.reportIntervalSeconds);
                    default -> {
                    }
                }
            }
            config.clientCount = Math.max(config.clientCount, 1);
            config.connectRatePerSecond = Math.max(config.connectRatePerSecond, 0);
            config.ioThreads = Math.max(config.ioThreads, 1);
            config.topicShards = Math.max(config.topicShards, 1);
            config.payloadBytes = Math.max(config.payloadBytes, 1);
            config.reportIntervalSeconds = Math.max(config.reportIntervalSeconds, 1);
            config.qos = (config.qos == 1) ? 1 : 0;
            return config;
        }

        private static int parseInt(String raw, int defaultValue) {
            try {
                return Integer.parseInt(raw);
            } catch (NumberFormatException ignored) {
                return defaultValue;
            }
        }

        private static boolean parseBoolean(String raw, boolean defaultValue) {
            if ("true".equalsIgnoreCase(raw) || "1".equals(raw) || "yes".equalsIgnoreCase(raw)) {
                return true;
            }
            if ("false".equalsIgnoreCase(raw) || "0".equals(raw) || "no".equalsIgnoreCase(raw)) {
                return false;
            }
            return defaultValue;
        }

        private static void printHelp() {
            String help = """
                jmqx-bench usage:
                  mvn -pl jmqx-bench -am exec:java -Dexec.args="--host=127.0.0.1 --port=1883 --clients=10000"

                options:
                  --host=127.0.0.1
                  --port=1883
                  --clients=10000
                  --connectRate=2000             connect attempts per second
                  --connectTimeoutMs=5000
                  --ioThreads=8
                  --clientPrefix=bench-client
                  --keepAliveSeconds=120
                  --cleanSession=true
                  --subscribe=false              auto subscribe topicPrefix/#
                  --topicPrefix=bench/topic
                  --topicShards=1024
                  --publishRate=0                messages per second, 0 means disabled
                  --payloadBytes=128
                  --qos=0                        supports 0/1
                  --reportSeconds=5
                """;
            System.out.println(help);
        }
    }

    /**
     * Named thread factory for benchmark schedulers.
     *
     * @author liucaiwen
     * @date 2026/4/9
     */
    private static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger index = new AtomicInteger(0);

        private NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, prefix + "-" + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
