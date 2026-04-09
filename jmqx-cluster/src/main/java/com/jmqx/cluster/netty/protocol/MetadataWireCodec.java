package com.jmqx.cluster.netty.protocol;

import com.jmqx.cluster.MetadataCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.List;
import java.util.zip.CRC32;

/**
 * 元数据自定义二进制协议编解码器。
 * 协议结构：magic/version/type/requestId/payloadLength/payload/crc32。
 *
 * @author liucaiwen
 * @date 2026/4/9
 */
public final class MetadataWireCodec {
    private static final short MAGIC = (short) 0x4A51;
    private static final byte VERSION = 1;

    private MetadataWireCodec() {
    }

    public static MessageToByteEncoder<MetadataWireMessage> encoder() {
        return new Encoder();
    }

    public static ByteToMessageDecoder decoder() {
        return new Decoder();
    }

    private static final class Encoder extends MessageToByteEncoder<MetadataWireMessage> {
        @Override
        protected void encode(ChannelHandlerContext ctx, MetadataWireMessage msg, ByteBuf out) {
            byte[] payload = encodePayload(msg);
            out.writeShort(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(msg.type());
            out.writeLong(msg.requestId());
            out.writeInt(payload.length);
            out.writeBytes(payload);
            out.writeInt(crc32(payload));
        }
    }

    private static final class Decoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < 2 + 1 + 1 + 8 + 4 + 4) {
                return;
            }
            short magic = in.readShort();
            if (magic != MAGIC) {
                throw new IllegalStateException("invalid metadata frame magic");
            }
            byte version = in.readByte();
            if (version != VERSION) {
                throw new IllegalStateException("unsupported metadata frame version: " + version);
            }
            byte type = in.readByte();
            long requestId = in.readLong();
            int payloadLength = in.readInt();
            if (payloadLength < 0 || payloadLength > in.readableBytes() - 4) {
                throw new IllegalStateException("invalid metadata payload length: " + payloadLength);
            }
            byte[] payload = new byte[payloadLength];
            in.readBytes(payload);
            int expectedCrc = in.readInt();
            if (expectedCrc != crc32(payload)) {
                throw new IllegalStateException("metadata payload crc mismatch");
            }
            out.add(decodePayload(type, requestId, payload));
        }
    }

    private static byte[] encodePayload(MetadataWireMessage message) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(256);
            DataOutputStream dataOutput = new DataOutputStream(out);
            switch (message.type()) {
                case MetadataMessageType.SUBMIT_REQUEST -> writeCommand(dataOutput, message.command());
                case MetadataMessageType.SUBMIT_RESPONSE -> {
                    dataOutput.writeLong(message.logIndex());
                    dataOutput.writeBoolean(message.success());
                    writeNullable(dataOutput, message.leaderEndpoint());
                    writeNullable(dataOutput, message.errorMessage());
                }
                case MetadataMessageType.SUBSCRIBE_REQUEST -> {
                    writeNullable(dataOutput, message.nodeId());
                    dataOutput.writeLong(message.lastAppliedLogIndex());
                }
                case MetadataMessageType.EVENT -> {
                    writeCommand(dataOutput, message.command());
                    dataOutput.writeLong(message.logIndex());
                }
                case MetadataMessageType.ACK_REQUEST -> {
                    writeNullable(dataOutput, message.nodeId());
                    dataOutput.writeLong(message.lastAppliedLogIndex());
                }
                case MetadataMessageType.ACK_RESPONSE -> dataOutput.writeBoolean(message.success());
                case MetadataMessageType.RESET -> dataOutput.writeLong(message.logIndex());
                default -> throw new IllegalStateException("unsupported metadata message type: " + message.type());
            }
            dataOutput.flush();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException("encode metadata payload failed", exception);
        }
    }

    private static MetadataWireMessage decodePayload(byte type, long requestId, byte[] payload) {
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
            return switch (type) {
                case MetadataMessageType.SUBMIT_REQUEST -> new MetadataWireMessage(
                    type, requestId, readCommand(input), 0L, 0L, null, false, null, null
                );
                case MetadataMessageType.SUBMIT_RESPONSE -> new MetadataWireMessage(
                    type,
                    requestId,
                    null,
                    input.readLong(),
                    0L,
                    null,
                    input.readBoolean(),
                    readNullable(input),
                    readNullable(input)
                );
                case MetadataMessageType.SUBSCRIBE_REQUEST -> {
                    String nodeId = readNullable(input);
                    long lastAppliedLogIndex = input.readLong();
                    yield new MetadataWireMessage(
                        type, requestId, null, 0L, lastAppliedLogIndex, nodeId, false, null, null
                    );
                }
                case MetadataMessageType.EVENT -> new MetadataWireMessage(
                    type, requestId, readCommand(input), input.readLong(), 0L, null, false, null, null
                );
                case MetadataMessageType.ACK_REQUEST -> {
                    String nodeId = readNullable(input);
                    long lastAppliedLogIndex = input.readLong();
                    yield new MetadataWireMessage(
                        type, requestId, null, 0L, lastAppliedLogIndex, nodeId, false, null, null
                    );
                }
                case MetadataMessageType.ACK_RESPONSE -> new MetadataWireMessage(
                    type, requestId, null, 0L, 0L, null, input.readBoolean(), null, null
                );
                case MetadataMessageType.RESET -> new MetadataWireMessage(
                    type, requestId, null, input.readLong(), 0L, null, false, null, null
                );
                default -> throw new IllegalStateException("unsupported metadata message type: " + type);
            };
        } catch (Exception exception) {
            throw new IllegalStateException("decode metadata payload failed", exception);
        }
    }

    private static void writeCommand(DataOutputStream out, MetadataCommand command) throws Exception {
        if (command == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        writeNullable(out, command.namespace());
        writeNullable(out, command.operation());
        writeNullable(out, command.key());
        writeNullable(out, command.value());
        writeNullable(out, command.sourceNodeId());
    }

    private static MetadataCommand readCommand(DataInputStream input) throws Exception {
        if (!input.readBoolean()) {
            return null;
        }
        return new MetadataCommand(
            readNullable(input),
            readNullable(input),
            readNullable(input),
            readNullable(input),
            readNullable(input)
        );
    }

    private static void writeNullable(DataOutputStream out, String value) throws Exception {
        if (value == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeUTF(value);
    }

    private static String readNullable(DataInputStream input) throws Exception {
        if (!input.readBoolean()) {
            return null;
        }
        return input.readUTF();
    }

    private static int crc32(byte[] payload) {
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        return (int) crc32.getValue();
    }
}
