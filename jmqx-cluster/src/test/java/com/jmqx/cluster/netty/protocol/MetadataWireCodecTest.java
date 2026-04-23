package com.jmqx.cluster.netty.protocol;

import com.jmqx.cluster.MetadataCommand;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataWireCodecTest {
    @Test
    void shouldRoundTripSubmitRequest() {
        MetadataWireMessage message = new MetadataWireMessage(
            MetadataMessageType.SUBMIT_REQUEST,
            1001L,
            new MetadataCommand("route.subscription", "upsert", "topic-a", "client-a", "core-1"),
            0L,
            0L,
            null,
            false,
            null,
            null
        );

        EmbeddedChannel encoderChannel = new EmbeddedChannel(MetadataWireCodec.encoder());
        EmbeddedChannel decoderChannel = new EmbeddedChannel(MetadataWireCodec.decoder());

        encoderChannel.writeOutbound(message);
        ByteBuf encoded = encoderChannel.readOutbound();
        assertNotNull(encoded);

        decoderChannel.writeInbound(encoded.retainedDuplicate());
        MetadataWireMessage decoded = decoderChannel.readInbound();

        assertNotNull(decoded);
        assertEquals(message.type(), decoded.type());
        assertEquals(message.requestId(), decoded.requestId());
        assertEquals(message.command(), decoded.command());
    }

    @Test
    void shouldRejectPayloadWhenCrcDoesNotMatch() {
        MetadataWireMessage message = new MetadataWireMessage(
            MetadataMessageType.ACK_RESPONSE,
            2002L,
            null,
            0L,
            0L,
            null,
            true,
            null,
            null
        );

        EmbeddedChannel encoderChannel = new EmbeddedChannel(MetadataWireCodec.encoder());
        EmbeddedChannel decoderChannel = new EmbeddedChannel(MetadataWireCodec.decoder());

        encoderChannel.writeOutbound(message);
        ByteBuf encoded = encoderChannel.readOutbound();
        assertNotNull(encoded);

        int lastIndex = encoded.writerIndex() - 1;
        encoded.setByte(lastIndex, encoded.getByte(lastIndex) ^ 0x01);

        DecoderException exception = assertThrows(
            DecoderException.class,
            () -> decoderChannel.writeInbound(encoded.retainedDuplicate())
        );

        assertTrue(exception.getCause() instanceof IllegalStateException);
        assertEquals("metadata payload crc mismatch", exception.getCause().getMessage());
    }
}
