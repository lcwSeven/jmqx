package com.jmqtt.transport;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

import java.util.List;
import java.util.logging.Logger;

/**
 * @author liucaiwen
 * @date 2026/4/6
 */
public class WebSocketFrameToByteBufDecoder extends MessageToMessageDecoder<WebSocketFrame> {
    private static final Logger LOG = Logger.getLogger(WebSocketFrameToByteBufDecoder.class.getName());

    @Override
    protected void decode(io.netty.channel.ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) {
        if (frame instanceof BinaryWebSocketFrame binary) {
            ByteBuf content = binary.content();
            out.add(content.retain());
            return;
        }
        if (frame instanceof PingWebSocketFrame || frame instanceof PongWebSocketFrame) {
            return;
        }
        if (frame instanceof CloseWebSocketFrame) {
            ctx.close();
            return;
        }
        if (frame instanceof TextWebSocketFrame) {
            LOG.warning(() -> "[WS] text frame is not supported, remote=" + ctx.channel().remoteAddress());
            ctx.close();
        }
    }
}
