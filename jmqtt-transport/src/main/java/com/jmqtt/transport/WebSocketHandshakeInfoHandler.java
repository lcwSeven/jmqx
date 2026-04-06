package com.jmqtt.transport;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * @author liucaiwen
 * @date 2026/4/6
 */
public class WebSocketHandshakeInfoHandler extends ChannelInboundHandlerAdapter {
    private static final AttributeKey<String> WS_USERNAME = AttributeKey.valueOf("jmqtt.ws.username");

    @Override
    public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            if (msg instanceof FullHttpRequest request) {
                String username = extractUsername(request);
                if (username != null && !username.isBlank()) {
                    ctx.channel().attr(WS_USERNAME).set(username.trim());
                }
            }
            ctx.fireChannelRead(ReferenceCountUtil.retain(msg));
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private String extractUsername(FullHttpRequest request) {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        String fromQuery = firstQuery(decoder, "username");
        if (fromQuery != null) {
            return fromQuery;
        }
        fromQuery = firstQuery(decoder, "user");
        if (fromQuery != null) {
            return fromQuery;
        }

        String fromHeader = request.headers().get("X-MQTT-Username");
        if (fromHeader != null && !fromHeader.isBlank()) {
            return fromHeader;
        }

        String authorization = request.headers().get(HttpHeaderNames.AUTHORIZATION);
        if (authorization != null && authorization.regionMatches(true, 0, "Basic ", 0, 6)) {
            String encoded = authorization.substring(6).trim();
            if (!encoded.isBlank()) {
                try {
                    String decoded = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
                    int split = decoded.indexOf(':');
                    if (split > 0) {
                        return decoded.substring(0, split);
                    }
                    if (!decoded.isBlank()) {
                        return decoded;
                    }
                } catch (IllegalArgumentException ignored) {
                    return null;
                }
            }
        }
        return null;
    }

    private String firstQuery(QueryStringDecoder decoder, String key) {
        List<String> values = decoder.parameters().get(key);
        if (values == null || values.isEmpty()) {
            return null;
        }
        String value = values.get(0);
        if (value == null || value.isBlank()) {
            return null;
        }
        return value;
    }
}
