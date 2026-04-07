package com.jmqx.transport;

import com.jmqx.common.BrokerProperties;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import java.io.File;

/**
 * TLS 证书上下文构建器，供 MQTTS/WSS 服务复用。
 *
 * @author liucaiwen
 * @date 2026/4/7
 */
public final class TlsSslContextProvider {
    private TlsSslContextProvider() {
    }

    public static SslContext buildServerSslContext(BrokerProperties properties) {
        String certPath = trim(properties.getTlsCertChainFile());
        String keyPath = trim(properties.getTlsPrivateKeyFile());
        String keyPassword = trim(properties.getTlsPrivateKeyPassword());
        if (certPath == null || keyPath == null) {
            throw new IllegalStateException(
                "TLS is enabled but cert/key is missing. Please set jmqx.broker.tls.certChainFile and jmqx.broker.tls.privateKeyFile"
            );
        }

        File certFile = new File(certPath);
        File keyFile = new File(keyPath);
        if (!certFile.isFile()) {
            throw new IllegalStateException("TLS cert chain file does not exist: " + certPath);
        }
        if (!keyFile.isFile()) {
            throw new IllegalStateException("TLS private key file does not exist: " + keyPath);
        }

        try {
            if (keyPassword == null) {
                return SslContextBuilder.forServer(certFile, keyFile).build();
            }
            return SslContextBuilder.forServer(certFile, keyFile, keyPassword).build();
        } catch (Exception e) {
            throw new IllegalStateException("Build TLS ssl context failed: " + e.getMessage(), e);
        }
    }

    private static String trim(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
