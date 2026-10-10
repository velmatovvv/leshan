/*******************************************************************************
 * Copyright (c) 2024 Sierra Wireless and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *    http://www.eclipse.org/org/documents/edl-v10.html.
 *
 * Contributors:
 *     Sierra Wireless - initial API and implementation
 *******************************************************************************/
package org.eclipse.leshan.transport.javacoap.server.coaptcp.transport;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.bouncycastle.tls.AlertDescription;
import org.bouncycastle.tls.PSKTlsServer;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.TlsFatalAlert;
import org.bouncycastle.tls.TlsPSKIdentityManager;
import org.bouncycastle.tls.TlsServerProtocol;
import org.bouncycastle.tls.crypto.impl.bc.BcTlsCrypto;
import org.eclipse.leshan.servers.security.SecurityInfo;
import org.eclipse.leshan.servers.security.SecurityStore;
import org.eclipse.leshan.transport.javacoap.PskTls;

import com.mbed.coap.transport.TransportContext;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.ReferenceCountUtil;

/** Nonblocking TLS PSK record processing; one instance per connection. */
public class PskTlsHandler extends ChannelDuplexHandler {
    private final SecurityStore securityStore;
    private final TlsServerProtocol protocol = new TlsServerProtocol();
    private TransportContext transportContext;
    private volatile String pskIdentity;
    private String endpoint;
    private byte[] handshakeKey;
    private ScheduledFuture<?> handshakeTimeout;

    public PskTlsHandler(SecurityStore securityStore) {
        this.securityStore = securityStore;
    }

    public String getPskIdentity() {
        return pskIdentity;
    }

    public TransportContext getTransportContext() {
        if (transportContext == null) {
            throw new IllegalStateException("PSK handshake has not completed");
        }
        return transportContext;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        handshakeTimeout = ctx.executor().schedule(() -> ctx.close(), 10, TimeUnit.SECONDS);
        protocol.accept(new PSKTlsServer(new BcTlsCrypto(new SecureRandom()), new TlsPSKIdentityManager() {
            @Override
            public byte[] getHint() {
                return null;
            }

            @Override
            public byte[] getPSK(byte[] identity) {
                if (securityStore == null) {
                    return null;
                }
                String name = new String(identity, StandardCharsets.UTF_8);
                // Reject malformed UTF-8 rather than authenticating a replacement-character identity.
                if (!Arrays.equals(identity, name.getBytes(StandardCharsets.UTF_8))) {
                    return null;
                }
                SecurityInfo info = securityStore.getByIdentity(name);
                if (info == null || info.getPreSharedKey() == null) {
                    return null;
                }
                pskIdentity = name;
                endpoint = info.getEndpoint();
                clearHandshakeKey();
                handshakeKey = info.getPreSharedKey().clone();
                return handshakeKey.clone();
            }
        }) {
            @Override
            protected ProtocolVersion[] getSupportedVersions() {
                return PskTls.versions();
            }

            @Override
            protected int[] getSupportedCipherSuites() {
                return PskTls.cipherSuites();
            }

            @Override
            public void notifyHandshakeComplete() throws IOException {
                try {
                    // A key may have been removed or replaced while the handshake was in progress.
                    SecurityInfo current = securityStore.getByIdentity(pskIdentity);
                    if (current == null || !endpoint.equals(current.getEndpoint()) || current.getPreSharedKey() == null
                            || handshakeKey == null
                            || !MessageDigest.isEqual(handshakeKey, current.getPreSharedKey())) {
                        throw new TlsFatalAlert(AlertDescription.access_denied);
                    }
                    super.notifyHandshakeComplete();
                    transportContext = PskTls.context(context.getSecurityParametersConnection(), pskIdentity);
                    handshakeTimeout.cancel(false);
                } finally {
                    clearHandshakeKey();
                }
            }
        });
        // Suppress connection activation until authentication completes.
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            ByteBuf buffer = (ByteBuf) msg;
            byte[] encrypted = new byte[buffer.readableBytes()];
            buffer.readBytes(encrypted);
            boolean authenticated = transportContext != null;
            protocol.offerInput(encrypted);
            drainOutput(ctx);
            if (!authenticated && transportContext != null) {
                ctx.fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS);
            }
            while (protocol.getAvailableInputBytes() > 0) {
                byte[] plain = new byte[protocol.getAvailableInputBytes()];
                int count = protocol.readInput(plain, 0, plain.length);
                ctx.fireChannelRead(Unpooled.wrappedBuffer(plain, 0, count));
            }
            if (protocol.isClosed()) {
                ctx.close();
            }
        } catch (Exception e) {
            drainOutput(ctx);
            ctx.close();
            throw e;
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            ByteBuf buffer = (ByteBuf) msg;
            byte[] plain = new byte[buffer.readableBytes()];
            buffer.readBytes(plain);
            protocol.writeApplicationData(plain, 0, plain.length);
            byte[] encrypted = new byte[protocol.getAvailableOutputBytes()];
            protocol.readOutput(encrypted, 0, encrypted.length);
            ctx.write(Unpooled.wrappedBuffer(encrypted), promise);
        } catch (Exception e) {
            promise.tryFailure(e);
            ctx.close();
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    private void drainOutput(ChannelHandlerContext ctx) throws IOException {
        while (protocol.getAvailableOutputBytes() > 0) {
            byte[] encrypted = new byte[protocol.getAvailableOutputBytes()];
            protocol.readOutput(encrypted, 0, encrypted.length);
            ctx.writeAndFlush(Unpooled.wrappedBuffer(encrypted));
        }
    }

    private void clearHandshakeKey() {
        if (handshakeKey != null) {
            Arrays.fill(handshakeKey, (byte) 0);
            handshakeKey = null;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (handshakeTimeout != null) {
            handshakeTimeout.cancel(false);
        }
        clearHandshakeKey();
        super.channelInactive(ctx);
    }
}
