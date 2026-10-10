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
package org.eclipse.leshan.transport.javacoap.client.coaptcp.endpoint;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;

import javax.net.SocketFactory;

import org.bouncycastle.tls.BasicTlsPSKIdentity;
import org.bouncycastle.tls.PSKTlsClient;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.TlsClientProtocol;
import org.bouncycastle.tls.crypto.impl.bc.BcTlsCrypto;
import org.eclipse.leshan.transport.javacoap.PskTls;

import com.mbed.coap.packet.CoapPacket;
import com.mbed.coap.transport.TransportContext;
import com.mbed.coap.transport.javassl.SocketClientTransport;

/** Blocking TLS PSK transport with the same reconnect behavior as the TCP transport. */
public class PskSocketClientTransport extends SocketClientTransport {
    private final String identity;
    private final byte[] key;
    private volatile TransportContext context;

    public PskSocketClientTransport(InetSocketAddress destination, String identity, byte[] key) {
        super(destination, SocketFactory.getDefault(), true);
        if (identity == null || identity.isEmpty() || key == null || key.length == 0) {
            throw new IllegalArgumentException("PSK identity and key must not be empty");
        }
        this.identity = identity;
        this.key = key.clone();
    }

    @Override
    protected void connect() throws IOException {
        socket = socketFactory.createSocket(destination.getAddress(), destination.getPort());
        try {
            // Bound the handshake, then restore the normal blocking read behavior.
            socket.setSoTimeout(10000);
            TlsClientProtocol protocol = new TlsClientProtocol(socket.getInputStream(), socket.getOutputStream());
            protocol.connect(new PSKTlsClient(new BcTlsCrypto(new SecureRandom()),
                    new BasicTlsPSKIdentity(identity.getBytes(StandardCharsets.UTF_8), key)) {
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
                    super.notifyHandshakeComplete();
                    PskSocketClientTransport.this.context = PskTls.context(context.getSecurityParametersConnection(),
                            identity);
                }
            });
            socket.setSoTimeout(0);
            synchronized (this) {
                outputStream = new BufferedOutputStream(protocol.getOutputStream());
            }
            inputStream = new BufferedInputStream(protocol.getInputStream(), 1024);
            listener.onConnected((InetSocketAddress) socket.getRemoteSocketAddress());
        } catch (IOException | RuntimeException e) {
            socket.close();
            throw e;
        }
    }

    @Override
    public CompletableFuture<CoapPacket> receive() {
        return super.receive().thenApply(packet -> {
            packet.setTransportContext(context);
            return packet;
        });
    }
}
