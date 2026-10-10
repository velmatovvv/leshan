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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.stream.IntStream;

import org.bouncycastle.tls.BasicTlsPSKIdentity;
import org.bouncycastle.tls.PSKTlsClient;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.TlsClientProtocol;
import org.bouncycastle.tls.crypto.impl.bc.BcTlsCrypto;
import org.eclipse.leshan.servers.security.InMemorySecurityStore;
import org.eclipse.leshan.servers.security.SecurityInfo;
import org.eclipse.leshan.transport.javacoap.PskTls;
import org.eclipse.leshan.transport.javacoap.identity.PskPrincipal;
import org.eclipse.leshan.transport.javacoap.identity.TlsTransportContextKeys;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

public class PskTlsHandlerTest {
    private static final String IDENTITY = "psk-client";
    private static final byte[] KEY = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

    static IntStream cipherSuites() {
        return IntStream.of(PskTls.cipherSuites());
    }

    @ParameterizedTest
    @MethodSource("cipherSuites")
    public void handshakeAndBidirectionalApplicationData(int cipherSuite) throws Exception {
        InMemorySecurityStore store = new InMemorySecurityStore();
        store.add(SecurityInfo.newPreSharedKeyInfo("endpoint", IDENTITY, KEY));
        PskTlsHandler handler = new PskTlsHandler(store);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        try {
            // Context must never expose an unauthenticated identity.
            assertThrows(IllegalStateException.class, handler::getTransportContext);
            TlsClientProtocol client = new TlsClientProtocol();
            client.connect(
                    new PSKTlsClient(new BcTlsCrypto(new SecureRandom()), new BasicTlsPSKIdentity(IDENTITY, KEY)) {
                        @Override
                        protected ProtocolVersion[] getSupportedVersions() {
                            return PskTls.versions();
                        }

                        @Override
                        protected int[] getSupportedCipherSuites() {
                            return new int[] { cipherSuite };
                        }
                    });
            for (int step = 0; step < 20 && !client.isConnected(); step++) {
                byte[] records = new byte[client.getAvailableOutputBytes()];
                client.readOutput(records, 0, records.length);
                // Feed fragmented records to exercise TCP stream handling.
                for (byte recordByte : records) {
                    channel.writeInbound(Unpooled.wrappedBuffer(new byte[] { recordByte }));
                }
                ByteBuf output;
                while ((output = channel.readOutbound()) != null) {
                    byte[] encrypted = new byte[output.readableBytes()];
                    output.readBytes(encrypted);
                    output.release();
                    client.offerInput(encrypted);
                }
            }
            assertTrue(client.isConnected());
            assertEquals(new PskPrincipal(IDENTITY),
                    handler.getTransportContext().get(TlsTransportContextKeys.PRINCIPAL));
            assertEquals(Integer.toString(cipherSuite),
                    handler.getTransportContext().get(TlsTransportContextKeys.CIPHER_SUITE));

            byte[] payload = "coap application data".getBytes(StandardCharsets.UTF_8);
            client.writeApplicationData(payload, 0, payload.length);
            byte[] encrypted = new byte[client.getAvailableOutputBytes()];
            client.readOutput(encrypted, 0, encrypted.length);
            channel.writeInbound(Unpooled.wrappedBuffer(encrypted));
            ByteBuf received = channel.readInbound();
            byte[] plain = new byte[received.readableBytes()];
            received.readBytes(plain);
            received.release();
            assertArrayEquals(payload, plain);

            channel.writeOutbound(Unpooled.wrappedBuffer(payload));
            ByteBuf response = channel.readOutbound();
            encrypted = new byte[response.readableBytes()];
            response.readBytes(encrypted);
            response.release();
            client.offerInput(encrypted);
            plain = new byte[client.getAvailableInputBytes()];
            client.readInput(plain, 0, plain.length);
            assertArrayEquals(payload, plain);
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
