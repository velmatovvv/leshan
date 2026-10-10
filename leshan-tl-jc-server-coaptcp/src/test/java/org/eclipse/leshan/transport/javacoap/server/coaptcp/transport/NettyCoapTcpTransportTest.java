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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.eclipse.leshan.servers.security.InMemorySecurityStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

public class NettyCoapTcpTransportTest {
    @Test
    public void failedBindReleasesResourcesAndAllowsRetry() throws Exception {
        NettyCoapTcpTransport transport;
        try (ServerSocket occupied = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            transport = new NettyCoapTcpTransport((InetSocketAddress) occupied.getLocalSocketAddress(),
                    new CoapTcpTransportResolver(), new DefaultTransportContextMatcher(), null);
            try {
                assertThrows(IOException.class, transport::start);
            } finally {
                transport.stop();
            }
        }
        try {
            transport.start();
            assertTrue(transport.getChannel().isActive());
        } finally {
            transport.stop();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true", "true, true" })
    public void stopClosesChannelsAndTerminatesBothEventLoopGroups(boolean pendingHandshake, boolean fromEventLoop)
            throws Exception {
        CompletableFuture<SocketChannel> accepted = new CompletableFuture<>();
        NettyCoapTcpTransport transport = new NettyCoapTcpTransport(new InetSocketAddress("127.0.0.1", 0),
                new CoapTcpTransportResolver(), new DefaultTransportContextMatcher(), null) {
            @Override
            protected ChannelHandler createTlsHandler(SocketChannel channel) {
                accepted.complete(channel);
                return pendingHandshake ? new PskTlsHandler(new InMemorySecurityStore()) : null;
            }
        };
        transport.start();
        EventLoopGroup boss = transport.getChannel().eventLoop().parent();
        try (Socket socket = new Socket()) {
            socket.connect(transport.getLocalSocketAddress());
            SocketChannel channel = accepted.get(2, TimeUnit.SECONDS);
            EventLoopGroup worker = channel.eventLoop().parent();
            if (fromEventLoop) {
                channel.eventLoop().submit(transport::stop).get(2, TimeUnit.SECONDS);
            } else {
                transport.stop();
            }
            assertTrue(channel.closeFuture().await(2, TimeUnit.SECONDS),
                    "Child socket must close, including before authentication");
            assertTrue(boss.terminationFuture().await(2, TimeUnit.SECONDS), "Boss event loop must terminate");
            assertTrue(worker.terminationFuture().await(2, TimeUnit.SECONDS), "Worker event loop must terminate");
            socket.setSoTimeout(2000);
            assertEquals(-1, socket.getInputStream().read());
        } finally {
            transport.stop();
        }
    }
}
