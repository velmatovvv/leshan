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
package org.eclipse.leshan.integration.tests.security;

import static org.eclipse.leshan.integration.tests.util.assertion.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.eclipse.leshan.core.endpoint.Protocol;
import org.eclipse.leshan.core.peer.PskIdentity;
import org.eclipse.leshan.core.request.ReadRequest;
import org.eclipse.leshan.integration.tests.util.LeshanTestClient;
import org.eclipse.leshan.integration.tests.util.LeshanTestClientBuilder;
import org.eclipse.leshan.integration.tests.util.LeshanTestServer;
import org.eclipse.leshan.integration.tests.util.LeshanTestServerBuilder;
import org.eclipse.leshan.server.registration.Registration;
import org.eclipse.leshan.servers.security.InMemorySecurityStore;
import org.eclipse.leshan.servers.security.SecurityInfo;
import org.eclipse.leshan.servers.security.SecurityStoreListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class PskTlsTest {
    private static final String IDENTITY = "tls-psk-идентификатор";
    private static final byte[] KEY = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    private InMemorySecurityStore securityStore;
    private LeshanTestServer server;
    private LeshanTestClient client;

    @BeforeEach
    public void startServer() {
        securityStore = spy(new InMemorySecurityStore());
        server = new LeshanTestServerBuilder(Protocol.COAPS_TCP).with("java-coap").with(securityStore).build();
        server.start();
        client = new LeshanTestClientBuilder(Protocol.COAPS_TCP).with("java-coap").connectingTo(server)
                .usingPsk(IDENTITY, KEY).build();
    }

    @AfterEach
    public void stop() throws InterruptedException {
        if (client != null) {
            client.destroy(false);
        }
        if (server != null) {
            server.destroy();
        }
    }

    @Test
    public void destroyDetachesSecurityStoreListener() {
        ArgumentCaptor<SecurityStoreListener> listener = ArgumentCaptor.forClass(SecurityStoreListener.class);
        verify(securityStore).addListener(listener.capture());
        server.destroy();
        server = null;
        verify(securityStore).removeListener(listener.getValue());
    }

    @Test
    public void registerReadDeregisterAndReconnect() throws Exception {
        server.getSecurityStore().add(SecurityInfo.newPreSharedKeyInfo(client.getEndpointName(), IDENTITY, KEY));
        client.start();
        server.waitForNewRegistrationOf(client);
        client.waitForRegistrationTo(server);
        Registration registration = server.getRegistrationFor(client);
        assertEquals(new PskIdentity(IDENTITY), registration.getClientTransportData().getIdentity());
        assertTrue(server.send(registration, new ReadRequest(3, 0, 1), 2000).isSuccess());
        client.stop(true);
        server.waitForDeregistrationOf(registration);
        client.start();
        server.waitForNewRegistrationOf(client);
        client.waitForRegistrationTo(server);
        assertTrue(server.send(server.getRegistrationFor(client), new ReadRequest(3, 0, 1), 2000).isSuccess());
    }

    @Test
    public void rejectUnknownIdentity() throws Exception {
        server.getSecurityStore().add(SecurityInfo.newPreSharedKeyInfo(client.getEndpointName(), "other", KEY));
        assertThrows(IllegalStateException.class, () -> client.start());
        assertThat(client).after(1, TimeUnit.SECONDS).isNotRegisteredAt(server);
    }

    @Test
    public void rejectWrongKey() throws Exception {
        byte[] wrong = KEY.clone();
        wrong[0] ^= 1;
        server.getSecurityStore().add(SecurityInfo.newPreSharedKeyInfo(client.getEndpointName(), IDENTITY, wrong));
        assertThrows(IllegalStateException.class, () -> client.start());
        assertThat(client).after(1, TimeUnit.SECONDS).isNotRegisteredAt(server);
    }

    @Test
    public void removingCredentialsClosesAuthenticatedConnection() throws Exception {
        server.getSecurityStore().add(SecurityInfo.newPreSharedKeyInfo(client.getEndpointName(), IDENTITY, KEY));
        client.start();
        server.waitForNewRegistrationOf(client);
        client.waitForRegistrationTo(server);
        server.getSecurityStore().remove(client.getEndpointName(), true);
        // Allow the close notification to reach the client before attempting an update.
        Thread.sleep(200);
        client.triggerRegistrationUpdate();
        client.waitForUpdateFailureTo(server, 2, TimeUnit.SECONDS);
    }
}
