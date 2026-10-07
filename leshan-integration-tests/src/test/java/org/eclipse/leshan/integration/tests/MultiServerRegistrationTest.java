/*******************************************************************************
 * Copyright (c) 2026 and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *******************************************************************************/
package org.eclipse.leshan.integration.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.leshan.integration.tests.util.LeshanTestClientBuilder.givenClientUsing;

import org.eclipse.leshan.client.servers.LwM2mServer;
import org.eclipse.leshan.core.endpoint.Protocol;
import org.eclipse.leshan.integration.tests.util.LeshanTestClient;
import org.eclipse.leshan.integration.tests.util.LeshanTestServer;
import org.eclipse.leshan.integration.tests.util.LeshanTestServerBuilder;
import org.eclipse.leshan.server.registration.Registration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class MultiServerRegistrationTest {

    private LeshanTestServer firstServer;
    private LeshanTestServer secondServer;
    private LeshanTestClient client;

    @AfterEach
    public void stop() {
        if (client != null) {
            client.destroy(false);
        }
        if (firstServer != null) {
            firstServer.destroy();
        }
        if (secondServer != null) {
            secondServer.destroy();
        }
    }

    @Test
    public void register_update_and_deregister_with_two_dm_servers() {
        firstServer = new LeshanTestServerBuilder(Protocol.COAP).with("Californium").build();
        secondServer = new LeshanTestServerBuilder(Protocol.COAP).with("Californium").build();
        firstServer.start();
        secondServer.start();

        client = givenClientUsing(Protocol.COAP).with("Californium").connectingTo(firstServer)
                .alsoConnectingTo(secondServer).build();

        client.start();

        firstServer.waitForNewRegistrationOf(client);
        secondServer.waitForNewRegistrationOf(client);
        client.waitForRegistrationTo(firstServer);
        client.waitForRegistrationTo(secondServer);

        assertThat(client.getRegisteredServers()).hasSize(2);
        Registration firstRegistration = firstServer.getRegistrationFor(client);
        Registration secondRegistration = secondServer.getRegistrationFor(client);
        assertThat(firstRegistration).isNotNull();
        assertThat(secondRegistration).isNotNull();

        client.triggerRegistrationUpdate();
        firstServer.waitForUpdateOf(firstRegistration);
        secondServer.waitForUpdateOf(secondRegistration);
        client.waitForUpdateTo(firstServer);
        client.waitForUpdateTo(secondServer);

        int firstServerPort = firstServer.getEndpoint(Protocol.COAP).getURI().getPort();
        LwM2mServer firstClientServer = client.getRegisteredServers().values().stream()
                .filter(server -> java.net.URI.create(server.getUri()).getPort() == firstServerPort).findFirst()
                .orElseThrow();
        client.triggerRegistrationUpdate(firstClientServer);
        firstServer.waitForUpdateOf(firstRegistration);

        client.stop(true);
        firstServer.waitForDeregistrationOf(firstRegistration);
        secondServer.waitForDeregistrationOf(secondRegistration);
        assertThat(client.getRegisteredServers()).isEmpty();
    }
}
