/*******************************************************************************
 * Copyright (c) 2026 and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *******************************************************************************/
package org.eclipse.leshan.integration.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.leshan.client.servers.LwM2mServer.SYSTEM;
import static org.eclipse.leshan.integration.tests.util.BootstrapConfigTestBuilder.givenBootstrapConfig;
import static org.eclipse.leshan.integration.tests.util.LeshanTestBootstrapServerBuilder.givenBootstrapServerUsing;
import static org.eclipse.leshan.integration.tests.util.LeshanTestClientBuilder.givenClientUsing;

import java.util.concurrent.TimeUnit;

import org.eclipse.leshan.bsserver.InvalidConfigurationException;
import org.eclipse.leshan.client.servers.LwM2mServer;
import org.eclipse.leshan.core.LwM2mId;
import org.eclipse.leshan.core.endpoint.Protocol;
import org.eclipse.leshan.core.node.LwM2mObject;
import org.eclipse.leshan.core.node.LwM2mObjectInstance;
import org.eclipse.leshan.core.request.ReadRequest;
import org.eclipse.leshan.integration.tests.util.LeshanTestBootstrapServer;
import org.eclipse.leshan.integration.tests.util.LeshanTestClient;
import org.eclipse.leshan.integration.tests.util.LeshanTestServer;
import org.eclipse.leshan.integration.tests.util.LeshanTestServerBuilder;
import org.eclipse.leshan.server.registration.Registration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class MultiServerRegistrationTest {

    private LeshanTestBootstrapServer bootstrapServer;
    private LeshanTestServer firstServer;
    private LeshanTestServer secondServer;
    private LeshanTestClient client;

    @AfterEach
    public void stop() {
        if (client != null) {
            client.destroy(false);
        }
        if (bootstrapServer != null) {
            bootstrapServer.destroy();
        }
        if (firstServer != null) {
            firstServer.destroy();
        }
        if (secondServer != null) {
            secondServer.destroy();
        }
    }

    @Test
    public void register_update_and_deregister_with_two_dm_servers() throws InvalidConfigurationException {
        firstServer = new LeshanTestServerBuilder(Protocol.COAP).with("Californium").build();
        secondServer = new LeshanTestServerBuilder(Protocol.COAP).with("Californium").build();
        firstServer.start();
        secondServer.start();

        bootstrapServer = givenBootstrapServerUsing(Protocol.COAP).with("Californium").build();
        bootstrapServer.start();

        client = givenClientUsing(Protocol.COAP).with("Californium").connectingTo(bootstrapServer).build();
        bootstrapServer.getConfigStore().add(client.getEndpointName(), givenBootstrapConfig()
                .adding(Protocol.COAP, bootstrapServer)
                .adding(Protocol.COAP, firstServer)
                .adding(Protocol.COAP, secondServer).build());

        client.start();
        System.out.println("MULTI_SERVER_PORTS first=" + firstServer.getEndpoint(Protocol.COAP).getURI()
                + " second=" + secondServer.getEndpoint(Protocol.COAP).getURI()
                + " registered=" + client.getRegisteredServers());

        bootstrapServer.waitForSuccessfullBootstrap(10, TimeUnit.SECONDS);
        LwM2mObject security = (LwM2mObject) client.getObjectTree()
                .getObjectEnabler(LwM2mId.SECURITY).read(SYSTEM, new ReadRequest(LwM2mId.SECURITY)).getContent();
        LwM2mObject servers = (LwM2mObject) client.getObjectTree()
                .getObjectEnabler(LwM2mId.SERVER).read(SYSTEM, new ReadRequest(LwM2mId.SERVER)).getContent();
        for (LwM2mObjectInstance instance : security.getInstances().values()) {
            System.out.println("BOOTSTRAP_SECURITY instance=" + instance.getId()
                    + " uri=" + instance.getResource(LwM2mId.SEC_SERVER_URI)
                    + " serverId=" + instance.getResource(LwM2mId.SEC_SERVER_ID)
                    + " bootstrap=" + instance.getResource(LwM2mId.SEC_BOOTSTRAP));
        }
        for (LwM2mObjectInstance instance : servers.getInstances().values()) {
            System.out.println("BOOTSTRAP_DM_SERVER instance=" + instance.getId()
                    + " serverId=" + instance.getResource(LwM2mId.SRV_SERVER_ID)
                    + " lifetime=" + instance.getResource(LwM2mId.SRV_LIFETIME));
        }
        System.out.println("BOOTSTRAP_SECURITY_INSTANCES " + security.getInstances());
        System.out.println("BOOTSTRAP_SERVER_INSTANCES " + servers.getInstances());
        assertThat(security.getInstances()).hasSize(3);
        assertThat(servers.getInstances()).hasSize(2);
        firstServer.waitForNewRegistrationOf(client, 10, TimeUnit.SECONDS);
        secondServer.waitForNewRegistrationOf(client, 10, TimeUnit.SECONDS);

        assertThat(client.getRegisteredServers()).hasSize(2);
        Registration firstRegistration = firstServer.getRegistrationFor(client);
        Registration secondRegistration = secondServer.getRegistrationFor(client);
        assertThat(firstRegistration).isNotNull();
        assertThat(secondRegistration).isNotNull();

        client.triggerRegistrationUpdate();
        firstServer.waitForUpdateOf(firstRegistration);
        secondServer.waitForUpdateOf(secondRegistration);

        int firstServerPort = firstServer.getEndpoint(Protocol.COAP).getURI().getPort();
        LwM2mServer firstClientServer = client.getRegisteredServers().values().stream()
                .filter(server -> java.net.URI.create(server.getUri()).getPort() == firstServerPort).findFirst()
                .orElseThrow(() -> new IllegalStateException("First DM server registration not found"));
        client.triggerRegistrationUpdate(firstClientServer);
        firstServer.waitForUpdateOf(firstRegistration);

        client.stop(true);
        firstServer.waitForDeregistrationOf(firstRegistration);
        secondServer.waitForDeregistrationOf(secondRegistration);
        assertThat(client.getRegisteredServers()).isEmpty();
    }
}
