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
package org.eclipse.leshan.integration.tests.observe;

import static org.eclipse.leshan.integration.tests.util.Credentials.clientPrivateKeyFromCert;
import static org.eclipse.leshan.integration.tests.util.Credentials.clientX509CertSignedByRoot;
import static org.eclipse.leshan.integration.tests.util.Credentials.serverPrivateKeyFromCert;
import static org.eclipse.leshan.integration.tests.util.Credentials.serverX509CertSignedByRoot;
import static org.eclipse.leshan.integration.tests.util.Credentials.trustedCertificatesByServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.Certificate;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.eclipse.leshan.client.servers.ServerInfo;
import org.eclipse.leshan.core.endpoint.Protocol;
import org.eclipse.leshan.core.observation.CompositeObservation;
import org.eclipse.leshan.core.observation.SingleObservation;
import org.eclipse.leshan.core.request.CancelObservationRequest;
import org.eclipse.leshan.core.request.ContentFormat;
import org.eclipse.leshan.core.request.ObserveCompositeRequest;
import org.eclipse.leshan.core.request.ObserveRequest;
import org.eclipse.leshan.core.request.ReadRequest;
import org.eclipse.leshan.core.request.WriteRequest;
import org.eclipse.leshan.integration.tests.util.LeshanTestClient;
import org.eclipse.leshan.integration.tests.util.LeshanTestClientBuilder;
import org.eclipse.leshan.integration.tests.util.LeshanTestServer;
import org.eclipse.leshan.integration.tests.util.LeshanTestServerBuilder;
import org.eclipse.leshan.server.registration.Registration;
import org.eclipse.leshan.servers.security.InMemorySecurityStore;
import org.eclipse.leshan.servers.security.SecurityInfo;
import org.eclipse.leshan.transport.javacoap.client.coaptcp.endpoint.JavaCoapTcpClientEndpointsProvider;
import org.eclipse.leshan.transport.javacoap.client.coaptcp.endpoint.JavaCoapsTcpClientEndpointsProvider;
import org.eclipse.leshan.transport.javacoap.client.observe.LwM2mKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.mbed.coap.packet.CoapRequest;
import com.mbed.coap.packet.CoapResponse;
import com.mbed.coap.server.CoapServer;
import com.mbed.coap.utils.Service;

public class ObserveCancellationTcpTest {
    private static final byte[] KEY = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    private final LinkedBlockingQueue<CoapRequest> cancellations = new LinkedBlockingQueue<>();
    private final AtomicInteger notificationsBuilt = new AtomicInteger();
    private LeshanTestServer server;
    private LeshanTestClient client;
    private Registration registration;

    static Stream<String> modes() {
        return Stream.of("TCP", "TLS-PSK", "TLS-X509");
    }

    private Service<CoapRequest, CoapResponse> instrument(Service<CoapRequest, CoapResponse> router) {
        return request -> {
            if (Boolean.TRUE.equals(request.getTransContext(LwM2mKeys.LESHAN_NOTIFICATION))) {
                notificationsBuilt.incrementAndGet();
            }
            return router.apply(request).thenApply(response -> {
                if (Integer.valueOf(1).equals(request.options().getObserve())) {
                    cancellations.add(request);
                }
                return response;
            });
        };
    }

    private void start(String mode) throws Exception {
        Protocol protocol = mode.equals("TCP") ? Protocol.COAP_TCP : Protocol.COAPS_TCP;
        LeshanTestServerBuilder sb = new LeshanTestServerBuilder(protocol).with("java-coap")
                .with(new InMemorySecurityStore());
        if (mode.equals("TLS-X509")) {
            sb.using(serverX509CertSignedByRoot, serverPrivateKeyFromCert).trusting(trustedCertificatesByServer);
        }
        server = sb.build();
        server.start();
        LeshanTestClientBuilder cb = new LeshanTestClientBuilder(protocol).connectingTo(server);
        if (mode.equals("TCP")) {
            cb.with(new JavaCoapTcpClientEndpointsProvider() {
                @Override
                protected CoapServer createCoapServer(ServerInfo info, Service<CoapRequest, CoapResponse> router,
                        List<Certificate> trustStore) {
                    return super.createCoapServer(info, instrument(router), trustStore);
                }
            });
        } else {
            cb.with(new JavaCoapsTcpClientEndpointsProvider() {
                @Override
                protected CoapServer createCoapServer(ServerInfo info, Service<CoapRequest, CoapResponse> router,
                        List<Certificate> trustStore) {
                    return super.createCoapServer(info, instrument(router), trustStore);
                }
            });
            if (mode.equals("TLS-PSK")) {
                cb.usingPsk("observe-client", KEY);
            } else {
                cb.using(clientX509CertSignedByRoot, clientPrivateKeyFromCert).trusting(serverX509CertSignedByRoot);
            }
        }
        client = cb.build();
        if (mode.equals("TLS-PSK")) {
            server.getSecurityStore()
                    .add(SecurityInfo.newPreSharedKeyInfo(client.getEndpointName(), "observe-client", KEY));
        } else if (mode.equals("TLS-X509")) {
            server.getSecurityStore().add(SecurityInfo.newX509CertInfo(client.getEndpointName()));
        }
        client.start();
        server.waitForNewRegistrationOf(client);
        client.waitForRegistrationTo(server);
        registration = server.getRegistrationFor(client);
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

    @ParameterizedTest
    @MethodSource("modes")
    public void serviceCancellationReachesClientAndStopsNotifications(String mode) throws Exception {
        start(mode);
        SingleObservation observation = server.send(registration, new ObserveRequest(3, 0, 15)).getObservation();
        server.getObservationService().cancelObservation(observation);
        CoapRequest cancellation = cancellations.poll(2, TimeUnit.SECONDS);
        assertNotNull(cancellation, "Cancellation must reach the client even without another notification");
        assertEquals(com.mbed.coap.packet.Opaque.of(observation.getId().getBytes()), cancellation.getToken());
        assertTrue(server.getObservationService().getObservations(registration).isEmpty());
        assertTrue(server.send(registration, new WriteRequest(3, 0, 15, "Europe/London")).isSuccess());
        assertEquals(0, notificationsBuilt.get(), "Client must stop generating notifications");
        assertTrue(server.send(registration, new ReadRequest(3, 0, 15)).isSuccess());
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void explicitCancellationKeepsItsToken(String mode) throws Exception {
        start(mode);
        SingleObservation observation = server.send(registration, new ObserveRequest(3, 0, 15)).getObservation();
        assertTrue(server.send(registration, new CancelObservationRequest(observation)).isSuccess());
        CoapRequest cancellation = cancellations.poll(2, TimeUnit.SECONDS);
        assertNotNull(cancellation);
        assertEquals(com.mbed.coap.packet.Opaque.of(observation.getId().getBytes()), cancellation.getToken());
        assertTrue(server.send(registration, new WriteRequest(3, 0, 15, "Europe/London")).isSuccess());
        assertEquals(0, notificationsBuilt.get());
    }

    @ParameterizedTest
    @MethodSource("modes")
    public void compositeServiceCancellationReachesClient(String mode) throws Exception {
        start(mode);
        CompositeObservation observation = server.send(registration,
                new ObserveCompositeRequest(ContentFormat.SENML_CBOR, ContentFormat.SENML_CBOR, "/3/0/15", "/3/0/14"))
                .getObservation();
        assertNotNull(observation);
        server.getObservationService().cancelObservation(observation);
        CoapRequest cancellation = cancellations.poll(2, TimeUnit.SECONDS);
        assertNotNull(cancellation);
        assertEquals(com.mbed.coap.packet.Method.FETCH, cancellation.getMethod());
        assertEquals(com.mbed.coap.packet.Opaque.of(observation.getId().getBytes()), cancellation.getToken());
        assertTrue(server.send(registration, new WriteRequest(3, 0, 15, "Europe/London")).isSuccess());
        assertEquals(0, notificationsBuilt.get());
    }
}
