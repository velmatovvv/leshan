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
package org.eclipse.leshan.transport.javacoap;

import org.bouncycastle.tls.CipherSuite;
import org.bouncycastle.tls.ProtocolVersion;
import org.bouncycastle.tls.SecurityParameters;
import org.eclipse.leshan.transport.javacoap.identity.PskPrincipal;
import org.eclipse.leshan.transport.javacoap.identity.TlsTransportContextKeys;

import com.mbed.coap.packet.Opaque;
import com.mbed.coap.transport.TransportContext;

/** TLS 1.2 PSK settings shared by the TCP client and server. */
public final class PskTls {
    private PskTls() {
    }

    public static ProtocolVersion[] versions() {
        return new ProtocolVersion[] { ProtocolVersion.TLSv12 };
    }

    public static int[] cipherSuites() {
        return new int[] { CipherSuite.TLS_DHE_PSK_WITH_AES_128_GCM_SHA256, CipherSuite.TLS_PSK_WITH_AES_128_GCM_SHA256,
                CipherSuite.TLS_PSK_WITH_AES_128_CCM_8 };
    }

    public static TransportContext context(SecurityParameters parameters, String identity) {
        // PSK sessions are not resumed; use the handshake random to distinguish connections.
        return TransportContext.of(TlsTransportContextKeys.PRINCIPAL, new PskPrincipal(identity))
                .with(TlsTransportContextKeys.TLS_SESSION_ID, new Opaque(parameters.getClientRandom()).toHex())
                .with(TlsTransportContextKeys.CIPHER_SUITE, Integer.toString(parameters.getCipherSuite()));
    }
}
