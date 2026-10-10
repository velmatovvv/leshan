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
package org.eclipse.leshan.transport.javacoap.identity;

import java.security.Principal;
import java.util.Objects;

import org.eclipse.leshan.core.peer.PskIdentity;

/** The identity authenticated by a completed TLS PSK handshake. */
public final class PskPrincipal implements Principal {
    private final String identity;

    public PskPrincipal(String identity) {
        this.identity = new PskIdentity(identity).getPskIdentity();
    }

    @Override
    public String getName() {
        return identity;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PskPrincipal && identity.equals(((PskPrincipal) other).identity);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(identity);
    }
}
