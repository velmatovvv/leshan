/*******************************************************************************
 * Copyright (c) 2023 Sierra Wireless and others.
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

import java.net.InetSocketAddress;
import java.util.function.Consumer;

import com.mbed.coap.transport.CoapTcpListener;

final class ObservationCleanupListener implements CoapTcpListener {
    private final CoapTcpListener delegate;
    private final Consumer<InetSocketAddress> cleanup;

    ObservationCleanupListener(CoapTcpListener delegate, Consumer<InetSocketAddress> cleanup) {
        this.delegate = delegate;
        this.cleanup = cleanup;
    }

    @Override
    public void onConnected(InetSocketAddress address) {
        delegate.onConnected(address);
    }

    @Override
    public void onDisconnected(InetSocketAddress address) {
        try {
            cleanup.accept(address);
        } finally {
            delegate.onDisconnected(address);
        }
    }
}
