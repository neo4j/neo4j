/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.testing.client;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.neo4j.bolt.testing.client.error.BoltTestClientStateException;
import org.neo4j.bolt.testing.client.tls.NaiveTrustManager;

public final class SecureWebSocketConnection extends WebSocketConnection implements SecureBoltTestConnection {

    private static final Factory factory = new Factory();

    public static BoltTestConnection.Factory factory() {
        return factory;
    }

    public SecureWebSocketConnection(InetSocketAddress address) {
        super(address);
    }

    @Override
    protected SslContext sslContext() throws SSLException {
        var builder = SslContextBuilder.forClient().trustManager(NaiveTrustManager.getInstance());

        if (this.certificate != null) {
            builder.keyManager(this.privateKey, this.certificate);
        }

        return builder.build();
    }

    @Override
    public Set<X509Certificate> getServerCertificatesSeen() {
        var sslEngine = this.sslEngine;
        if (sslEngine == null) {
            return Set.of();
        }

        try {
            return Arrays.stream(this.sslEngine.getSession().getPeerCertificates())
                    .map(cert -> (X509Certificate) cert)
                    .collect(Collectors.toSet());
        } catch (SSLPeerUnverifiedException ex) {
            return Set.of();
        }
    }

    protected URI webSocketAddress() {
        try {
            return new URI("wss", null, this.address.getHostString(), this.address.getPort(), "/", null, null);
        } catch (URISyntaxException ex) {
            throw new BoltTestClientStateException("Failed to construct WebSocket address", ex);
        }
    }

    private static class Factory implements BoltTestConnection.Factory {

        @Override
        public BoltTestConnection create(SocketAddress address) {
            if (address instanceof InetSocketAddress inetSocketAddress) {
                return new SecureWebSocketConnection(inetSocketAddress);
            }

            throw new IllegalArgumentException("Cannot initialize TLS WebSocket connection with address of type "
                    + address.getClass().getSimpleName());
        }

        @Override
        public String toString() {
            return "TLS WebSocket";
        }
    }
}
