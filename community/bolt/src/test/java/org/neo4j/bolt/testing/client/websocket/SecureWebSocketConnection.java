/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.testing.client.websocket;

import java.net.URI;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.internal.helpers.HostnamePort;

public class SecureWebSocketConnection extends WebSocketConnection {
    private static final Factory factory = new Factory();

    public static Factory factory() {
        return factory;
    }

    public SecureWebSocketConnection(HostnamePort address) {
        super(address);
    }

    @Override
    protected WebSocketClient createClient() {
        var sslContextFactory = new SslContextFactory.Client(/* trustAll= */ true);
        // remove extra filters added by jetty on cipher suites
        sslContextFactory.setExcludeCipherSuites();
        var httpClient = new HttpClient(sslContextFactory);
        return new WebSocketClient(httpClient);
    }

    @Override
    protected URI createTargetUri(HostnamePort address) {
        return URI.create("wss://" + address.getHost() + ":" + address.getPort());
    }

    private static class Factory implements TransportConnection.Factory {

        @Override
        public TransportConnection create(HostnamePort address) {
            return new SecureWebSocketConnection(address);
        }

        @Override
        public String toString() {
            return "TLS WebSocket";
        }
    }
}
