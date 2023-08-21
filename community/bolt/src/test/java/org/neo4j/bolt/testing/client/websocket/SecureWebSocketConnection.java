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
package org.neo4j.bolt.testing.client.websocket;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.dynamic.HttpClientTransportDynamic;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.neo4j.bolt.testing.client.TransportConnection;

public class SecureWebSocketConnection extends WebSocketConnection {
    private static final Factory factory = new Factory();

    public static Factory factory() {
        return factory;
    }

    public SecureWebSocketConnection(SocketAddress address) {
        super(address);
    }

    @Override
    protected WebSocketClient createClient() {
        ClientConnector clientConnector = new ClientConnector();
        clientConnector.setSslContextFactory(new SslContextFactory.Client(true));

        return new WebSocketClient(new HttpClient(new HttpClientTransportDynamic(clientConnector)));
    }

    @Override
    protected URI createTargetUri(SocketAddress address) {
        var socketAddress = (InetSocketAddress) address;
        return URI.create("wss://" + socketAddress.getHostString() + ":" + socketAddress.getPort());
    }

    private static class Factory implements TransportConnection.Factory {

        @Override
        public TransportConnection create(SocketAddress address) {
            return new SecureWebSocketConnection(address);
        }

        @Override
        public String toString() {
            return "TLS WebSocket";
        }
    }
}
