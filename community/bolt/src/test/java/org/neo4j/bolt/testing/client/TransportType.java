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

import org.neo4j.bolt.testing.client.tls.SecureSocketConnection;
import org.neo4j.bolt.testing.client.websocket.SecureWebSocketConnection;
import org.neo4j.bolt.testing.client.websocket.WebSocketConnection;

public enum TransportType {
    TCP(SocketConnection.factory()),
    TCP_TLS(SecureSocketConnection.factory()),

    WEBSOCKET(WebSocketConnection.factory()),
    WEBSOCKET_TLS(SecureWebSocketConnection.factory()),
    UNIX(UnixDomainSocketConnection.factory()),

    LOCAL(LocalConnection.factory());

    private final TransportConnection.Factory factory;

    TransportType(TransportConnection.Factory factory) {
        this.factory = factory;
    }

    public TransportConnection.Factory getFactory() {
        return this.factory;
    }
}
