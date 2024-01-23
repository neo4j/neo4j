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
package org.neo4j.server.web;

import java.net.SocketAddress;
import org.eclipse.jetty.io.Connection;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;

public class JettyHttp2Connection implements TrackedNetworkConnection {

    private final Connection connection;
    private final String connectorId;
    private final String connectionId;

    public JettyHttp2Connection(String connectorId, String connectionId, Connection connection) {
        this.connection = connection;
        this.connectionId = connectionId;
        this.connectorId = connectorId;
    }

    @Override
    public String id() {
        return connectionId;
    }

    @Override
    public long connectTime() {
        return connection.getCreatedTimeStamp();
    }

    @Override
    public String connectorId() {
        return connectorId;
    }

    @Override
    public SocketAddress serverAddress() {
        return connection.getEndPoint().getLocalAddress();
    }

    @Override
    public SocketAddress clientAddress() {
        return connection.getEndPoint().getRemoteAddress();
    }

    @Override
    public String username() {
        return null;
    }

    @Override
    public String userAgent() {
        return null;
    }

    @Override
    public void updateUser(String username, String userAgent) {
        // Ignored
    }

    @Override
    public void close() {
        connection.close();
    }
}
