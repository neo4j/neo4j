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

import java.util.HashMap;
import java.util.Map;
import org.eclipse.jetty.io.Connection;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;

public class JettyHttp2ConnectionListener implements Connection.Listener {

    private final NetworkConnectionTracker connectionTracker;
    private final String connectorName;
    private final Map<Connection, String> connectionHashToConnectionId = new HashMap<>();

    public JettyHttp2ConnectionListener(NetworkConnectionTracker connectionTracker, String connectorName) {
        this.connectionTracker = connectionTracker;
        this.connectorName = connectorName;
    }

    @Override
    public void onOpened(Connection connection) {
        var connectionId = connectionTracker.newConnectionId(connectorName);
        connectionHashToConnectionId.put(connection, connectionId);
        connectionTracker.add(new JettyHttp2Connection(connectorName, connectionId, connection));
    }

    @Override
    public void onClosed(Connection connection) {
        var connectionId = connectionHashToConnectionId.get(connection);
        connectionTracker.remove(connectionTracker.get(connectionId));
    }
}
