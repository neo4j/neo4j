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
package org.neo4j.configuration.connectors;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;

/**
 * Connector tracker that keeps information about local address that any configured connector get during bootstrapping.
 */
public class ConnectorPortRegister {
    public interface Listener {
        void portRegistered(ConnectorType connectorType, SocketAddress socketAddress);
    }

    private final ConcurrentHashMap<ConnectorType, HostnamePort> connectorsInfo = new ConcurrentHashMap<>();
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    public void register(ConnectorType connectorKey, InetSocketAddress localAddress) {
        register(connectorKey, localAddress.getHostString(), localAddress.getPort());
    }

    public void register(ConnectorType connectorKey, SocketAddress localAddress) {
        register(connectorKey, localAddress.getHostname(), localAddress.getPort());
    }

    public void deregister(ConnectorType connectorKey) {
        connectorsInfo.remove(connectorKey);
    }

    public HostnamePort getLocalAddress(ConnectorType connectorKey) {
        return connectorsInfo.get(connectorKey);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    private void register(ConnectorType connectorKey, String hostname, int port) {
        HostnamePort hostnamePort = new HostnamePort(hostname, port);
        connectorsInfo.put(connectorKey, hostnamePort);
        listeners.forEach(listener -> listener.portRegistered(connectorKey, new SocketAddress(hostname, port)));
    }
}
