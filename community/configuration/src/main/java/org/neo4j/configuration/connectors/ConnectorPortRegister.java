/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.configuration.connectors;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;

/**
 * Connector tracker that keeps information about local address that any configured connector get during bootstrapping.
 */
public class ConnectorPortRegister
{

    private final ConcurrentHashMap<String,HostnamePort> connectorsInfo = new ConcurrentHashMap<>();

    public void register( String connectorKey, InetSocketAddress localAddress )
    {
        register( connectorKey, localAddress.getHostString(), localAddress.getPort() );
    }

    public void register( String connectorKey, SocketAddress localAddress )
    {
        register( connectorKey, localAddress.getHostname(), localAddress.getPort() );
    }

    public void deregister( String connectorKey )
    {
        connectorsInfo.remove( connectorKey );
    }

    public HostnamePort getLocalAddress( String connectorKey )
    {
        return connectorsInfo.get( connectorKey );
    }

    private void register( String connectorKey, String hostname, int port )
    {
        HostnamePort hostnamePort = new HostnamePort( hostname, port );
        connectorsInfo.put( connectorKey, hostnamePort );
    }
}
