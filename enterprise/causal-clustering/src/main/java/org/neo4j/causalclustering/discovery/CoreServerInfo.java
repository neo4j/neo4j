/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Set;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Collections.emptySet;

public class CoreServerInfo implements CatchupServerAddress, ClientConnector, GroupedServer
{
    private final AdvertisedSocketAddress raftServer;
    private final AdvertisedSocketAddress catchupServer;
    private final ClientConnectorAddresses clientConnectorAddresses;
    private final Set<String> groups;

    public CoreServerInfo( AdvertisedSocketAddress raftServer, AdvertisedSocketAddress catchupServer,
            ClientConnectorAddresses clientConnectors )
    {
        this( raftServer, catchupServer, clientConnectors, emptySet() );
    }

    public CoreServerInfo( AdvertisedSocketAddress raftServer, AdvertisedSocketAddress catchupServer,
            ClientConnectorAddresses clientConnectorAddresses, Set<String> groups )
    {
        this.raftServer = raftServer;
        this.catchupServer = catchupServer;
        this.clientConnectorAddresses = clientConnectorAddresses;
        this.groups = groups;
    }

    public AdvertisedSocketAddress getRaftServer()
    {
        return raftServer;
    }

    @Override
    public AdvertisedSocketAddress getCatchupServer()
    {
        return catchupServer;
    }

    @Override
    public ClientConnectorAddresses connectors()
    {
        return clientConnectorAddresses;
    }

    @Override
    public Set<String> groups()
    {
        return groups;
    }

    @Override
    public String toString()
    {
        return "CoreServerInfo{" +
               "raftServer=" + raftServer +
               ", catchupServer=" + catchupServer +
               ", clientConnectorAddresses=" + clientConnectorAddresses +
               ", groups=" + groups +
               '}';
    }
}
