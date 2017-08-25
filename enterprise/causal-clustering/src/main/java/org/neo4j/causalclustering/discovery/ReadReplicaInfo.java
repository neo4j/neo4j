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

public class ReadReplicaInfo implements DiscoveryServerInfo
{
    private final AdvertisedSocketAddress catchupServerAddress;
    private final ClientConnectorAddresses clientConnectorAddresses;
    private final Set<String> groups;

    public ReadReplicaInfo( ClientConnectorAddresses clientConnectorAddresses, AdvertisedSocketAddress catchupServerAddress )
    {
        this( clientConnectorAddresses, catchupServerAddress, emptySet() );
    }

    public ReadReplicaInfo( ClientConnectorAddresses clientConnectorAddresses,
            AdvertisedSocketAddress catchupServerAddress, Set<String> groups )
    {
        this.clientConnectorAddresses = clientConnectorAddresses;
        this.catchupServerAddress = catchupServerAddress;
        this.groups = groups;
    }

    @Override
    public ClientConnectorAddresses connectors()
    {
        return clientConnectorAddresses;
    }

    @Override
    public AdvertisedSocketAddress getCatchupServer()
    {
        return catchupServerAddress;
    }

    @Override
    public Set<String> groups()
    {
        return groups;
    }

    @Override
    public String toString()
    {
        return "ReadReplicaInfo{" +
               "catchupServerAddress=" + catchupServerAddress +
               ", clientConnectorAddresses=" + clientConnectorAddresses +
               ", groups=" + groups +
               '}';
    }
}
