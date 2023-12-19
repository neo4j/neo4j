/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
    private final String dbName;

    public ReadReplicaInfo( ClientConnectorAddresses clientConnectorAddresses, AdvertisedSocketAddress catchupServerAddress, String dbName )
    {
        this( clientConnectorAddresses, catchupServerAddress, emptySet(), dbName );
    }

    public ReadReplicaInfo( ClientConnectorAddresses clientConnectorAddresses,
            AdvertisedSocketAddress catchupServerAddress, Set<String> groups, String dbName )
    {
        this.clientConnectorAddresses = clientConnectorAddresses;
        this.catchupServerAddress = catchupServerAddress;
        this.groups = groups;
        this.dbName = dbName;
    }

    @Override
    public String getDatabaseName()
    {
        return dbName;
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
