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

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class TestTopology
{
    private TestTopology()
    {
    }

    private static ClientConnectorAddresses wrapAsClientConnectorAddresses( AdvertisedSocketAddress advertisedSocketAddress )
    {
        return new ClientConnectorAddresses( singletonList( new ClientConnectorAddresses.ConnectorUri( bolt, advertisedSocketAddress ) ) );
    }

    public static CoreServerInfo addressesForCore( int id )
    {
        AdvertisedSocketAddress raftServerAddress = new AdvertisedSocketAddress( "localhost", 3000 + id );
        AdvertisedSocketAddress catchupServerAddress = new AdvertisedSocketAddress( "localhost", 4000 + id );
        AdvertisedSocketAddress boltServerAddress = new AdvertisedSocketAddress( "localhost", 5000 + id );
        return new CoreServerInfo( raftServerAddress, catchupServerAddress, wrapAsClientConnectorAddresses( boltServerAddress ),
                asSet( "core", "core" + id ), "default" );
    }

    public static ReadReplicaInfo addressesForReadReplica( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", 6000 + id );
        ClientConnectorAddresses clientConnectorAddresses = new ClientConnectorAddresses(
                singletonList( new ClientConnectorAddresses.ConnectorUri( bolt, advertisedSocketAddress ) ) );

        return new ReadReplicaInfo( clientConnectorAddresses, advertisedSocketAddress,
                asSet( "replica", "replica" + id ), "default" );
    }

    public static Map<MemberId,ReadReplicaInfo> readReplicaInfoMap( int... ids )
    {
        return Arrays.stream( ids ).mapToObj( TestTopology::readReplicaInfo ).collect( Collectors
                .toMap( p -> new MemberId( UUID.randomUUID() ), Function.identity() ) );
    }

    private static ReadReplicaInfo readReplicaInfo( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", 6000 + id );
        return new ReadReplicaInfo(
                new ClientConnectorAddresses( singletonList( new ClientConnectorAddresses.ConnectorUri( bolt, advertisedSocketAddress ) ) ),
                new AdvertisedSocketAddress( "localhost", 4000 + id ),
                asSet( "replica", "replica" + id ), "default" );
    }
}
