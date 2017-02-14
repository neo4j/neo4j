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

import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;

public class TestTopology
{
    private static ClientConnectorAddresses wrapAsClientConnectorAddresses( AdvertisedSocketAddress advertisedSocketAddress )
    {
        return new ClientConnectorAddresses( singletonList( new ClientConnectorAddresses.ConnectorUri( bolt, advertisedSocketAddress ) ) );
    }

    public static CoreServerInfo adressesForCore( int id )
    {
        AdvertisedSocketAddress raftServerAddress = new AdvertisedSocketAddress( "localhost", (3000 + id) );
        AdvertisedSocketAddress catchupServerAddress = new AdvertisedSocketAddress( "localhost", (4000 + id) );
        AdvertisedSocketAddress boltServerAddress = new AdvertisedSocketAddress( "localhost", (5000 + id) );
        return new CoreServerInfo( raftServerAddress, catchupServerAddress, wrapAsClientConnectorAddresses( boltServerAddress ) );
    }

    public static ReadReplicaInfo addressesForReadReplica( int id )
    {
        AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "localhost", (6000 + id) );
        ClientConnectorAddresses clientConnectorAddresses = new ClientConnectorAddresses(
                singletonList( new ClientConnectorAddresses.ConnectorUri( bolt, advertisedSocketAddress ) ) );

        return new ReadReplicaInfo( clientConnectorAddresses, advertisedSocketAddress );
    }
}
